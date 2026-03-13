using Dataverse.Linq.Expressions;
using Dataverse.Linq.Model;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Linq;

namespace Dataverse.Linq;

internal class DataverseQueryProvider<T> : IAsyncQueryProvider where T : Entity
{
    internal IOrganizationServiceAsync Service { get; }
    internal string EntityLogicalName { get; }
    internal IReadOnlyList<string>? Columns { get; }

    internal DataverseQueryProvider(IOrganizationServiceAsync service, string entityLogicalName, IReadOnlyList<string>? columns = null)
    {
        Service = service;
        EntityLogicalName = entityLogicalName;
        Columns = columns;
    }

    // -------------------------------------------------------------------------
    // IQueryProvider
    // -------------------------------------------------------------------------

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Use the generic CreateQuery<TElement> overload.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) == typeof(T))
            return (IQueryable<TElement>)(object)new DataverseQueryable<T>(this, expression);

        return new DataverseProjectedQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression) => Execute<List<T>>(expression);

    /// <summary>
    /// Synchronous execution — used by <see cref="DataverseProjectedQueryable{TElement}.GetEnumerator"/>.
    /// <typeparamref name="TResult"/> is typically <see cref="IEnumerable{T}"/> for projected types.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns);
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = RetrieveAll(fetchXml);

        var elementType = typeof(TResult).IsGenericType
            ? typeof(TResult).GetGenericArguments()[0]
            : typeof(T);

        var method = GetPrivateMethod(nameof(ProjectEntities)).MakeGenericMethod(elementType);
        return (TResult)method.Invoke(this, [entities, query])!;
    }

    // -------------------------------------------------------------------------
    // IAsyncQueryProvider (EF Core)
    // Note: TResult is the Task itself (e.g. Task<List<TElement>>), not the
    // unwrapped value — this matches EF Core's interface contract.
    // -------------------------------------------------------------------------

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        // TResult = Task<List<TElement>>
        var elementType = typeof(TResult).GetGenericArguments()[0].GetGenericArguments()[0];
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns);
        var method = GetPrivateMethod(nameof(ExecuteQueryAsync)).MakeGenericMethod(elementType);
        return (TResult)method.Invoke(this, [query, cancellationToken])!;
    }

    // -------------------------------------------------------------------------
    // Internal — used by DataverseQueryable<T>.GetEnumerator()
    // -------------------------------------------------------------------------

    internal List<T> ExecuteList(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns);
        var fetchXml = FetchXmlBuilder.Build(query);
        return RetrieveAll(fetchXml).Select(e => e.ToEntity<T>()).ToList();
    }

    internal string GenerateFetchXml(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns);
        return FetchXmlBuilder.Build(query);
    }

    // -------------------------------------------------------------------------
    // Async execution
    // -------------------------------------------------------------------------

    private async Task<List<TElement>> ExecuteQueryAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return ProjectEntities<TElement>(entities, query);
    }

    // -------------------------------------------------------------------------
    // Paged retrieval
    // -------------------------------------------------------------------------

    private List<Entity> RetrieveAll(string baseFetchXml)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = Service.RetrieveMultiple(new FetchExpression(fetchDocument.ToString()));
            results.AddRange(response.Entities);

            if (!response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }

        return results;
    }

    private async Task<List<Entity>> RetrieveAllAsync(string baseFetchXml, CancellationToken cancellationToken)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = await Service.RetrieveMultipleAsync(new FetchExpression(fetchDocument.ToString()));
            results.AddRange(response.Entities);

            if (!response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }

        return results;
    }

    // -------------------------------------------------------------------------
    // Projection
    // -------------------------------------------------------------------------

    /// <summary>
    /// Projects raw Dataverse entity rows into <typeparamref name="TElement"/> based
    /// on the query model: inner join (2-param projector), simple/left join (1-param
    /// projector), or no projection (typed entity).
    /// </summary>
    private List<TElement> ProjectEntities<TElement>(List<Entity> entities, FetchXmlQuery query)
    {
        // Inner join: 2-param result selector (outer, inner) → TElement
        if (query.InnerEntityType is not null && query.Projector is not null)
        {
            var link = query.Links[0];
            return entities.Select(e =>
            {
                var outer = e.ToEntity<T>();
                var inner = ExtractLinkedEntity(e, link.Alias, link.Name, query.InnerEntityType);
                return (TElement)query.Projector.DynamicInvoke(outer, inner)!;
            }).ToList();
        }

        // Simple select / left join: 1-param projector (entity) → TElement
        if (query.Projector is not null)
        {
            return entities.Select(e =>
                (TElement)query.Projector.DynamicInvoke(e.ToEntity<T>())!
            ).ToList();
        }

        // No projection: return typed entities
        return entities.Select(e => (TElement)(object)e.ToEntity<T>()).ToList();
    }

    /// <summary>
    /// Strips the alias prefix from linked-entity attributes, unwraps <see cref="AliasedValue"/>,
    /// and converts the result to the correct proxy type via <see cref="Entity.ToEntity{T}"/>.
    /// </summary>
    private static object ExtractLinkedEntity(Entity source, string alias, string logicalName, Type innerType)
    {
        var inner = new Entity(logicalName);
        var prefix = alias + ".";

        foreach (var attr in source.Attributes)
        {
            if (!attr.Key.StartsWith(prefix)) continue;
            var name = attr.Key[prefix.Length..];
            inner[name] = attr.Value is AliasedValue av ? av.Value : attr.Value;
        }

        if (inner.Attributes.TryGetValue(logicalName + "id", out var idVal) && idVal is Guid id)
            inner.Id = id;

        return typeof(Entity).GetMethod(nameof(Entity.ToEntity))!
            .MakeGenericMethod(innerType)
            .Invoke(inner, null)!;
    }

    private static MethodInfo GetPrivateMethod(string name) =>
        typeof(DataverseQueryProvider<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}

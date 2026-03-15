using XrmToolkit.Dataverse.Linq.Expressions;
using XrmToolkit.Dataverse.Linq.Model;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Linq;

namespace XrmToolkit.Dataverse.Linq;

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
    /// <typeparamref name="TResult"/> is typically <see cref="IEnumerable{T}"/> for projected types,
    /// or a scalar element type for First/Single operators.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = RetrieveAll(fetchXml);

        // Aggregate terminal operator (Min, Max, Sum, Average, Count)
        if (query.TerminalOperator is QueryTerminalOperator.Min or QueryTerminalOperator.Max
            or QueryTerminalOperator.Sum or QueryTerminalOperator.Average
            or QueryTerminalOperator.Count or QueryTerminalOperator.LongCount
            or QueryTerminalOperator.CountColumn)
            return ExtractAggregateResult<TResult>(entities, query.TerminalOperator);

        // Scalar terminal operator (First, Single, etc.)
        if (query.TerminalOperator is not QueryTerminalOperator.List)
        {
            var projected = ProjectEntities<TResult>(entities, query);
            return ApplyTerminalOperator(projected, query.TerminalOperator);
        }

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
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);

        // Aggregate terminal operator: TResult = Task<TElement>
        if (query.TerminalOperator is QueryTerminalOperator.Min or QueryTerminalOperator.Max
            or QueryTerminalOperator.Sum or QueryTerminalOperator.Average
            or QueryTerminalOperator.Count or QueryTerminalOperator.LongCount
            or QueryTerminalOperator.CountColumn)
        {
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var method = GetPrivateMethod(nameof(ExecuteAggregateAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }

        // Scalar terminal operator: TResult = Task<TElement>
        if (query.TerminalOperator is not QueryTerminalOperator.List)
        {
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var method = GetPrivateMethod(nameof(ExecuteScalarAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }

        // List: TResult = Task<List<TElement>>
        {
            var elementType = typeof(TResult).GetGenericArguments()[0].GetGenericArguments()[0];
            var method = GetPrivateMethod(nameof(ExecuteQueryAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }
    }

    // -------------------------------------------------------------------------
    // Internal — used by DataverseQueryable<T>.GetEnumerator()
    // -------------------------------------------------------------------------

    internal List<T> ExecuteList(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        return RetrieveAll(fetchXml).Select(e => e.ToEntity<T>()).ToList();
    }

    internal async Task ForEachPageAsync<TElement>(
        Expression expression,
        Func<List<TElement>, Task> onPage,
        CancellationToken cancellationToken)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        var fetchDocument = XDocument.Parse(fetchXml);
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = await Service.RetrieveMultipleAsync(new FetchExpression(fetchDocument.ToString()));
            var page = ProjectEntities<TElement>(response.Entities.ToList(), query);
            await onPage(page);

            if (!response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }
    }

    internal void ForEachPage<TElement>(
        Expression expression,
        Action<List<TElement>> onPage)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        var fetchDocument = XDocument.Parse(fetchXml);
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
            var page = ProjectEntities<TElement>(response.Entities.ToList(), query);
            onPage(page);

            if (!response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }
    }

    internal string GenerateFetchXml(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
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

    private async Task<TElement> ExecuteScalarAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        var projected = ProjectEntities<TElement>(entities, query);
        return ApplyTerminalOperator(projected, query.TerminalOperator);
    }

    private async Task<TElement> ExecuteAggregateAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return ExtractAggregateResult<TElement>(entities, query.TerminalOperator);
    }

    private static TResult ExtractAggregateResult<TResult>(List<Entity> entities, QueryTerminalOperator op)
    {
        if (entities.Count == 0)
            throw new InvalidOperationException("Aggregate query returned no results.");

        var alias = op switch
        {
            QueryTerminalOperator.Min => "min",
            QueryTerminalOperator.Max => "max",
            QueryTerminalOperator.Sum => "sum",
            QueryTerminalOperator.Average => "avg",
            QueryTerminalOperator.Count or QueryTerminalOperator.LongCount => "count",
            QueryTerminalOperator.CountColumn => "countcolumn",
            _ => throw new InvalidOperationException($"Unexpected aggregate operator '{op}'.")
        };

        var entity = entities[0];
        var aliasedValue = entity.GetAttributeValue<AliasedValue>(alias)
            ?? throw new InvalidOperationException(
                $"Aggregate result did not contain expected '{alias}' alias.");

        var rawValue = aliasedValue.Value;

        // Money types return Money objects; extract the decimal value
        if (rawValue is Money money)
            rawValue = money.Value;

        var targetType = Nullable.GetUnderlyingType(typeof(TResult)) ?? typeof(TResult);
        return (TResult)Convert.ChangeType(rawValue, targetType);
    }

    private static TElement ApplyTerminalOperator<TElement>(List<TElement> results, QueryTerminalOperator op)
    {
        return op switch
        {
            QueryTerminalOperator.First => results.First(),
            QueryTerminalOperator.FirstOrDefault => results.FirstOrDefault()!,
            QueryTerminalOperator.Single => results.Single(),
            QueryTerminalOperator.SingleOrDefault => results.SingleOrDefault()!,
            _ => throw new InvalidOperationException($"Unexpected terminal operator '{op}'.")
        };
    }

    // -------------------------------------------------------------------------
    // Paged retrieval
    // -------------------------------------------------------------------------

    private List<Entity> RetrieveAll(string baseFetchXml)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        var explicitPage = fetchDocument.Root!.Attribute("page") != null;
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

            if (explicitPage || !response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }

        return results;
    }

    private async Task<List<Entity>> RetrieveAllAsync(string baseFetchXml, CancellationToken cancellationToken)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        var explicitPage = fetchDocument.Root!.Attribute("page") != null;
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

            if (explicitPage || !response.MoreRecords) break;

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
        // Grouped aggregate: projector takes raw Entity (not typed)
        if (query.Aggregate && query.Projector is not null)
        {
            return entities.Select(e =>
                (TElement)query.Projector.DynamicInvoke(e)!
            ).ToList();
        }

        // Inner join: single-param (Entity) projector that extracts aliased values
        if (query.InnerEntityType is not null && query.Projector is not null)
        {
            return entities.Select(e =>
                (TElement)query.Projector.DynamicInvoke(e)!
            ).ToList();
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

    private static MethodInfo GetPrivateMethod(string name) =>
        typeof(DataverseQueryProvider<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}

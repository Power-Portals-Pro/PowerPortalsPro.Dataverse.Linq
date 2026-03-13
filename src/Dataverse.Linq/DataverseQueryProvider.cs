using Dataverse.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Collections;
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

    public object? Execute(Expression expression) => ExecuteList(expression);

    /// <summary>
    /// Synchronous execution — used by <see cref="DataverseProjectedQueryable{TElement}.GetEnumerator"/>.
    /// <typeparamref name="TResult"/> is typically <see cref="IEnumerable{T}"/> for projected types.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        var joinInfo = JoinExpressionParser.TryParse(expression);
        if (joinInfo is not null)
        {
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var method = GetPrivateMethod(nameof(FetchJoinList)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [joinInfo])!;
        }

        var (selectColumns, projector) = SelectExpressionParser.Parse(expression);
        var columns = selectColumns ?? Columns;
        var entities = FetchList(columns);

        if (projector is not null)
            return BuildProjectedList<TResult>(entities, projector);

        if (entities is TResult directResult)
            return directResult;

        throw new InvalidCastException($"Cannot convert List<{typeof(T).Name}> to {typeof(TResult).Name}.");
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

        var joinInfo = JoinExpressionParser.TryParse(expression);
        if (joinInfo is not null)
        {
            var method = GetPrivateMethod(nameof(FetchJoinListAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [joinInfo, cancellationToken])!;
        }

        var (selectColumns, projector) = SelectExpressionParser.Parse(expression);
        var columns = selectColumns ?? Columns;

        if (projector is null)
            return (TResult)(object)FetchListAsync(columns, cancellationToken);

        var projectedMethod = GetPrivateMethod(nameof(FetchProjectedListAsync)).MakeGenericMethod(elementType);
        return (TResult)projectedMethod.Invoke(this, [columns, projector, cancellationToken])!;
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    internal List<T> ExecuteList(Expression expression) => FetchList(Columns);

    // -------------------------------------------------------------------------
    // Sync fetch
    // -------------------------------------------------------------------------

    private List<T> FetchList(IReadOnlyList<string>? columns)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        return RetrieveAll(fetchXml).Select(e => e.ToEntity<T>()).ToList();
    }

    private List<TElement> FetchJoinList<TElement>(JoinInfo joinInfo)
    {
        var entities = RetrieveAll(FetchXmlBuilder.BuildJoin(joinInfo));
        return entities.Select(e => ProjectJoinEntity<TElement>(e, joinInfo)).ToList();
    }

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

    // -------------------------------------------------------------------------
    // Async fetch
    // -------------------------------------------------------------------------

    private async Task<List<T>> FetchListAsync(IReadOnlyList<string>? columns, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return entities.Select(e => e.ToEntity<T>()).ToList();
    }

    private async Task<List<TElement>> FetchProjectedListAsync<TElement>(
        IReadOnlyList<string>? columns, Delegate projector, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return entities.Select(e => (TElement)projector.DynamicInvoke(e.ToEntity<T>())!).ToList();
    }

    private async Task<List<TElement>> FetchJoinListAsync<TElement>(
        JoinInfo joinInfo, CancellationToken cancellationToken)
    {
        var entities = await RetrieveAllAsync(FetchXmlBuilder.BuildJoin(joinInfo), cancellationToken);
        return entities.Select(e => ProjectJoinEntity<TElement>(e, joinInfo)).ToList();
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
    /// Projects a raw Dataverse entity row to <typeparamref name="TElement"/> based on the join type.
    /// For outer joins the outer entity is passed to <see cref="JoinInfo.Projector"/>;
    /// for inner joins both entities are passed to <see cref="JoinInfo.ResultSelector"/>.
    /// </summary>
    private TElement ProjectJoinEntity<TElement>(Entity e, JoinInfo joinInfo)
    {
        var outer = e.ToEntity<T>();

        if (joinInfo.IsOuterJoin)
            return joinInfo.Projector is not null
                ? (TElement)joinInfo.Projector.DynamicInvoke(outer)!
                : (TElement)(object)outer;

        var inner = ExtractLinkedEntity(e, joinInfo.InnerAlias, joinInfo.InnerEntityLogicalName, joinInfo.InnerEntityType!);
        return (TElement)joinInfo.ResultSelector!.DynamicInvoke(outer, inner)!;
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

    private static TResult BuildProjectedList<TResult>(List<T> entities, Delegate projector)
    {
        var elementType = typeof(TResult).GetGenericArguments()[0];
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;

        foreach (var entity in entities)
            list.Add(projector.DynamicInvoke(entity));

        return (TResult)(object)list;
    }

    private static MethodInfo GetPrivateMethod(string name) =>
        typeof(DataverseQueryProvider<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}

using PowerPortalsPro.Dataverse.Linq.Expressions;
using PowerPortalsPro.Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Linq;

namespace PowerPortalsPro.Dataverse.Linq;

internal class DataverseQueryProvider<T> : IQueryProvider where T : Entity
{
    internal IOrganizationService Service { get; }
    internal string EntityLogicalName { get; }
    internal IReadOnlyList<string>? Columns { get; }

    internal DataverseQueryProvider(IOrganizationService service, string entityLogicalName, IReadOnlyList<string>? columns = null)
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
        if (query.TerminalOperator.IsAggregate())
            return ExtractAggregateResult<TResult>(entities, query.TerminalOperator);

        // Scalar terminal operator (First, Single, etc.)
        if (query.TerminalOperator.IsScalar())
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
    // Internal — used by DataverseQueryable<T>.GetEnumerator()
    // -------------------------------------------------------------------------

    internal List<T> ExecuteList(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        return RetrieveAll(fetchXml).Select(e => e.ToEntity<T>()).ToList();
    }

    internal void ForEachPage<TElement>(
        Expression expression,
        Action<List<TElement>> onPage)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);

        PagedFetch(fetchXml, expr => Service.RetrieveMultiple(expr), (response, _) =>
        {
            onPage(ProjectEntities<TElement>(response.Entities.ToList(), query));
            return response.MoreRecords;
        });
    }

    internal string GenerateFetchXml(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        return FetchXmlBuilder.Build(query);
    }

    // -------------------------------------------------------------------------
    // Paged retrieval
    // -------------------------------------------------------------------------

    protected List<Entity> RetrieveAll(string baseFetchXml) =>
        RetrieveWithPaging(baseFetchXml, expr => Service.RetrieveMultiple(expr));

    protected static List<Entity> RetrieveWithPaging(
        string baseFetchXml, Func<FetchExpression, EntityCollection> retrieve)
    {
        var results = new List<Entity>();
        PagedFetch(baseFetchXml, retrieve, (response, _) =>
        {
            results.AddRange(response.Entities);
            return response.MoreRecords;
        });
        return results;
    }

    /// <summary>
    /// Core paging loop shared by all retrieval methods. Calls <paramref name="retrieve"/>
    /// for each page and passes the response to <paramref name="onPage"/>. The callback
    /// returns <c>true</c> to continue paging or <c>false</c> to stop.
    /// </summary>
    protected static void PagedFetch(
        string baseFetchXml,
        Func<FetchExpression, EntityCollection> retrieve,
        Func<EntityCollection, int, bool> onPage)
    {
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

            var response = retrieve(new FetchExpression(fetchDocument.ToString()));
            var shouldContinue = onPage(response, pageNumber);

            if (explicitPage || !shouldContinue || !response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    protected static TResult ExtractAggregateResult<TResult>(List<Entity> entities, QueryTerminalOperator op)
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

    protected static TElement ApplyTerminalOperator<TElement>(List<TElement> results, QueryTerminalOperator op)
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

    /// <summary>
    /// Projects raw Dataverse entity rows into <typeparamref name="TElement"/> based
    /// on the query model: inner join (2-param projector), simple/left join (1-param
    /// projector), or no projection (typed entity).
    /// </summary>
    protected List<TElement> ProjectEntities<TElement>(List<Entity> entities, FetchXmlQuery query)
    {
        if (query.Projector is null)
            return entities.Select(e => (TElement)(object)e.ToEntity<T>()).ToList();

        // Aggregate, inner join, and left join projectors all take raw Entity
        if (query.Aggregate || query.InnerEntityType is not null)
            return entities.Select(e => (TElement)query.Projector.DynamicInvoke(e)!).ToList();

        // Simple select: projector takes typed entity
        return entities.Select(e => (TElement)query.Projector.DynamicInvoke(e.ToEntity<T>())!).ToList();
    }

    protected static MethodInfo GetPrivateMethod(string name) =>
        typeof(DataverseQueryProvider<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}

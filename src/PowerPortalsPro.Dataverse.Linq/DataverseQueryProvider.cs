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
        var entities = RetrieveAll(fetchXml, query.OnRecordCount, BuildFetchXmlNotifier(query));

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
        return RetrieveAll(fetchXml, query.OnRecordCount, BuildFetchXmlNotifier(query))
            .Select(e => e.ToEntity<T>()).ToList();
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
        }, BuildFetchXmlNotifier(query));
    }

    internal string GenerateFetchXml(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        return FetchXmlBuilder.Build(query);
    }

    // -------------------------------------------------------------------------
    // Paged retrieval
    // -------------------------------------------------------------------------

    protected List<Entity> RetrieveAll(
        string baseFetchXml,
        Action<RecordCountArguments>? onRecordCount = null,
        Action<string>? onFetchXml = null) =>
        RetrieveWithPaging(baseFetchXml, expr => Service.RetrieveMultiple(expr), onRecordCount, onFetchXml);

    protected static List<Entity> RetrieveWithPaging(
        string baseFetchXml, Func<FetchExpression, EntityCollection> retrieve,
        Action<RecordCountArguments>? onRecordCount = null,
        Action<string>? onFetchXml = null)
    {
        var results = new List<Entity>();
        var recordCountInvoked = false;
        PagedFetch(baseFetchXml, retrieve, (response, _) =>
        {
            if (onRecordCount != null && !recordCountInvoked)
            {
                onRecordCount(new RecordCountArguments(response.TotalRecordCount, response.TotalRecordCountLimitExceeded));
                recordCountInvoked = true;
            }
            results.AddRange(response.Entities);
            return response.MoreRecords;
        }, onFetchXml);
        return results;
    }

    /// <summary>
    /// Builds the notifier invoked with each request's FetchXml just before it is sent,
    /// combining the per-query <see cref="FetchXmlQuery.OnFetchXml"/> callback with the
    /// global <see cref="DataverseQueryDiagnostics.FetchXmlRequested"/> hook. Returns
    /// <c>null</c> when nothing is listening so the paging loop can skip notification.
    /// </summary>
    protected static Action<string>? BuildFetchXmlNotifier(FetchXmlQuery query)
    {
        var perQuery = query.OnFetchXml;
        if (perQuery is null && !DataverseQueryDiagnostics.HasFetchXmlSubscribers)
            return null;

        return fetchXml =>
        {
            perQuery?.Invoke(fetchXml);
            DataverseQueryDiagnostics.RaiseFetchXmlRequested(fetchXml);
        };
    }

    /// <summary>
    /// Core paging loop shared by all retrieval methods. Calls <paramref name="retrieve"/>
    /// for each page and passes the response to <paramref name="onPage"/>. The callback
    /// returns <c>true</c> to continue paging or <c>false</c> to stop. When supplied,
    /// <paramref name="onFetchXml"/> is invoked with each page's FetchXml just before the
    /// request is sent.
    /// </summary>
    protected static void PagedFetch(
        string baseFetchXml,
        Func<FetchExpression, EntityCollection> retrieve,
        Func<EntityCollection, int, bool> onPage,
        Action<string>? onFetchXml = null)
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

            var requestFetchXml = fetchDocument.ToString();
            onFetchXml?.Invoke(requestFetchXml);

            var response = retrieve(new FetchExpression(requestFetchXml));
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
    /// on the query model: materializer-based projection or plain typed entity cast.
    /// </summary>
    protected List<TElement> ProjectEntities<TElement>(List<Entity> entities, FetchXmlQuery query)
    {
        NormalizeCrossApplyAliases(entities, query);

        if (query.Materializer is not null)
            return entities.Select(e => (TElement)query.Materializer.Invoke(e)).ToList();

        return entities.Select(e => (TElement)(object)e.ToEntity<T>()).ToList();
    }

    /// <summary>
    /// A <c>matchfirstrowusingcrossapply</c> link returns its columns merged into the root
    /// row as <see cref="AliasedValue"/>s keyed by the column's <i>schema</i> name (e.g.
    /// <c>new_ParentAccount</c>), rather than the <c>{alias}.{logicalname}</c> aliasing a
    /// normal link uses. The materializer reads <c>{alias}.{logicalname}</c>, so without
    /// this normalization those columns resolve to null. For each cross-apply link, re-key
    /// its returned values to <c>{alias}.{attributelogicalname}</c> using the
    /// <see cref="AliasedValue"/> metadata.
    /// </summary>
    protected static void NormalizeCrossApplyAliases(List<Entity> entities, FetchXmlQuery query)
    {
        var crossApplyLinks = new List<FetchLinkEntity>();
        CollectCrossApplyLinks(query.Links, crossApplyLinks);
        if (crossApplyLinks.Count == 0)
            return;

        foreach (var entity in entities)
        {
            // Snapshot first: we add keys while iterating.
            var schemaNamedAliasedValues = entity.Attributes
                .Where(kvp => kvp.Value is AliasedValue && !kvp.Key.Contains('.'))
                .ToList();

            foreach (var kvp in schemaNamedAliasedValues)
            {
                var av = (AliasedValue)kvp.Value;
                var link = crossApplyLinks.FirstOrDefault(l => l.Name == av.EntityLogicalName);
                if (link is null)
                    continue;

                var normalizedKey = $"{link.Alias}.{av.AttributeLogicalName}";
                if (!entity.Attributes.ContainsKey(normalizedKey))
                    entity[normalizedKey] = av;
            }
        }
    }

    private static void CollectCrossApplyLinks(List<FetchLinkEntity> links, List<FetchLinkEntity> result)
    {
        foreach (var link in links)
        {
            if (link.LinkType == "matchfirstrowusingcrossapply")
                result.Add(link);
            CollectCrossApplyLinks(link.Links, result);
        }
    }

    protected static MethodInfo GetPrivateMethod(string name) =>
        typeof(DataverseQueryProvider<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}

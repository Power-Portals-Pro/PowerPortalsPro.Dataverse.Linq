using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using PowerPortalsPro.Dataverse.Linq.Model;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Extension methods for <see cref="IOrganizationService"/> that provide LINQ query
/// entry points, paging, FetchXml configuration, and aggregate operations.
/// </summary>
public static partial class ServiceClientExtensions
{
    /// <summary>
    /// Creates a <see cref="IQueryable{T}"/> for the given entity type.
    /// The entity type must be decorated with <see cref="EntityLogicalNameAttribute"/>.
    /// </summary>
    public static IQueryable<T> Queryable<T>(this IOrganizationService service, params string[] columns)
        where T : Entity
    {
        var entityLogicalName = typeof(T).GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
            ?? throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' must be decorated with '{nameof(EntityLogicalNameAttribute)}'.");

        return new DataverseQueryable<T>(service, entityLogicalName, columns.Length > 0 ? columns : null);
    }

    /// <summary>
    /// Creates a <see cref="IQueryable{Entity}"/> for unbound queries using the
    /// <see cref="Entity"/> base class directly (no proxy class required).
    /// </summary>
    public static IQueryable<Entity> Queryable(this IOrganizationService service, string entityLogicalName, params string[] columns)
    {
        return new DataverseQueryable<Entity>(service, entityLogicalName, columns.Length > 0 ? columns : null);
    }

    /// <summary>
    /// Sets the page size for the query by adding the FetchXml <c>count</c> attribute.
    /// </summary>
    public static IQueryable<TElement> WithPageSize<TElement>(
        this IQueryable<TElement> queryable,
        int pageSize)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithPageSize),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(pageSize));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Sets the page number (1-based) for the query by adding the FetchXml <c>page</c> attribute.
    /// Page 1 returns the first set of results.
    /// Use in combination with <see cref="WithPageSize{TElement}"/> to control paging.
    /// </summary>
    public static IQueryable<TElement> WithPage<TElement>(
        this IQueryable<TElement> queryable,
        int page)
    {
        if (page < 1)
            throw new ArgumentOutOfRangeException(nameof(page), page,
                "Page number must be 1 or greater. Paging is 1-based.");

        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithPage),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(page));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Sets the aggregate limit for the query by adding the FetchXml <c>aggregatelimit</c> attribute.
    /// The value must be between 1 and 50,000.
    /// </summary>
    public static IQueryable<TElement> WithAggregateLimit<TElement>(
        this IQueryable<TElement> queryable,
        int aggregateLimit)
    {
        if (aggregateLimit < 1 || aggregateLimit > 50_000)
            throw new ArgumentOutOfRangeException(nameof(aggregateLimit), aggregateLimit,
                "Aggregate limit must be between 1 and 50,000.");

        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithAggregateLimit),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(aggregateLimit));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Sets the datasource for the query by adding the FetchXml <c>datasource</c> attribute.
    /// </summary>
    public static IQueryable<TElement> WithDatasource<TElement>(
        this IQueryable<TElement> queryable,
        FetchDatasource datasource)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithDatasource),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(datasource));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Enables late materialization for the query by adding the FetchXml <c>latematerialize</c> attribute.
    /// </summary>
    public static IQueryable<TElement> WithLateMaterialize<TElement>(
        this IQueryable<TElement> queryable)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithLateMaterialize),
            [typeof(TElement)],
            queryable.Expression);

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Adds the FetchXml <c>no-lock</c> attribute to the query.
    /// </summary>
    [Obsolete("The no-lock attribute is deprecated by Dataverse and has no effect. All queries are now executed without locks.")]
    public static IQueryable<TElement> WithNoLock<TElement>(
        this IQueryable<TElement> queryable)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithNoLock),
            [typeof(TElement)],
            queryable.Expression);

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Adds SQL query hints to the FetchXml <c>options</c> attribute as a comma-delimited string.
    /// </summary>
    public static IQueryable<TElement> WithQueryHints<TElement>(
        this IQueryable<TElement> queryable,
        params SqlQueryHint[] hints)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithQueryHints),
            [typeof(TElement)],
            queryable.Expression,
            Expression.NewArrayInit(typeof(SqlQueryHint),
                hints.Select(h => Expression.Constant(h))));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Specifies that choice column data sorting should use raw order by mode, sorting by the
    /// integer value. Without this, the default is to sort choice columns using the choice label values.
    /// </summary>
    public static IQueryable<TElement> WithUseRawOrderBy<TElement>(
        this IQueryable<TElement> queryable)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithUseRawOrderBy),
            [typeof(TElement)],
            queryable.Expression);

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// When used on the inner source of a <c>join</c>, limits the join to return only the
    /// first matching row from the related entity. Translates to FetchXml
    /// <c>link-type="matchfirstrowusingcrossapply"</c>.
    /// </summary>
    public static IQueryable<TElement> WithFirstRow<TElement>(
        this IQueryable<TElement> queryable)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(WithFirstRow),
            [typeof(TElement)],
            queryable.Expression);

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Synchronously executes the query page by page, invoking <paramref name="onPage"/>
    /// with each page of results as they are retrieved from Dataverse.
    /// </summary>
    public static void ForEachPage<TElement>(
        this IQueryable<TElement> queryable,
        Action<List<TElement>> onPage)
    {
        var providerType = queryable.Provider.GetType();
        if (IsDataverseProvider(providerType))
        {
            ((dynamic)queryable.Provider).ForEachPage<TElement>(queryable.Expression, onPage);
            return;
        }

        throw new InvalidOperationException(
            "ForEachPage can only be used with Dataverse queryables created via Queryable<T>().");
    }

    /// <summary>
    /// Adds the FetchXml <c>returntotalrecordcount</c> attribute to the query and registers
    /// a callback that will be invoked with the total record count after the first page of
    /// results is retrieved from Dataverse.
    /// </summary>
    public static IQueryable<TElement> ReturnRecordCount<TElement>(
        this IQueryable<TElement> queryable,
        Action<RecordCountArguments> onRecordCount)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(ReturnRecordCount),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(onRecordCount));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Counts non-null values of the column projected by a preceding <c>Select</c>.
    /// Translates to FetchXml <c>aggregate="countcolumn"</c>.
    /// Usage: <c>query.Select(x => x.SomeColumn).CountColumn()</c>
    /// </summary>
    public static int CountColumn<TSource>(this IQueryable<TSource> queryable)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(CountColumn),
            [typeof(TSource)],
            queryable.Expression);

        return queryable.Provider.Execute<int>(expression);
    }

    /// <summary>
    /// Counts non-null values of the selected column within a grouped aggregate query.
    /// Translates to FetchXml <c>aggregate="countcolumn"</c>.
    /// Usage: <c>g.CountColumn(x =&gt; x.SomeColumn)</c>
    /// </summary>
    public static int CountColumn<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, object> selector)
    {
        throw new NotImplementedException(
            "This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");
    }

    /// <summary>
    /// Filters to rows where a related record exists matching the predicate.
    /// Translates to FetchXml <c>link-type="exists"</c>.
    /// This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.
    /// </summary>
    public static bool Exists<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
    {
        throw new NotImplementedException(
            "This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");
    }

    /// <summary>
    /// Filters to rows where a related record matches the predicate using an IN subquery.
    /// Translates to FetchXml <c>link-type="in"</c>.
    /// Semantically equivalent to <see cref="Exists{TSource}"/> but may have different
    /// performance characteristics depending on the query optimizer.
    /// This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.
    /// </summary>
    public static bool In<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
    {
        throw new NotImplementedException(
            "This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");
    }

    /// <summary>
    /// Returns the count of child records for the entity.
    /// Translates to FetchXml <c>rowaggregate="CountChildren"</c>.
    /// This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.
    /// Usage: <c>.Select(a => new { a.Name, Children = a.CountChildren() })</c>
    /// </summary>
    public static int CountChildren(this Entity entity)
    {
        throw new NotImplementedException(
            "This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");
    }

    /// <summary>
    /// Translates the LINQ query into its FetchXml representation without executing it.
    /// Useful for debugging, logging, or inspecting the generated query.
    /// </summary>
    public static string ToFetchXml<TElement>(this IQueryable<TElement> queryable)
    {
        var providerType = queryable.Provider.GetType();

        if (IsDataverseProvider(providerType))
        {
            return ((dynamic)queryable.Provider).GenerateFetchXml(queryable.Expression);
        }

        throw new InvalidOperationException(
            "ToFetchXml can only be used with Dataverse queryables created via Queryable<T>().");
    }

    /// <summary>
    /// Translates a query terminated by an operator that does not return an
    /// <see cref="IQueryable{T}"/> — an aggregate (<c>Count</c>, <c>Sum</c>, <c>Min</c>,
    /// <c>Max</c>, <c>Average</c>, <c>CountColumn</c>) or an element operator (<c>First</c>,
    /// <c>Single</c>, …) — into its FetchXml representation <b>without executing it</b>.
    /// The <paramref name="terminal"/> delegate describes the terminal call; it is run
    /// against a capturing queryable that records the expression instead of querying
    /// Dataverse.
    /// </summary>
    /// <example>
    /// <code>
    /// var fetchXml = query.ToFetchXml(q => q.Count());
    /// var fetchXml = query.ToFetchXml(q => q.Max(x => x.Revenue));
    /// var fetchXml = query.ToFetchXml(q => q.First());
    /// </code>
    /// </example>
    public static string ToFetchXml<TElement, TResult>(
        this IQueryable<TElement> queryable,
        Func<IQueryable<TElement>, TResult> terminal)
    {
        if (!IsDataverseProvider(queryable.Provider.GetType()))
            throw new InvalidOperationException(
                "ToFetchXml can only be used with Dataverse queryables created via Queryable<T>().");

        var capture = new FetchXmlCaptureProvider(queryable.Provider);
        var capturingQueryable = new FetchXmlCaptureQueryable<TElement>(capture, queryable.Expression);

        // Running the terminal builds the LINQ expression and routes execution into the
        // capturing provider, which records the expression rather than querying Dataverse.
        terminal(capturingQueryable);

        if (capture.CapturedExpression is null)
            throw new InvalidOperationException(
                "The terminal delegate did not produce a query to translate.");

        return capture.GenerateFetchXml();
    }

    /// <summary>
    /// Registers a callback invoked with the exact FetchXml of each request immediately
    /// before it is sent to Dataverse, then returns the query for further composition or
    /// execution. Unlike <see cref="ToFetchXml{TElement}(IQueryable{TElement})"/>, this
    /// captures the FetchXml of <i>every</i> execution path — including aggregates,
    /// element operators, and paged retrieval — as the query runs. For multi-page
    /// queries the callback fires once per page, with the page number and paging-cookie
    /// embedded in the captured FetchXml.
    /// </summary>
    /// <example>
    /// <code>
    /// var count = query.CaptureFetchXml(xml => _logger.LogDebug(xml)).Count();
    /// </code>
    /// </example>
    public static IQueryable<TElement> CaptureFetchXml<TElement>(
        this IQueryable<TElement> queryable,
        Action<string> onFetchXml)
    {
        if (onFetchXml is null)
            throw new ArgumentNullException(nameof(onFetchXml));

        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(CaptureFetchXml),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(onFetchXml));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Registers a per-query transform invoked with each raw <see cref="Entity"/> row
    /// immediately before it is materialized into the result type. Return the entity to
    /// materialize (the same instance — <see cref="Entity"/> is mutable — or a
    /// replacement), or <c>null</c> to leave the row unchanged. Runs after (and therefore
    /// takes precedence over) the global
    /// <see cref="DataverseQueryDiagnostics.BeforeMaterialize"/> hook.
    /// </summary>
    /// <example>
    /// <code>
    /// var rows = query
    ///     .OnBeforeMaterialize(e => { e["computed"] = Derive(e); return e; })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TElement> OnBeforeMaterialize<TElement>(
        this IQueryable<TElement> queryable,
        Func<Entity, Entity?> onBeforeMaterialize)
    {
        if (onBeforeMaterialize is null)
            throw new ArgumentNullException(nameof(onBeforeMaterialize));

        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(OnBeforeMaterialize),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(onBeforeMaterialize));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    /// <summary>
    /// Registers a per-query transform invoked with the source <see cref="Entity"/> and
    /// the materialized result. Return the result to use (the same instance, mutated, or a
    /// replacement), or <c>null</c> to leave it unchanged. Runs after (and therefore takes
    /// precedence over) the global <see cref="DataverseQueryDiagnostics.AfterMaterialize"/> hook.
    /// </summary>
    /// <example>
    /// <code>
    /// var contacts = query
    ///     .OnAfterMaterialize((source, c) => { c.FullName = $"{c.FirstName} {c.LastName}"; return c; })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TElement> OnAfterMaterialize<TElement>(
        this IQueryable<TElement> queryable,
        Func<Entity, TElement, TElement?> onAfterMaterialize)
    {
        if (onAfterMaterialize is null)
            throw new ArgumentNullException(nameof(onAfterMaterialize));

        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(OnAfterMaterialize),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(onAfterMaterialize, typeof(Func<Entity, TElement, TElement?>)));

        return queryable.Provider.CreateQuery<TElement>(expression);
    }

    internal static bool IsDataverseProvider(Type providerType)
    {
        var type = providerType;
        while (type != null)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(DataverseQueryProvider<>))
                return true;
            type = type.BaseType;
        }
        return false;
    }
}

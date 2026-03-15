using XrmToolkit.Dataverse.Linq.Model;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace XrmToolkit.Dataverse.Linq;

/// <summary>
/// Extension methods for <see cref="IOrganizationService"/> that provide LINQ query
/// entry points, paging, FetchXml configuration, and aggregate operations.
/// </summary>
public static partial class ServiceClientExtensions
{
    /// <summary>
    /// Creates a <see cref="DataverseQueryable{T}"/> for the given entity type.
    /// The entity type must be decorated with <see cref="EntityLogicalNameAttribute"/>.
    /// </summary>
#if NETFRAMEWORK
    public static DataverseQueryable<T> Queryable<T>(this IOrganizationService service, params string[] columns)
#else
    public static DataverseQueryable<T> Queryable<T>(this IOrganizationServiceAsync service, params string[] columns)
#endif
        where T : Entity
    {
        var entityLogicalName = typeof(T).GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
            ?? throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' must be decorated with '{nameof(EntityLogicalNameAttribute)}'.");

        return new DataverseQueryable<T>(service, entityLogicalName, columns.Length > 0 ? columns : null);
    }

    /// <summary>
    /// Creates a <see cref="DataverseQueryable{Entity}"/> for unbound queries using the
    /// <see cref="Entity"/> base class directly (no proxy class required).
    /// </summary>
#if NETFRAMEWORK
    public static DataverseQueryable<Entity> Queryable(this IOrganizationService service, string entityLogicalName, params string[] columns)
#else
    public static DataverseQueryable<Entity> Queryable(this IOrganizationServiceAsync service, string entityLogicalName, params string[] columns)
#endif
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

using Dataverse.Linq.Model;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq;

/// <summary>
/// Extension methods for <see cref="IOrganizationServiceAsync"/> that provide LINQ query
/// entry points, async execution, paging, FetchXml configuration, and aggregate operations.
/// </summary>
public static class ServiceClientExtensions
{
    /// <summary>
    /// Creates a <see cref="DataverseQueryable{T}"/> for the given entity type.
    /// The entity type must be decorated with <see cref="EntityLogicalNameAttribute"/>.
    /// </summary>
    public static DataverseQueryable<T> Queryable<T>(this IOrganizationServiceAsync service, params string[] columns)
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
    public static DataverseQueryable<Entity> Queryable(this IOrganizationServiceAsync service, string entityLogicalName, params string[] columns)
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
    /// Asynchronously executes the query and returns all results as a <see cref="List{T}"/>.
    /// Works on both root queryables and projected queryables (e.g. after a Select clause).
    /// Handles paging automatically.
    /// </summary>
    public static Task<List<TElement>> ToListAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<List<TElement>>>(queryable.Expression, cancellationToken);

        return Task.FromResult(queryable.ToList());
    }

    /// <summary>
    /// Asynchronously executes the query page by page, invoking <paramref name="onPage"/>
    /// with each page of results as they are retrieved from Dataverse.
    /// </summary>
    public static Task ForEachPageAsync<TElement>(
        this IQueryable<TElement> queryable,
        Func<List<TElement>, Task> onPage,
        CancellationToken cancellationToken = default)
    {
        var providerType = queryable.Provider.GetType();
        if (providerType.IsGenericType &&
            providerType.GetGenericTypeDefinition() == typeof(DataverseQueryProvider<>))
        {
            return ((dynamic)queryable.Provider).ForEachPageAsync<TElement>(
                queryable.Expression, onPage, cancellationToken);
        }

        throw new InvalidOperationException(
            "ForEachPageAsync can only be used with Dataverse queryables created via Queryable<T>().");
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
        if (providerType.IsGenericType &&
            providerType.GetGenericTypeDefinition() == typeof(DataverseQueryProvider<>))
        {
            ((dynamic)queryable.Provider).ForEachPage<TElement>(queryable.Expression, onPage);
            return;
        }

        throw new InvalidOperationException(
            "ForEachPage can only be used with Dataverse queryables created via Queryable<T>().");
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence.
    /// Throws <see cref="InvalidOperationException"/> if the sequence is empty.
    /// </summary>
    public static Task<TElement> FirstAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.First),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);

        return Task.FromResult(queryable.First());
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence that satisfies the predicate.
    /// Throws <see cref="InvalidOperationException"/> if no element is found.
    /// </summary>
    public static Task<TElement> FirstAsync<TElement>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.First),
            [typeof(TElement)], queryable.Expression, predicate);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);

        return Task.FromResult(queryable.First(predicate));
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a default value if empty.
    /// </summary>
    public static Task<TElement?> FirstOrDefaultAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.FirstOrDefault),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);

        return Task.FromResult(queryable.FirstOrDefault());
    }

    /// <summary>
    /// Asynchronously returns the first element matching the predicate, or a default value if none found.
    /// </summary>
    public static Task<TElement?> FirstOrDefaultAsync<TElement>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.FirstOrDefault),
            [typeof(TElement)], queryable.Expression, predicate);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);

        return Task.FromResult(queryable.FirstOrDefault(predicate));
    }

    /// <summary>
    /// Asynchronously returns the only element of the sequence.
    /// Throws <see cref="InvalidOperationException"/> if the sequence does not contain exactly one element.
    /// </summary>
    public static Task<TElement> SingleAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Single),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);

        return Task.FromResult(queryable.Single());
    }

    /// <summary>
    /// Asynchronously returns the only element of the sequence that satisfies the predicate.
    /// Throws <see cref="InvalidOperationException"/> if zero or more than one element is found.
    /// </summary>
    public static Task<TElement> SingleAsync<TElement>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Single),
            [typeof(TElement)], queryable.Expression, predicate);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);

        return Task.FromResult(queryable.Single(predicate));
    }

    /// <summary>
    /// Asynchronously returns the only element of the sequence, or a default value if empty.
    /// Throws <see cref="InvalidOperationException"/> if more than one element is found.
    /// </summary>
    public static Task<TElement?> SingleOrDefaultAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.SingleOrDefault),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);

        return Task.FromResult(queryable.SingleOrDefault());
    }

    /// <summary>
    /// Asynchronously returns the only element matching the predicate, or a default value if none found.
    /// Throws <see cref="InvalidOperationException"/> if more than one element matches.
    /// </summary>
    public static Task<TElement?> SingleOrDefaultAsync<TElement>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.SingleOrDefault),
            [typeof(TElement)], queryable.Expression, predicate);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);

        return Task.FromResult(queryable.SingleOrDefault(predicate));
    }

    // -------------------------------------------------------------------------
    // Async aggregate operators
    // -------------------------------------------------------------------------

    /// <summary>
    /// Asynchronously returns the number of elements in the sequence.
    /// </summary>
    public static Task<int> CountAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Count),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);

        return Task.FromResult(queryable.Count());
    }

    /// <summary>
    /// Asynchronously returns the number of elements in the sequence that satisfy a condition.
    /// </summary>
    public static Task<int> CountAsync<TElement>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Count),
            [typeof(TElement)], queryable.Expression, predicate);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);

        return Task.FromResult(queryable.Count(predicate));
    }

    /// <summary>
    /// Asynchronously returns the number of elements in the sequence as a <see cref="long"/>.
    /// </summary>
    public static Task<long> LongCountAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.LongCount),
            [typeof(TElement)], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<long>>(expression, cancellationToken);

        return Task.FromResult(queryable.LongCount());
    }

    /// <summary>
    /// Asynchronously returns the minimum value using a selector.
    /// </summary>
    public static Task<TResult> MinAsync<TElement, TResult>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, TResult>> selector,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Min),
            [typeof(TElement), typeof(TResult)],
            queryable.Expression, selector);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TResult>>(expression, cancellationToken);

        return Task.FromResult(queryable.Min(selector))!;
    }

    /// <summary>
    /// Asynchronously returns the maximum value using a selector.
    /// </summary>
    public static Task<TResult> MaxAsync<TElement, TResult>(
        this IQueryable<TElement> queryable,
        Expression<Func<TElement, TResult>> selector,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Max),
            [typeof(TElement), typeof(TResult)],
            queryable.Expression, selector);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<TResult>>(expression, cancellationToken);

        return Task.FromResult(queryable.Max(selector))!;
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of <see cref="int"/> values.
    /// </summary>
    public static Task<int> SumAsync(
        this IQueryable<int> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);

        return Task.FromResult(queryable.Sum());
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of nullable <see cref="int"/> values.
    /// </summary>
    public static Task<int?> SumAsync(
        this IQueryable<int?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<int?>>(expression, cancellationToken);

        return Task.FromResult(queryable.Sum());
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal> SumAsync(
        this IQueryable<decimal> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<decimal>>(expression, cancellationToken);

        return Task.FromResult(queryable.Sum());
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of nullable <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal?> SumAsync(
        this IQueryable<decimal?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<decimal?>>(expression, cancellationToken);

        return Task.FromResult(queryable.Sum());
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of <see cref="int"/> values.
    /// </summary>
    public static Task<double> AverageAsync(
        this IQueryable<int> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<double>>(expression, cancellationToken);

        return Task.FromResult(queryable.Average());
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of nullable <see cref="int"/> values.
    /// </summary>
    public static Task<double?> AverageAsync(
        this IQueryable<int?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<double?>>(expression, cancellationToken);

        return Task.FromResult(queryable.Average());
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal> AverageAsync(
        this IQueryable<decimal> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<decimal>>(expression, cancellationToken);

        return Task.FromResult(queryable.Average());
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of nullable <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal?> AverageAsync(
        this IQueryable<decimal?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(System.Linq.Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<decimal?>>(expression, cancellationToken);

        return Task.FromResult(queryable.Average());
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
    /// Asynchronously counts non-null values of the column projected by a preceding <c>Select</c>.
    /// Translates to FetchXml <c>aggregate="countcolumn"</c>.
    /// </summary>
    public static Task<int> CountColumnAsync<TSource>(
        this IQueryable<TSource> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(CountColumn),
            [typeof(TSource)],
            queryable.Expression);

        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);

        return Task.FromResult(queryable.Provider.Execute<int>(expression));
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

        if (providerType.IsGenericType &&
            providerType.GetGenericTypeDefinition() == typeof(DataverseQueryProvider<>))
        {
            return ((dynamic)queryable.Provider).GenerateFetchXml(queryable.Expression);
        }

        throw new InvalidOperationException(
            "ToFetchXml can only be used with Dataverse queryables created via Queryable<T>().");
    }
}

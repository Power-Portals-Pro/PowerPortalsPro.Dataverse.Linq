#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq;

public static partial class ServiceClientExtensions
{
    /// <summary>
    /// Creates a <see cref="IQueryable{T}"/> for the given entity type.
    /// The entity type must be decorated with <see cref="EntityLogicalNameAttribute"/>.
    /// </summary>
    public static IQueryable<T> Queryable<T>(this IOrganizationServiceAsync service, params string[] columns)
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
    public static IQueryable<Entity> Queryable(this IOrganizationServiceAsync service, string entityLogicalName, params string[] columns)
    {
        return new DataverseQueryable<Entity>(service, entityLogicalName, columns.Length > 0 ? columns : null);
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
        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<List<TElement>>>(queryable.Expression, cancellationToken);
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
        return ((dynamic)queryable.Provider).ForEachPageAsync<TElement>(
            queryable.Expression, onPage, cancellationToken);
    }

    /// <summary>
    /// Adds the FetchXml <c>returntotalrecordcount</c> attribute to the query and registers
    /// an async callback that will be invoked with the total record count after the first page of
    /// results is retrieved from Dataverse.
    /// </summary>
    public static IQueryable<TElement> ReturnRecordCountAsync<TElement>(
        this IQueryable<TElement> queryable,
        Func<RecordCountArguments, Task> onRecordCount)
    {
        var expression = Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(ReturnRecordCountAsync),
            [typeof(TElement)],
            queryable.Expression,
            Expression.Constant(onRecordCount));

        return queryable.Provider.CreateQuery<TElement>(expression);
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
            typeof(Queryable), nameof(System.Linq.Queryable.First),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.First),
            [typeof(TElement)], queryable.Expression, predicate);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a default value if empty.
    /// </summary>
    public static Task<TElement?> FirstOrDefaultAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.FirstOrDefault),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.FirstOrDefault),
            [typeof(TElement)], queryable.Expression, predicate);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Single),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Single),
            [typeof(TElement)], queryable.Expression, predicate);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.SingleOrDefault),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.SingleOrDefault),
            [typeof(TElement)], queryable.Expression, predicate);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TElement?>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Count),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Count),
            [typeof(TElement)], queryable.Expression, predicate);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously returns the number of elements in the sequence as a <see cref="long"/>.
    /// </summary>
    public static Task<long> LongCountAsync<TElement>(
        this IQueryable<TElement> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.LongCount),
            [typeof(TElement)], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<long>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Min),
            [typeof(TElement), typeof(TResult)],
            queryable.Expression, selector);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TResult>>(expression, cancellationToken);
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
            typeof(Queryable), nameof(System.Linq.Queryable.Max),
            [typeof(TElement), typeof(TResult)],
            queryable.Expression, selector);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<TResult>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of <see cref="int"/> values.
    /// </summary>
    public static Task<int> SumAsync(
        this IQueryable<int> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of nullable <see cref="int"/> values.
    /// </summary>
    public static Task<int?> SumAsync(
        this IQueryable<int?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<int?>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal> SumAsync(
        this IQueryable<decimal> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<decimal>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the sum of a sequence of nullable <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal?> SumAsync(
        this IQueryable<decimal?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Sum),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<decimal?>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of <see cref="int"/> values.
    /// </summary>
    public static Task<double> AverageAsync(
        this IQueryable<int> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<double>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of nullable <see cref="int"/> values.
    /// </summary>
    public static Task<double?> AverageAsync(
        this IQueryable<int?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<double?>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal> AverageAsync(
        this IQueryable<decimal> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<decimal>>(expression, cancellationToken);
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence of nullable <see cref="decimal"/> values.
    /// </summary>
    public static Task<decimal?> AverageAsync(
        this IQueryable<decimal?> queryable,
        CancellationToken cancellationToken = default)
    {
        var expression = Expression.Call(
            typeof(Queryable), nameof(System.Linq.Queryable.Average),
            [], queryable.Expression);

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<decimal?>>(expression, cancellationToken);
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

        var asyncProvider = (IAsyncQueryProvider)queryable.Provider;
        return asyncProvider.ExecuteAsync<Task<int>>(expression, cancellationToken);
    }
}
#endif

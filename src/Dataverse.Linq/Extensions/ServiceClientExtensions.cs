using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq;

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

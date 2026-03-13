using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
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

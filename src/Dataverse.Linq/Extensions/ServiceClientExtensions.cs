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
    public static DataverseQueryable<T> Queryable<T>(this IOrganizationServiceAsync service)
        where T : Entity
    {
        var entityLogicalName = typeof(T).GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
            ?? throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' must be decorated with '{nameof(EntityLogicalNameAttribute)}'.");

        return new DataverseQueryable<T>(service, entityLogicalName);
    }

    /// <summary>
    /// Asynchronously executes the query and returns all results as a <see cref="List{T}"/>.
    /// Handles paging automatically.
    /// </summary>
    public static Task<List<T>> ToListAsync<T>(
        this DataverseQueryable<T> queryable,
        CancellationToken cancellationToken = default)
        where T : Entity
    {
        if (queryable.Provider is IAsyncQueryProvider asyncProvider)
            return asyncProvider.ExecuteAsync<List<T>>(queryable.Expression, cancellationToken);

        return Task.FromResult(queryable.ToList());
    }
}

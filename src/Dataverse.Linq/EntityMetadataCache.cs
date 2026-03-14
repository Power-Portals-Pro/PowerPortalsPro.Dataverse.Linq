using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Concurrent;

namespace Dataverse.Linq;

/// <summary>
/// Caches entity primary key attribute names to avoid repeated metadata queries.
/// Used when LINQ expressions reference <see cref="Entity.Id"/> directly,
/// which requires knowing the actual primary key column name for FetchXml generation.
/// </summary>
internal static class EntityMetadataCache
{
    private static readonly ConcurrentDictionary<string, string> PrimaryKeyCache = new();

    /// <summary>
    /// Gets the primary key attribute name for the specified entity, querying Dataverse
    /// metadata on the first call and caching the result for subsequent calls.
    /// </summary>
    internal static string GetPrimaryIdAttribute(IOrganizationService service, string entityLogicalName)
    {
        return PrimaryKeyCache.GetOrAdd(entityLogicalName, name =>
        {
            var request = new RetrieveEntityRequest
            {
                LogicalName = name,
                EntityFilters = EntityFilters.Entity,
            };
            var response = (RetrieveEntityResponse)service.Execute(request);
            return response.EntityMetadata.PrimaryIdAttribute;
        });
    }

    /// <summary>
    /// Clears all cached metadata. Primarily useful for testing.
    /// </summary>
    internal static void Clear() => PrimaryKeyCache.Clear();
}

using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Helper methods for extracting aggregate/grouped query results from
/// Dataverse Entity rows. Called by compiled projectors built in the translator.
/// </summary>
internal static class AggregateProjection
{
    /// <summary>
    /// Extracts a root-entity attribute value with proper type unwrapping.
    /// Handles OptionSetValue → enum, Money → decimal, EntityReference → Guid.
    /// Used by join projectors where the raw Entity isn't strongly-typed.
    /// </summary>
    public static T ExtractRootValue<T>(Entity entity, string attributeName)
    {
        var raw = entity.Attributes.TryGetValue(attributeName, out var val) ? val : null;
        if (raw == null) return default!;

        return ConvertRawValue<T>(raw);
    }

    public static T ExtractValue<T>(Entity entity, string alias)
    {
        var av = entity.GetAttributeValue<AliasedValue>(alias);
        if (av?.Value == null) return default!;

        return ConvertRawValue<T>(av.Value);
    }

    /// <summary>
    /// Reconstructs a linked entity from AliasedValue entries with the given alias prefix.
    /// Returns <c>null</c> when no aliased values match (left-join, no match).
    /// </summary>
    public static T? ExtractLinkedEntity<T>(Entity entity, string alias) where T : Entity, new()
    {
        var prefix = alias + ".";
        T? result = null;

        foreach (var kvp in entity.Attributes)
        {
            if (kvp.Key.StartsWith(prefix) && kvp.Value is AliasedValue av)
            {
                result ??= new T { LogicalName = av.EntityLogicalName, Id = Guid.Empty };
                var attrName = kvp.Key.Substring(prefix.Length);
                result.Attributes[attrName] = av.Value;

                // Set the entity Id if this is the primary key
                if (attrName == av.EntityLogicalName + "id" && av.Value is Guid id)
                    result.Id = id;
            }
        }

        return result;
    }

    private static T ConvertRawValue<T>(object raw)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        // Direct cast when already the right type (e.g. EntityReference → EntityReference)
        if (targetType.IsInstanceOfType(raw))
            return (T)raw;

        // Unwrap Dataverse wrapper types to their underlying CLR values
        if (raw is Money m) raw = m.Value;
        else if (raw is OptionSetValue osv) raw = osv.Value;
        else if (raw is EntityReference er) raw = er.Id;

        // Check again after unwrapping
        if (targetType.IsInstanceOfType(raw))
            return (T)raw;

        // Handle int → enum conversion (e.g. OptionSetValue → AccountRating_Enum)
        if (targetType.IsEnum)
            return (T)Enum.ToObject(targetType, raw);

        return (T)Convert.ChangeType(raw, targetType);
    }
}

using Microsoft.Xrm.Sdk;

namespace Dataverse.Linq;

/// <summary>
/// Helper methods for extracting aggregate/grouped query results from
/// Dataverse Entity rows. Called by compiled projectors built in the translator.
/// </summary>
internal static class AggregateProjection
{
    public static T ExtractValue<T>(Entity entity, string alias)
    {
        var av = entity.GetAttributeValue<AliasedValue>(alias);
        if (av?.Value == null) return default!;

        var raw = av.Value;

        // Unwrap Dataverse wrapper types to their underlying CLR values
        if (raw is Money m) raw = m.Value;
        else if (raw is OptionSetValue osv) raw = osv.Value;
        else if (raw is EntityReference er) raw = er.Id;

        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        // Direct cast when already the right type (avoids IConvertible requirement)
        if (targetType.IsInstanceOfType(raw))
            return (T)raw;

        return (T)Convert.ChangeType(raw, targetType);
    }
}

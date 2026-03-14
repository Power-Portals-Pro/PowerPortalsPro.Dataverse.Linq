using Microsoft.Xrm.Sdk;

namespace Dataverse.Linq.Extensions;

/// <summary>
/// Extension methods for multi-select option set fields that are translated into
/// FetchXml condition operators by the LINQ query provider.
/// </summary>
public static class MultiSelectExtensions
{
    private static bool Throw() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");


    /// <summary>
    /// Determines if the collection contains one or more of the specified values.
    /// Translates to FetchXml <c>contain-values</c> (or <c>not-contain-values</c> when negated).
    /// </summary>
    public static bool ContainsValues<T>(this OptionSetValueCollection collection, params T[] values)
    {
        return collection.Select(x => (T)(object)x.Value).ContainsValues(values);
    }

    /// <summary>
    /// Determines if the collection contains one or more of the specified values.
    /// Translates to FetchXml <c>contain-values</c> (or <c>not-contain-values</c> when negated).
    /// </summary>
    public static bool ContainsValues<T>(this IEnumerable<T> collection, params T[] values)
    {
        if (collection == null && (values?.Length ?? 0) > 0)
            return false;
        else if (collection != null && (values?.Length ?? 0) == 0)
            return false;

        return collection!.Intersect(values).Any();
    }

    /// <summary>
    /// Determines if there is an exact match with the same specified values.
    /// Translates to FetchXml <c>eq</c> / <c>ne</c> for a single value,
    /// or <c>in</c> / <c>not-in</c> for multiple values.
    /// </summary>
    public static bool Equals<T>(this OptionSetValueCollection collection, params T[] values) => Throw();

    /// <summary>
    /// Determines if there is an exact match with the same specified values.
    /// Translates to FetchXml <c>eq</c> / <c>ne</c> for a single value,
    /// or <c>in</c> / <c>not-in</c> for multiple values.
    /// </summary>
    public static bool Equals<T>(this IEnumerable<T> collection, params T[] values) => Throw();
}

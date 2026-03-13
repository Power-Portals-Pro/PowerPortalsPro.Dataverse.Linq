using Microsoft.Xrm.Sdk;

namespace Dataverse.Linq.Extensions;

/// <summary>
/// Placeholder extension methods for <see cref="OptionSetValueCollection"/> that are
/// translated into FetchXml contain-values / not-contain-values condition operators
/// by the LINQ query provider. These methods should only be used inside LINQ Where
/// clauses — they throw at runtime if invoked directly.
/// </summary>
public static class MultiSelectExtensions
{
    private static bool Throw() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");

    public static bool ContainValues(this OptionSetValueCollection collection, params int[] values) => Throw();

    public static bool DoesNotContainValues(this OptionSetValueCollection collection, params int[] values) => Throw();
}

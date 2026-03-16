namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Placeholder extension methods for Guid/Guid? that are translated into
/// FetchXml user/business unit condition operators by the LINQ query provider.
/// These methods should only be used inside LINQ Where clauses — they throw at
/// runtime if invoked directly.
/// </summary>
public static class UserExtensions
{
    private static bool Throw() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");

    /// <summary>
    /// Translates to the FetchXml <c>eq-userid</c> condition operator.
    /// </summary>
    public static bool EqualUserId(this Guid id) => Throw();
    /// <inheritdoc cref="EqualUserId(Guid)" />
    public static bool EqualUserId(this Guid? id) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>ne-userid</c> condition operator.
    /// </summary>
    public static bool NotEqualUserId(this Guid id) => Throw();
    /// <inheritdoc cref="NotEqualUserId(Guid)" />
    public static bool NotEqualUserId(this Guid? id) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>eq-businessid</c> condition operator.
    /// </summary>
    public static bool EqualBusinessId(this Guid id) => Throw();
    /// <inheritdoc cref="EqualBusinessId(Guid)" />
    public static bool EqualBusinessId(this Guid? id) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>ne-businessid</c> condition operator.
    /// </summary>
    public static bool NotEqualBusinessId(this Guid id) => Throw();
    /// <inheritdoc cref="NotEqualBusinessId(Guid)" />
    public static bool NotEqualBusinessId(this Guid? id) => Throw();
}

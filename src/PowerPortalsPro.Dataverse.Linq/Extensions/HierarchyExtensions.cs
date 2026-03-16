namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Placeholder extension methods for Guid/Guid? that are translated into
/// FetchXml hierarchy condition operators by the LINQ query provider. These methods
/// should only be used inside LINQ Where clauses — they throw at runtime if invoked directly.
/// </summary>
public static class HierarchyExtensions
{
    private static bool Throw() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");

    // -------------------------------------------------------------------------
    // Hierarchy operators (single Guid argument — the record to compare against)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Translates to the FetchXml <c>above</c> hierarchy condition operator.
    /// Returns <see langword="true"/> when the record is above <paramref name="value"/> in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    /// <param name="value">The record identifier to compare against.</param>
    public static bool Above(this Guid id, Guid value) => Throw();
    /// <inheritdoc cref="Above(Guid, Guid)" />
    public static bool Above(this Guid? id, Guid value) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>eq-or-above</c> hierarchy condition operator.
    /// Returns <see langword="true"/> when the record is equal to or above <paramref name="value"/> in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    /// <param name="value">The record identifier to compare against.</param>
    public static bool AboveOrEqual(this Guid id, Guid value) => Throw();
    /// <inheritdoc cref="AboveOrEqual(Guid, Guid)" />
    public static bool AboveOrEqual(this Guid? id, Guid value) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>under</c> hierarchy condition operator.
    /// Returns <see langword="true"/> when the record is under <paramref name="value"/> in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    /// <param name="value">The record identifier to compare against.</param>
    public static bool Under(this Guid id, Guid value) => Throw();
    /// <inheritdoc cref="Under(Guid, Guid)" />
    public static bool Under(this Guid? id, Guid value) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>eq-or-under</c> hierarchy condition operator.
    /// Returns <see langword="true"/> when the record is equal to or under <paramref name="value"/> in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    /// <param name="value">The record identifier to compare against.</param>
    public static bool UnderOrEqual(this Guid id, Guid value) => Throw();
    /// <inheritdoc cref="UnderOrEqual(Guid, Guid)" />
    public static bool UnderOrEqual(this Guid? id, Guid value) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>not-under</c> hierarchy condition operator.
    /// Returns <see langword="true"/> when the record is not under <paramref name="value"/> in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    /// <param name="value">The record identifier to compare against.</param>
    public static bool NotUnder(this Guid id, Guid value) => Throw();
    /// <inheritdoc cref="NotUnder(Guid, Guid)" />
    public static bool NotUnder(this Guid? id, Guid value) => Throw();

    // -------------------------------------------------------------------------
    // User hierarchy operators (parameterless — checks against the current user)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Translates to the FetchXml <c>eq-useroruserhierarchy</c> condition operator.
    /// Returns <see langword="true"/> when the record belongs to the current user or a user above them in the hierarchy.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    public static bool EqualUserOrUserHierarchy(this Guid id) => Throw();
    /// <inheritdoc cref="EqualUserOrUserHierarchy(Guid)" />
    public static bool EqualUserOrUserHierarchy(this Guid? id) => Throw();

    /// <summary>
    /// Translates to the FetchXml <c>eq-useroruserhierarchyandteams</c> condition operator.
    /// Returns <see langword="true"/> when the record belongs to the current user, a user above them in the hierarchy, or their teams.
    /// </summary>
    /// <param name="id">The identifier to test.</param>
    public static bool EqualUserOrUserHierarchyAndTeams(this Guid id) => Throw();
    /// <inheritdoc cref="EqualUserOrUserHierarchyAndTeams(Guid)" />
    public static bool EqualUserOrUserHierarchyAndTeams(this Guid? id) => Throw();
}

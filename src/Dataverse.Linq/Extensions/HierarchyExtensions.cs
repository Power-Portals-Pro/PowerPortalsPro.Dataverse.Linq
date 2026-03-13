namespace Dataverse.Linq.Extensions;

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

    public static bool Above(this Guid id, Guid value) => Throw();
    public static bool Above(this Guid? id, Guid value) => Throw();

    public static bool AboveOrEqual(this Guid id, Guid value) => Throw();
    public static bool AboveOrEqual(this Guid? id, Guid value) => Throw();

    public static bool Under(this Guid id, Guid value) => Throw();
    public static bool Under(this Guid? id, Guid value) => Throw();

    public static bool UnderOrEqual(this Guid id, Guid value) => Throw();
    public static bool UnderOrEqual(this Guid? id, Guid value) => Throw();

    public static bool NotUnder(this Guid id, Guid value) => Throw();
    public static bool NotUnder(this Guid? id, Guid value) => Throw();

    // -------------------------------------------------------------------------
    // User hierarchy operators (parameterless — checks against the current user)
    // -------------------------------------------------------------------------

    public static bool EqualUserOrUserHierarchy(this Guid id) => Throw();
    public static bool EqualUserOrUserHierarchy(this Guid? id) => Throw();

    public static bool EqualUserOrUserHierarchyAndTeams(this Guid id) => Throw();
    public static bool EqualUserOrUserHierarchyAndTeams(this Guid? id) => Throw();
}

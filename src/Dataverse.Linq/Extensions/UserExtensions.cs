namespace Dataverse.Linq.Extensions;

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

    public static bool EqualUserId(this Guid id) => Throw();
    public static bool EqualUserId(this Guid? id) => Throw();

    public static bool NotEqualUserId(this Guid id) => Throw();
    public static bool NotEqualUserId(this Guid? id) => Throw();

    public static bool EqualBusinessId(this Guid id) => Throw();
    public static bool EqualBusinessId(this Guid? id) => Throw();

    public static bool NotEqualBusinessId(this Guid id) => Throw();
    public static bool NotEqualBusinessId(this Guid? id) => Throw();
}

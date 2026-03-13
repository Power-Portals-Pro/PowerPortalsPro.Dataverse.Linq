namespace Dataverse.Linq.Model;

/// <summary>
/// Represents a &lt;condition attribute="..." operator="..." value="..."&gt; element.
/// </summary>
internal sealed class FetchCondition
{
    /// <summary>
    /// The alias of the linked entity this condition applies to,
    /// or null if it applies to the root entity.
    /// </summary>
    public string? EntityAlias { get; set; }

    public string Attribute { get; set; } = null!;

    /// <summary>
    /// FetchXml operator: eq, ne, lt, le, gt, ge, null, not-null,
    /// like, not-like, in, not-in, etc.
    /// </summary>
    public string Operator { get; set; } = null!;

    public object? Value { get; set; }

    /// <summary>
    /// Multiple values for in / not-in operators.
    /// </summary>
    public List<object> Values { get; } = [];
}

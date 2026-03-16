using Microsoft.Xrm.Sdk.Query;

namespace PowerPortalsPro.Dataverse.Linq.Model;

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

    public ConditionOperator Operator { get; set; }

    public object? Value { get; set; }

    /// <summary>
    /// When set, compares the attribute against another column value
    /// using the FetchXml <c>valueof</c> attribute instead of <c>value</c>.
    /// For cross-table comparisons, use the format "alias.columnname".
    /// </summary>
    public string? ValueOf { get; set; }

    /// <summary>
    /// Multiple values for in / not-in operators.
    /// </summary>
    public List<object> Values { get; } = [];
}

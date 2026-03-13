namespace Dataverse.Linq.Model;

/// <summary>
/// Represents a &lt;filter type="and|or"&gt; element with nested conditions
/// and sub-filters.
/// </summary>
internal sealed class FetchFilter
{
    public FilterType Type { get; set; } = FilterType.And;
    public List<FetchCondition> Conditions { get; } = [];
    public List<FetchFilter> Filters { get; } = [];
}

internal enum FilterType
{
    And,
    Or
}

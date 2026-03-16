namespace PowerPortalsPro.Dataverse.Linq.Model;

/// <summary>
/// Represents a &lt;filter type="and|or"&gt; element with nested conditions
/// and sub-filters.
/// </summary>
internal sealed class FetchFilter
{
    public FilterType Type { get; set; } = FilterType.And;
    public List<FetchCondition> Conditions { get; } = [];
    public List<FetchFilter> Filters { get; } = [];

    /// <summary>
    /// Link-entities nested inside this filter, used for
    /// <c>link-type="any"</c> / <c>"not any"</c> / <c>"all"</c> / <c>"not all"</c>.
    /// </summary>
    public List<FetchLinkEntity> Links { get; } = [];
}

internal enum FilterType
{
    And,
    Or
}

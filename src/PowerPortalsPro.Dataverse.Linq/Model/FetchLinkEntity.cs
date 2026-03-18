namespace PowerPortalsPro.Dataverse.Linq.Model;

/// <summary>
/// Represents a &lt;link-entity name="..." from="..." to="..." alias="..."
/// link-type="inner|outer"&gt; element with its own attributes, filters,
/// and nested links.
/// </summary>
internal sealed class FetchLinkEntity
{
    public string Name { get; set; } = null!;
    public string From { get; set; } = null!;
    public string To { get; set; } = null!;
    public string Alias { get; set; } = null!;
    public string LinkType { get; set; } = "inner";
    public List<FetchAttribute> Attributes { get; } = [];
    public bool AllAttributes { get; set; }
    public List<FetchOrder> Orders { get; } = [];
    public FetchFilter? Filter { get; set; }
    public List<FetchLinkEntity> Links { get; } = [];
}

namespace Dataverse.Linq.Model;

/// <summary>
/// Represents a single &lt;attribute&gt; element in FetchXml.
/// </summary>
internal sealed class FetchAttribute
{
    public string Name { get; set; } = null!;
    public string? Alias { get; set; }
    public string? Aggregate { get; set; }
    public bool GroupBy { get; set; }
    public string? DateGrouping { get; set; }
}

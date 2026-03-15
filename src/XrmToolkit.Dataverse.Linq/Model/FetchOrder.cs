namespace XrmToolkit.Dataverse.Linq.Model;

/// <summary>
/// Represents an &lt;order attribute="..." descending="true|false"&gt; element.
/// </summary>
internal sealed class FetchOrder
{
    public string? EntityAlias { get; set; }
    public string Attribute { get; set; } = null!;
    public string? Alias { get; set; }
    public bool Descending { get; set; }
}

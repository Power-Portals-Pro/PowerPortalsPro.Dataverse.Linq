namespace Dataverse.Linq.Model;

/// <summary>
/// Represents an &lt;order attribute="..." descending="true|false"&gt; element.
/// </summary>
internal sealed class FetchOrder
{
    public string Attribute { get; set; } = null!;
    public bool Descending { get; set; }
}

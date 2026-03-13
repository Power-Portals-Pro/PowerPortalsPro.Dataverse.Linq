namespace Dataverse.Linq.Model;

/// <summary>
/// Root query object representing a complete FetchXml query.
/// One instance per LINQ query, populated by the translator
/// and serialised by the builder.
/// </summary>
internal sealed class FetchXmlQuery
{
    public string EntityLogicalName { get; set; } = null!;
    public List<FetchAttribute> Attributes { get; } = [];
    public bool AllAttributes { get; set; } = true;
    public FetchFilter? Filter { get; set; }
    public List<FetchOrder> Orders { get; } = [];
    public List<FetchLinkEntity> Links { get; } = [];
    public int? Top { get; set; }
    public int? Skip { get; set; }
    public bool Distinct { get; set; }
    public bool Aggregate { get; set; }

    /// <summary>
    /// Compiled delegate that projects a raw Entity (or join result) into the
    /// final TElement shape requested by the LINQ query.
    /// </summary>
    public Delegate? Projector { get; set; }

    /// <summary>
    /// The CLR type that the projector produces.
    /// </summary>
    public Type? ProjectionType { get; set; }
}

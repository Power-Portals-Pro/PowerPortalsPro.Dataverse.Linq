namespace Dataverse.Linq.Model;

/// <summary>
/// Indicates which terminal LINQ operator was used.
/// </summary>
internal enum QueryTerminalOperator
{
    List,
    First,
    FirstOrDefault,
    Single,
    SingleOrDefault,
}

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
    /// The terminal operator that determines how the result set is returned
    /// (e.g. First, Single). Defaults to <see cref="QueryTerminalOperator.List"/>.
    /// </summary>
    public QueryTerminalOperator TerminalOperator { get; set; } = QueryTerminalOperator.List;

    /// <summary>
    /// Compiled delegate that projects a raw Entity (or join result) into the
    /// final TElement shape requested by the LINQ query.
    /// For simple selects and left joins: single-parameter (outer entity → result).
    /// For inner joins: two-parameter (outer entity, inner entity → result).
    /// </summary>
    public Delegate? Projector { get; set; }

    /// <summary>
    /// The CLR type that the projector produces.
    /// </summary>
    public Type? ProjectionType { get; set; }

    /// <summary>
    /// For inner joins: the CLR type of the inner/linked entity.
    /// The provider uses this to extract linked-entity attributes and convert
    /// them via <c>Entity.ToEntity&lt;T&gt;()</c> before passing to the
    /// two-parameter <see cref="Projector"/> delegate.
    /// </summary>
    public Type? InnerEntityType { get; set; }
}

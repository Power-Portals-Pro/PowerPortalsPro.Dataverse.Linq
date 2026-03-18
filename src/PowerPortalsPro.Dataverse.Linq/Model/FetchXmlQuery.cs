namespace PowerPortalsPro.Dataverse.Linq.Model;

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
    Min,
    Max,
    Sum,
    Average,
    Count,
    LongCount,
    CountColumn,
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
    public int? AggregateLimit { get; set; }
    public FetchDatasource? Datasource { get; set; }
    public bool LateMaterialize { get; set; }
    public bool NoLock { get; set; }
    public List<SqlQueryHint>? QueryHints { get; set; }
    public bool UseRawOrderBy { get; set; }
    public int? PageSize { get; set; }
    public int? Page { get; set; }
    public bool Distinct { get; set; }
    public bool Aggregate { get; set; }

    /// <summary>
    /// The terminal operator that determines how the result set is returned
    /// (e.g. First, Single). Defaults to <see cref="QueryTerminalOperator.List"/>.
    /// </summary>
    public QueryTerminalOperator TerminalOperator { get; set; } = QueryTerminalOperator.List;

    /// <summary>
    /// Materializer-based projection. When set, the provider extracts data from
    /// each raw <c>Entity</c> result according to the materializer's slots and
    /// calls the compiled projector to produce the final <c>TElement</c>.
    /// </summary>
    public MaterializerInfo? Materializer { get; set; }
}

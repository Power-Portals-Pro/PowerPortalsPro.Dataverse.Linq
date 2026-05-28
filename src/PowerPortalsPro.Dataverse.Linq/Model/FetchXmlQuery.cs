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
    public bool ReturnTotalRecordCount { get; set; }
    public Action<RecordCountArguments>? OnRecordCount { get; set; }
    public Func<RecordCountArguments, Task>? OnRecordCountAsync { get; set; }

    /// <summary>
    /// Per-query callback invoked with the exact FetchXml of each request immediately
    /// before it is sent to Dataverse. Fires once per page for multi-page queries, with
    /// the page number and paging-cookie embedded in the captured FetchXml. Registered
    /// via <c>CaptureFetchXml(...)</c>.
    /// </summary>
    public Action<string>? OnFetchXml { get; set; }

    /// <summary>
    /// Per-query transform invoked with each raw <see cref="Microsoft.Xrm.Sdk.Entity"/>
    /// row before it is materialized into the result type. Returns the entity to
    /// materialize (the same instance, mutated, or a replacement); returning <c>null</c>
    /// leaves the row unchanged. Registered via <c>OnBeforeMaterialize(...)</c>.
    /// </summary>
    public Func<Microsoft.Xrm.Sdk.Entity, Microsoft.Xrm.Sdk.Entity?>? OnBeforeMaterialize { get; set; }

    /// <summary>
    /// Per-query transform invoked with the source <see cref="Microsoft.Xrm.Sdk.Entity"/>
    /// and the materialized result. Stored as a <see cref="Delegate"/> of type
    /// <c>Func&lt;Entity, TElement, TElement&gt;</c> (the element type is not known to this
    /// non-generic model) and cast back by the provider during materialization. Returning
    /// <c>null</c> leaves the result unchanged. Registered via <c>OnAfterMaterialize(...)</c>.
    /// </summary>
    public Delegate? OnAfterMaterialize { get; set; }

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

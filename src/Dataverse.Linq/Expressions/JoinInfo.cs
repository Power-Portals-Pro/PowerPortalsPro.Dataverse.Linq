namespace Dataverse.Linq.Expressions;

/// <summary>
/// Describes a join query. <see cref="IsOuterJoin"/> = false for inner joins,
/// true for left-outer joins.
/// </summary>
internal record JoinInfo(
    string OuterEntityLogicalName,
    string OuterKeyAttribute,
    string InnerEntityLogicalName,
    string InnerKeyAttribute,
    string InnerAlias,
    bool IsOuterJoin,
    IReadOnlyList<string>? OuterColumns,
    IReadOnlyList<string>? InnerColumns,    // inner join only
    Delegate? ResultSelector,               // inner join: (outer, inner) → TElement
    Delegate? Projector,                    // left join: (outer) → TElement
    bool FilterWhereInnerIsNull,            // left join only
    Type? InnerEntityType                   // inner join only
);

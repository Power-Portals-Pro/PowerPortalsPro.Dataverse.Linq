namespace Dataverse.Linq.Expressions;

internal record LeftJoinInfo(
    string OuterEntityLogicalName,
    string OuterKeyAttribute,
    string InnerEntityLogicalName,
    string InnerKeyAttribute,
    string InnerAlias,
    IReadOnlyList<string>? OuterColumns,
    bool FilterWhereInnerIsNull,
    Delegate? Projector
);

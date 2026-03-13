namespace Dataverse.Linq.Expressions;

internal record JoinInfo(
    string OuterEntityLogicalName,
    string OuterKeyAttribute,
    string InnerEntityLogicalName,
    string InnerKeyAttribute,
    string InnerAlias,
    IReadOnlyList<string>? OuterColumns,
    IReadOnlyList<string>? InnerColumns,
    Delegate ResultSelector,
    Type InnerEntityType
);

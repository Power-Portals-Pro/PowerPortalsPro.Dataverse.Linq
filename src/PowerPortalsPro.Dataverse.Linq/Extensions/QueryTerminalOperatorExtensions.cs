using PowerPortalsPro.Dataverse.Linq.Model;

namespace PowerPortalsPro.Dataverse.Linq;

internal static class QueryTerminalOperatorExtensions
{
    public static bool IsAggregate(this QueryTerminalOperator op) =>
        op is QueryTerminalOperator.Min or QueryTerminalOperator.Max
            or QueryTerminalOperator.Sum or QueryTerminalOperator.Average
            or QueryTerminalOperator.Count or QueryTerminalOperator.LongCount
            or QueryTerminalOperator.CountColumn;

    public static bool IsScalar(this QueryTerminalOperator op) =>
        op is not QueryTerminalOperator.List && !op.IsAggregate();
}

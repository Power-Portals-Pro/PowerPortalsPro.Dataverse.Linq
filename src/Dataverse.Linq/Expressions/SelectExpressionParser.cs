using Microsoft.Xrm.Sdk;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

internal static class SelectExpressionParser
{
    /// <summary>
    /// Inspects a LINQ expression tree for a Select call and returns the attribute names
    /// referenced by the lambda together with the compiled projector delegate.
    /// Returns (null, null) when no Select is present.
    /// </summary>
    internal static (IReadOnlyList<string>? Columns, Delegate? Projector) Parse(Expression expression)
    {
        if (expression is MethodCallExpression methodCall && IsSelectMethod(methodCall))
        {
            var lambda = ExtractLambda(methodCall.Arguments[1]);
            var columns = ExtractColumns(lambda.Body);
            return (columns, lambda.Compile());
        }

        return (null, null);
    }

    private static bool IsSelectMethod(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Queryable)
        && call.Method.Name == nameof(Queryable.Select);

    private static LambdaExpression ExtractLambda(Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    private static IReadOnlyList<string>? ExtractColumns(Expression body)
    {
        switch (body)
        {
            case NewExpression newExpr:
                // new { a.Name, a.Website } or new AnonymousType(a.Name, a.Website)
                return CollectFromArgs(newExpr.Arguments);

            case MemberInitExpression memberInit:
                // new Account { Name = a.Name, Website = a.Website }
                return CollectFromArgs(
                    memberInit.Bindings.OfType<MemberAssignment>().Select(b => b.Expression));

            case MemberExpression memberExpr:
                // single property: select a.Name
                var col = GetAttributeName(memberExpr);
                return col is not null ? [col] : null;

            default:
                return null;
        }
    }

    private static IReadOnlyList<string>? CollectFromArgs(IEnumerable<Expression> args)
    {
        var columns = args
            .Select(GetAttributeName)
            .Where(c => c is not null)
            .Select(c => c!)
            .ToList();

        return columns.Count > 0 ? columns : null;
    }

    private static string? GetAttributeName(Expression expr) =>
        expr is MemberExpression { Member: PropertyInfo prop }
            ? prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName
            : null;
}

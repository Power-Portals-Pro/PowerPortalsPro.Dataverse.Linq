using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

internal static class JoinExpressionParser
{
    internal static JoinInfo? TryParse(Expression expression)
    {
        if (expression is not MethodCallExpression call ||
            call.Method.DeclaringType != typeof(Queryable) ||
            call.Method.Name != nameof(Queryable.Join))
            return null;

        // Queryable.Join(outer, inner, outerKeySelector, innerKeySelector, resultSelector)
        var (outerEntityLogicalName, _) = GetSourceInfo(call.Arguments[0]);
        var (innerEntityLogicalName, innerEntityType) = GetSourceInfo(call.Arguments[1]);

        var outerKeyLambda = ExtractLambda(call.Arguments[2]);
        var innerKeyLambda = ExtractLambda(call.Arguments[3]);
        var resultLambda = ExtractLambda(call.Arguments[4]);

        var outerKeyAttribute = GetAttributeName(outerKeyLambda.Body)
            ?? throw new NotSupportedException("Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttribute = GetAttributeName(innerKeyLambda.Body)
            ?? throw new NotSupportedException("Inner join key must be a property decorated with [AttributeLogicalName].");

        var (outerColumns, innerColumns) = ExtractJoinColumns(resultLambda);

        return new JoinInfo(
            outerEntityLogicalName,
            outerKeyAttribute,
            innerEntityLogicalName,
            innerKeyAttribute,
            InnerAlias: "je0",
            IsOuterJoin: false,
            outerColumns,
            innerColumns,
            ResultSelector: resultLambda.Compile(),
            Projector: null,
            FilterWhereInnerIsNull: false,
            innerEntityType
        );
    }

    private static (string EntityLogicalName, Type EntityType) GetSourceInfo(Expression sourceExpr)
    {
        if (sourceExpr is ConstantExpression { Value: { } val })
        {
            var type = val.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(DataverseQueryable<>))
            {
                var entityType = type.GetGenericArguments()[0];
                var logicalName = entityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
                    ?? throw new InvalidOperationException(
                        $"Type '{entityType.Name}' must be decorated with [EntityLogicalName].");
                return (logicalName, entityType);
            }
        }

        throw new NotSupportedException("Join sources must be DataverseQueryable<T> instances.");
    }

    private static (IReadOnlyList<string>? OuterColumns, IReadOnlyList<string>? InnerColumns)
        ExtractJoinColumns(LambdaExpression lambda)
    {
        var outerParam = lambda.Parameters[0];
        var innerParam = lambda.Parameters[1];

        var outerColumns = new List<string>();
        var innerColumns = new List<string>();

        IEnumerable<Expression> args = lambda.Body switch
        {
            NewExpression ne => ne.Arguments,
            _ => []
        };

        foreach (var arg in args)
        {
            if (arg is not MemberExpression { Member: PropertyInfo prop, Expression: ParameterExpression paramExpr })
                continue;

            var attrName = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is null) continue;

            if (paramExpr == outerParam)
                outerColumns.Add(attrName);
            else if (paramExpr == innerParam)
                innerColumns.Add(attrName);
        }

        return (outerColumns.Count > 0 ? outerColumns : null,
                innerColumns.Count > 0 ? innerColumns : null);
    }

    private static LambdaExpression ExtractLambda(Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    private static string? GetAttributeName(Expression expr)
    {
        // Unwrap implicit conversions (e.g. Guid → Guid? when join key types differ)
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        return expr is MemberExpression { Member: PropertyInfo prop }
            ? prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName
            : null;
    }
}

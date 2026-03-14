using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

/// <summary>
/// Extension methods for evaluating, inspecting, and extracting information
/// from LINQ expression trees.
/// </summary>
internal static class ExpressionExtensions
{
    internal static bool IsNullConstant(this Expression expr) =>
        expr is ConstantExpression { Value: null }
        || (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            && convert.Operand is ConstantExpression { Value: null });

    internal static object? EvaluateValue(this Expression expr)
    {
        var result = EvaluateValueCore(expr);
        // Convert enum values to their underlying integer for FetchXml serialization.
        if (result is not null && result.GetType().IsEnum)
            return Convert.ChangeType(result, Enum.GetUnderlyingType(result.GetType()));
        // Convert enum arrays to their underlying integer arrays for FetchXml serialization.
        if (result is Array arr && arr.Length > 0 && arr.GetType().GetElementType() is { IsEnum: true } elemType)
        {
            var intArray = new object[arr.Length];
            var underlying = Enum.GetUnderlyingType(elemType);
            for (var i = 0; i < arr.Length; i++)
                intArray[i] = Convert.ChangeType(arr.GetValue(i)!, underlying);
            return intArray;
        }
        return result;
    }

    private static object? EvaluateValueCore(Expression expr)
    {
        if (expr is ConstantExpression constant)
            return constant.Value;

        // Closure field/property access: closureInstance.field (may be nested)
        if (expr is MemberExpression memberExpr)
        {
            var target = memberExpr.Expression is not null
                ? memberExpr.Expression.EvaluateValue()
                : null;
            return memberExpr.Member switch
            {
                FieldInfo fi => fi.GetValue(target),
                PropertyInfo pi => pi.GetValue(target),
                _ => throw new NotSupportedException($"Unsupported member type: {memberExpr.Member.GetType()}")
            };
        }

        // new[] { ... } inline array — use EvaluateValueCore to preserve element types
        // (enum conversion happens in EvaluateValue after the array is constructed)
        if (expr is NewArrayExpression newArray)
        {
            var items = newArray.Expressions.Select(EvaluateValueCore).ToArray();
            var array = Array.CreateInstance(newArray.Type.GetElementType()!, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }

        // Convert / cast
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return convert.Operand.EvaluateValue();

        // Method call (e.g. implicit conversions, static methods)
        if (expr is MethodCallExpression methodCall)
        {
            // Implicit/explicit conversion operators with a single argument — just evaluate the argument
            if (methodCall.Method is { IsSpecialName: true, Name: "op_Implicit" or "op_Explicit" }
                && methodCall.Arguments.Count == 1)
            {
                return methodCall.Arguments[0].EvaluateValue();
            }

            var target = methodCall.Object is not null ? methodCall.Object.EvaluateValue() : null;
            var args = methodCall.Arguments.Select(a => a.EvaluateValue()).ToArray();
            return methodCall.Method.Invoke(target, args);
        }

        // Constructor call (e.g. new DateTime(2020, 1, 1))
        if (expr is NewExpression newExpr && newExpr.Constructor is not null)
        {
            var args = newExpr.Arguments.Select(a => a.EvaluateValue()).ToArray();
            return newExpr.Constructor.Invoke(args);
        }

        throw new NotSupportedException(
            $"Unable to evaluate expression of type {expr.GetType().Name} (NodeType={expr.NodeType}): {expr}");
    }

    /// <summary>
    /// Unwraps Convert nodes and Nullable&lt;T&gt; to get the underlying CLR type of an expression.
    /// </summary>
    internal static Type UnwrapExpressionType(this Expression expr)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;
        var type = expr.Type;
        return Nullable.GetUnderlyingType(type) ?? type;
    }

    internal static LambdaExpression ExtractLambda(this Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    /// <summary>
    /// Flattens a chain of <see cref="ExpressionType.AndAlso"/> nodes into a flat list.
    /// </summary>
    internal static List<Expression> FlattenAndAlso(this Expression expr)
    {
        var result = new List<Expression>();
        if (expr is BinaryExpression { NodeType: ExpressionType.AndAlso } binary)
        {
            result.AddRange(binary.Left.FlattenAndAlso());
            result.AddRange(binary.Right.FlattenAndAlso());
        }
        else
        {
            result.Add(expr);
        }
        return result;
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="expr"/> contains a reference
    /// to the given <paramref name="param"/>.
    /// </summary>
    internal static bool ReferencesParameter(this Expression expr, ParameterExpression param)
    {
        return expr switch
        {
            _ when expr == param => true,
            UnaryExpression u => u.Operand.ReferencesParameter(param),
            MemberExpression m => m.Expression is not null && m.Expression.ReferencesParameter(param),
            MethodCallExpression mc =>
                (mc.Object is not null && mc.Object.ReferencesParameter(param))
                || mc.Arguments.Any(a => a.ReferencesParameter(param)),
            BinaryExpression b =>
                b.Left.ReferencesParameter(param) || b.Right.ReferencesParameter(param),
            _ => false
        };
    }

    internal static bool IsStringLength(this Expression expr) =>
        expr is MemberExpression { Member: { Name: "Length", DeclaringType: var dt } }
        && dt == typeof(string);
}

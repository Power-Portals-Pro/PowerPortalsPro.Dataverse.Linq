using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

internal static class LeftJoinExpressionParser
{
    /// <summary>
    /// Detects the left-join LINQ pattern:
    ///   Select( Where( SelectMany( GroupJoin(...) ) ) )
    /// and returns a <see cref="LeftJoinInfo"/> when matched, or null otherwise.
    /// </summary>
    internal static LeftJoinInfo? TryParse(Expression expression)
    {
        LambdaExpression? selectLambda = null;
        var filterNullInner = false;
        var current = expression;

        // 1. Unwrap optional Select
        if (IsQueryableMethod(current, nameof(Queryable.Select), out var selectCall))
        {
            selectLambda = ExtractLambda(selectCall!.Arguments[1]);
            current = selectCall.Arguments[0];
        }

        // 2. Unwrap optional Where
        if (IsQueryableMethod(current, nameof(Queryable.Where), out var whereCall))
        {
            filterNullInner = IsNullCheck(ExtractLambda(whereCall!.Arguments[1]));
            current = whereCall.Arguments[0];
        }

        // 3. Must be SelectMany (the DefaultIfEmpty flattening step)
        if (!IsQueryableMethod(current, nameof(Queryable.SelectMany), out var selectManyCall))
            return null;

        var selectManyResultSelector = ExtractLambda(selectManyCall!.Arguments[2]);
        current = selectManyCall.Arguments[0];

        // 4. Must be GroupJoin
        if (!IsQueryableMethod(current, nameof(Queryable.GroupJoin), out var groupJoinCall))
            return null;

        var (outerLogicalName, _) = GetSourceInfo(groupJoinCall!.Arguments[0]);
        var (innerLogicalName, _) = GetSourceInfo(groupJoinCall.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[2]).Body)
            ?? throw new NotSupportedException("Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[3]).Body)
            ?? throw new NotSupportedException("Inner join key must be a property decorated with [AttributeLogicalName].");

        // Determine which property of the transparent identifier holds the outer entity
        var outerPropertyName = GetOuterPropertyName(selectManyResultSelector);

        IReadOnlyList<string>? outerColumns = null;
        Delegate? projector = null;

        // When a Where clause is present the compiler emits a separate Select call.
        // When there is no Where, the compiler inlines the final projection directly
        // into the SelectMany result selector — so selectLambda is null in that case.
        var projectionSource = selectLambda ?? (outerPropertyName is not null ? selectManyResultSelector : null);

        if (projectionSource is not null && outerPropertyName is not null)
        {
            outerColumns = ExtractOuterColumns(projectionSource, outerPropertyName);
            projector = RebuildProjector(projectionSource, outerPropertyName);
        }

        return new LeftJoinInfo(
            outerLogicalName,
            outerKeyAttr,
            innerLogicalName,
            innerKeyAttr,
            InnerAlias: "je0",
            outerColumns,
            filterNullInner,
            projector
        );
    }

    // -------------------------------------------------------------------------
    // Structure detection
    // -------------------------------------------------------------------------

    private static bool IsQueryableMethod(Expression expr, string methodName, out MethodCallExpression? call)
    {
        if (expr is MethodCallExpression mc &&
            mc.Method.DeclaringType == typeof(Queryable) &&
            mc.Method.Name == methodName)
        {
            call = mc;
            return true;
        }

        call = null;
        return false;
    }

    private static bool IsNullCheck(LambdaExpression lambda) =>
        lambda.Body is BinaryExpression { NodeType: ExpressionType.Equal } binary &&
        (binary.Left is ConstantExpression { Value: null } ||
         binary.Right is ConstantExpression { Value: null });

    // -------------------------------------------------------------------------
    // Column / projector extraction
    // -------------------------------------------------------------------------

    /// <summary>
    /// Finds the property name of the outer entity inside the SelectMany result selector.
    /// Handles two shapes:
    /// <list type="bullet">
    ///   <item>Transparent identifier: <c>(x, c) => new { x.a, c }</c> → "a"</item>
    ///   <item>Inlined projection (no Where, compiler optimization): <c>(x, c) => new { x.a.Name }</c> → "a"</item>
    /// </list>
    /// </summary>
    private static string? GetOuterPropertyName(LambdaExpression selectManyResultSelector)
    {
        var xParam = selectManyResultSelector.Parameters[0];

        if (selectManyResultSelector.Body is not NewExpression newExpr)
            return null;

        foreach (var arg in newExpr.Arguments)
        {
            // Transparent identifier shape: x.a (direct property of x)
            if (arg is MemberExpression { Expression: ParameterExpression p } && p == xParam)
                return ((MemberExpression)arg).Member.Name;

            // Inlined projection shape: x.a.Property (two-level; outer entity is x.a)
            if (arg is MemberExpression { Expression: MemberExpression { Expression: ParameterExpression p2, Member: { } outerMember } }
                && p2 == xParam)
                return outerMember.Name;
        }

        return null;
    }

    private static IReadOnlyList<string>? ExtractOuterColumns(LambdaExpression selectLambda, string outerPropertyName)
    {
        var xParam = selectLambda.Parameters[0];
        var columns = new List<string>();

        IEnumerable<Expression> args = selectLambda.Body switch
        {
            NewExpression ne => ne.Arguments,
            _ => []
        };

        foreach (var arg in args)
        {
            // Match x.outerPropertyName.SomeAttribute pattern
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: MemberExpression inner }
                && inner.Expression == xParam
                && inner.Member.Name == outerPropertyName)
            {
                var attrName = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (attrName is not null)
                    columns.Add(attrName);
            }
        }

        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Rewrites <c>x => new { x.a.Name }</c> into <c>(T outer) => new { outer.Name }</c>
    /// so it can be invoked directly with the outer entity.
    /// </summary>
    private static Delegate RebuildProjector(LambdaExpression selectLambda, string outerPropertyName)
    {
        var originalParam = selectLambda.Parameters[0];
        var outerType = originalParam.Type.GetProperty(outerPropertyName)!.PropertyType;
        var outerParam = Expression.Parameter(outerType, "outer");

        var newBody = new OuterEntityRewriter(originalParam, outerParam, outerPropertyName)
            .Visit(selectLambda.Body);

        return Expression.Lambda(newBody, outerParam).Compile();
    }

    private sealed class OuterEntityRewriter(
        ParameterExpression originalParam,
        ParameterExpression outerParam,
        string outerPropertyName) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            // x.a.Property → outer.Property
            if (node.Expression is MemberExpression inner &&
                inner.Expression == originalParam &&
                inner.Member.Name == outerPropertyName)
            {
                return Expression.MakeMemberAccess(outerParam, node.Member);
            }

            return base.VisitMember(node);
        }
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

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

    private static LambdaExpression ExtractLambda(Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    private static string? GetAttributeName(Expression expr)
    {
        // Unwrap implicit conversions
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        if (expr is not MemberExpression memberExpr)
            return null;

        // Direct property: a.AccountId → [AttributeLogicalName("new_customaccountid")]
        if (memberExpr.Member is PropertyInfo directProp)
        {
            var attrName = directProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is not null) return attrName;
        }

        // Two-level EntityReference access: c.ParentAccount.Id
        // → [AttributeLogicalName] comes from the EntityReference property (ParentAccount)
        if (memberExpr.Member.Name == nameof(Entity.Id) &&
            memberExpr.Expression is MemberExpression { Member: PropertyInfo refProp })
        {
            return refProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
        }

        return null;
    }
}

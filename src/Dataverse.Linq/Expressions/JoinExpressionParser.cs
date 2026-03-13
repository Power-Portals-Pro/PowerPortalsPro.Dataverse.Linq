using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

internal static class JoinExpressionParser
{
    /// <summary>
    /// Attempts to parse <paramref name="expression"/> as either a left-outer join
    /// (<c>GroupJoin → SelectMany → Where → Select</c>) or an inner join
    /// (<c>Queryable.Join</c>), returning a <see cref="JoinInfo"/> when matched.
    /// Left join is tried first because its pattern is a superset of what a plain
    /// <c>Select</c> might look like.
    /// </summary>
    internal static JoinInfo? TryParse(Expression expression) =>
        TryParseOuter(expression) ?? TryParseInner(expression);

    // -------------------------------------------------------------------------
    // Inner join  —  Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    // -------------------------------------------------------------------------

    private static JoinInfo? TryParseInner(Expression expression)
    {
        if (expression is not MethodCallExpression call ||
            call.Method.DeclaringType != typeof(Queryable) ||
            call.Method.Name != nameof(Queryable.Join))
            return null;

        var (outerLogicalName, _) = GetSourceInfo(call.Arguments[0]);
        var (innerLogicalName, innerEntityType) = GetSourceInfo(call.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[2]).Body)
            ?? throw new NotSupportedException("Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[3]).Body)
            ?? throw new NotSupportedException("Inner join key must be a property decorated with [AttributeLogicalName].");

        var resultLambda = ExtractLambda(call.Arguments[4]);
        var (outerColumns, innerColumns) = ExtractInnerJoinColumns(resultLambda);

        return new JoinInfo(
            outerLogicalName,
            outerKeyAttr,
            innerLogicalName,
            innerKeyAttr,
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

    private static (IReadOnlyList<string>? OuterColumns, IReadOnlyList<string>? InnerColumns)
        ExtractInnerJoinColumns(LambdaExpression lambda)
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

    // -------------------------------------------------------------------------
    // Left-outer join  —  Select( Where( SelectMany( GroupJoin(...) ) ) )
    // -------------------------------------------------------------------------

    private static JoinInfo? TryParseOuter(Expression expression)
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

        var (outerLogicalName, outerEntityType) = GetSourceInfo(groupJoinCall!.Arguments[0]);
        var (innerLogicalName, _) = GetSourceInfo(groupJoinCall.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[2]).Body)
            ?? throw new NotSupportedException("Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[3]).Body)
            ?? throw new NotSupportedException("Inner join key must be a property decorated with [AttributeLogicalName].");

        // Find the path to the outer entity through any number of transparent-identifier
        // nesting levels. E.g. for a with-where query the compiler produces:
        //   TI2 { <>h__TransparentIdentifier0: TI1 { a: CustomAccount, contacts }, c }
        // so the path is ["<>h__TransparentIdentifier0", "a"].
        var lambdaToInspect = selectLambda ?? selectManyResultSelector;
        var outerPath = FindOuterPropertyPath(lambdaToInspect.Parameters[0].Type, outerEntityType);

        IReadOnlyList<string>? outerColumns = null;
        Delegate? projector = null;

        var projectionSource = selectLambda ?? (outerPath is not null ? selectManyResultSelector : null);

        if (projectionSource is not null && outerPath is not null)
        {
            outerColumns = ExtractOuterColumns(projectionSource, outerPath);
            projector = RebuildProjector(projectionSource, outerPath, outerEntityType);
        }

        return new JoinInfo(
            outerLogicalName,
            outerKeyAttr,
            innerLogicalName,
            innerKeyAttr,
            InnerAlias: "je0",
            IsOuterJoin: true,
            outerColumns,
            InnerColumns: null,
            ResultSelector: null,
            projector,
            FilterWhereInnerIsNull: filterNullInner,
            InnerEntityType: null
        );
    }

    private static bool IsNullCheck(LambdaExpression lambda) =>
        lambda.Body is BinaryExpression { NodeType: ExpressionType.Equal } binary &&
        (binary.Left is ConstantExpression { Value: null } ||
         binary.Right is ConstantExpression { Value: null });

    /// <summary>
    /// Recursively searches <paramref name="type"/> for a property whose type is (or is
    /// assignable from) <paramref name="outerEntityType"/>, descending into compiler-generated
    /// transparent-identifier types (<c>&lt;&gt;h__TransparentIdentifier</c>).
    /// Returns the property-name chain or <c>null</c> if not found.
    /// </summary>
    private static string[]? FindOuterPropertyPath(Type type, Type outerEntityType, int maxDepth = 5)
    {
        foreach (var prop in type.GetProperties())
        {
            if (outerEntityType.IsAssignableFrom(prop.PropertyType))
                return [prop.Name];

            if (maxDepth > 1 && prop.PropertyType.Name.StartsWith("<>"))
            {
                var nested = FindOuterPropertyPath(prop.PropertyType, outerEntityType, maxDepth - 1);
                if (nested is not null)
                    return [prop.Name, .. nested];
            }
        }

        return null;
    }

    private static IReadOnlyList<string>? ExtractOuterColumns(LambdaExpression selectLambda, string[] outerPath)
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
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: { } attrExpr }
                && IsOuterEntityAccess(attrExpr, xParam, outerPath))
            {
                var attrName = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (attrName is not null)
                    columns.Add(attrName);
            }
        }

        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Rewrites <c>x => new { x.ti0.a.Name }</c> into <c>(T outer) => new { outer.Name }</c>
    /// so it can be invoked directly with the outer entity.
    /// </summary>
    private static Delegate RebuildProjector(LambdaExpression selectLambda, string[] outerPath, Type outerEntityType)
    {
        var originalParam = selectLambda.Parameters[0];
        var outerParam = Expression.Parameter(outerEntityType, "outer");

        var newBody = new OuterEntityRewriter(originalParam, outerParam, outerPath)
            .Visit(selectLambda.Body);

        return Expression.Lambda(newBody, outerParam).Compile();
    }

    private sealed class OuterEntityRewriter(
        ParameterExpression originalParam,
        ParameterExpression outerParam,
        string[] outerPath) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null && IsOuterEntityAccess(node.Expression, originalParam, outerPath))
                return Expression.MakeMemberAccess(outerParam, node.Member);

            return base.VisitMember(node);
        }
    }

    /// <summary>Returns true when <paramref name="expr"/> matches <c>param.path[0]...path[n-1]</c>.</summary>
    private static bool IsOuterEntityAccess(Expression expr, ParameterExpression param, string[] path)
    {
        for (var i = path.Length - 1; i >= 0; i--)
        {
            if (expr is not MemberExpression me || me.Member.Name != path[i])
                return false;
            expr = me.Expression!;
        }

        return expr is ParameterExpression p && p == param;
    }

    // -------------------------------------------------------------------------
    // Shared helpers
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
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        if (expr is not MemberExpression memberExpr)
            return null;

        // Direct property: a.AccountId → [AttributeLogicalName]
        if (memberExpr.Member is PropertyInfo directProp)
        {
            var attrName = directProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is not null) return attrName;
        }

        // Two-level EntityReference access: c.ParentAccount.Id → [AttributeLogicalName] on ParentAccount
        if (memberExpr.Member.Name == nameof(Entity.Id) &&
            memberExpr.Expression is MemberExpression { Member: PropertyInfo refProp })
        {
            return refProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
        }

        return null;
    }
}

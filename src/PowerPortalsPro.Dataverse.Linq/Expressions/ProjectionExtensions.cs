using PowerPortalsPro.Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Expressions;

/// <summary>
/// Extension methods for extracting column lists, row aggregates, and projection
/// arguments from LINQ select/projection expression trees.
/// </summary>
internal static class ProjectionExtensions
{
    /// <summary>
    /// Extracts the member-argument expressions from a projection body
    /// (<see cref="NewExpression"/>, <see cref="MemberInitExpression"/>, or single
    /// <see cref="MemberExpression"/>).
    /// </summary>
    internal static IEnumerable<Expression> GetProjectionArguments(this Expression body) => body switch
    {
        NewExpression ne => ne.Arguments,
        MemberInitExpression init => init.Bindings
            .OfType<MemberAssignment>().Select(b => b.Expression),
        MemberExpression me => [me],
        _ => []
    };

    /// <summary>
    /// Extracts attribute logical names from a simple select body (anonymous type,
    /// member-init, or single property access).
    /// </summary>
    internal static IReadOnlyList<string>? ExtractColumns(this Expression body)
    {
        var columns = new List<string>();

        foreach (var arg in body.GetProjectionArguments())
        {
            var name = arg.GetAttributeName();
            if (name is not null)
                columns.Add(name);
        }

        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Extracts attribute logical names from a lambda whose parameter is a transparent-
    /// identifier type. Only members accessed through <paramref name="outerPath"/> are
    /// collected; inner-entity references are ignored.
    /// </summary>
    internal static IReadOnlyList<string>? ExtractColumnsViaPath(
        this LambdaExpression lambda, string[] outerPath)
    {
        var param = lambda.Parameters[0];
        var columns = new List<string>();
        CollectColumns(lambda.Body, arg => arg switch
        {
            MemberExpression { Member: PropertyInfo prop, Expression: { } attrExpr }
                when attrExpr.IsOuterEntityAccess(outerPath, param)
                => prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName,
            MethodCallExpression
            {
                Method.Name: nameof(Microsoft.Xrm.Sdk.Entity.GetAttributeValue),
                Arguments: [ConstantExpression { Value: string attrName }],
                Object: { } obj
            }
                when obj.IsOuterEntityAccess(outerPath, param)
                => attrName,
            _ => null
        }, columns);
        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Returns <c>true</c> when the projection references the whole inner entity
    /// (e.g. <c>select new { ..., Contact = c }</c>) rather than individual properties on it.
    /// </summary>
    internal static bool ReferencesWholeInnerEntity(
        this LambdaExpression lambda, string innerPropertyName)
    {
        var param = lambda.Parameters[0];
        return MatchesProjectionArgument(lambda.Body, arg =>
            arg is MemberExpression { Member.Name: var name, Expression: ParameterExpression p }
            && p == param && name == innerPropertyName);
    }

    /// <summary>
    /// Extracts attribute logical names from inner entity property accesses in a left-join
    /// projection. Only members accessed through <paramref name="innerPropertyName"/> are
    /// collected.
    /// </summary>
    internal static IReadOnlyList<string>? ExtractInnerColumnsViaProperty(
        this LambdaExpression lambda, string innerPropertyName)
    {
        var param = lambda.Parameters[0];
        var columns = new List<string>();
        CollectColumns(lambda.Body, arg => arg switch
        {
            MemberExpression { Member: PropertyInfo prop, Expression: MemberExpression inner }
                when inner.Member.Name == innerPropertyName
                && inner.Expression is ParameterExpression p && p == param
                => prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName,
            MethodCallExpression
            {
                Method.Name: nameof(Microsoft.Xrm.Sdk.Entity.GetAttributeValue),
                Arguments: [ConstantExpression { Value: string attrName }],
                Object: MemberExpression inner
            }
                when inner.Member.Name == innerPropertyName
                && inner.Expression is ParameterExpression p && p == param
                => attrName,
            _ => null
        }, columns);
        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Recursively walks projection arguments and collects column names using the provided
    /// matcher. Descends into nested <see cref="NewExpression"/>, <see cref="MemberInitExpression"/>,
    /// <see cref="ConditionalExpression"/>, conversion <see cref="UnaryExpression"/>, and
    /// <see cref="MethodCallExpression"/> arguments.
    /// </summary>
    private static void CollectColumns(Expression body, Func<Expression, string?> matcher, List<string> columns)
    {
        foreach (var arg in body.GetProjectionArguments())
            CollectColumnsFromExpr(arg, matcher, columns);
    }

    private static void CollectColumnsFromExpr(Expression arg, Func<Expression, string?> matcher, List<string> columns)
    {
        var name = matcher(arg);
        if (name is not null)
        {
            columns.Add(name);
            return;
        }

        switch (arg)
        {
            case NewExpression or MemberInitExpression:
                CollectColumns(arg, matcher, columns);
                break;
            case ConditionalExpression conditional:
                CollectColumnsFromExpr(conditional.IfTrue, matcher, columns);
                CollectColumnsFromExpr(conditional.IfFalse, matcher, columns);
                break;
            case UnaryExpression { NodeType: ExpressionType.Convert } unary:
                CollectColumnsFromExpr(unary.Operand, matcher, columns);
                break;
            case MethodCallExpression mc:
                if (mc.Object is not null)
                    CollectColumnsFromExpr(mc.Object, matcher, columns);
                foreach (var ca in mc.Arguments)
                    CollectColumnsFromExpr(ca, matcher, columns);
                break;
            case ConstantExpression or DefaultExpression or MemberExpression or ParameterExpression:
                break;
            default:
                throw new NotSupportedException(
                    $"Unsupported expression type '{arg.NodeType}' in projection. " +
                    $"Column references inside '{arg.NodeType}' expressions cannot be extracted.");
        }
    }

    private static bool MatchesProjectionArgument(Expression body, Func<Expression, bool> predicate)
    {
        foreach (var arg in body.GetProjectionArguments())
        {
            if (MatchesExpr(arg, predicate))
                return true;
        }
        return false;
    }

    private static bool MatchesExpr(Expression arg, Func<Expression, bool> predicate)
    {
        if (predicate(arg))
            return true;

        switch (arg)
        {
            case NewExpression or MemberInitExpression:
                return MatchesProjectionArgument(arg, predicate);
            case ConditionalExpression conditional:
                return MatchesExpr(conditional.IfTrue, predicate)
                    || MatchesExpr(conditional.IfFalse, predicate);
            case UnaryExpression { NodeType: ExpressionType.Convert } unary:
                return MatchesExpr(unary.Operand, predicate);
            case MethodCallExpression mc:
                if (mc.Object is not null && MatchesExpr(mc.Object, predicate))
                    return true;
                foreach (var ca in mc.Arguments)
                {
                    if (MatchesExpr(ca, predicate))
                        return true;
                }
                return false;
            case ConstantExpression or DefaultExpression or MemberExpression or ParameterExpression:
                return false;
            default:
                throw new NotSupportedException(
                    $"Unsupported expression type '{arg.NodeType}' in projection. " +
                    $"Column references inside '{arg.NodeType}' expressions cannot be extracted.");
        }
    }

    internal static IReadOnlyList<RowAggregateInfo>? ExtractRowAggregates(this Expression body)
    {
        List<RowAggregateInfo>? results = null;

        if (body is NewExpression ne && ne.Members is not null)
        {
            for (var i = 0; i < ne.Arguments.Count; i++)
            {
                if (ne.Arguments[i] is MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.CountChildren) } } mc
                    && mc.Method.DeclaringType == typeof(ServiceClientExtensions))
                {
                    var member = ne.Members[i];
                    var memberName = member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                        ? getter.Name[4..] : member.Name;
                    results ??= [];
                    results.Add(new RowAggregateInfo(memberName.ToLowerInvariant()));
                }
            }
        }
        else if (body is MemberInitExpression init)
        {
            foreach (var binding in init.Bindings.OfType<MemberAssignment>())
            {
                if (binding.Expression is MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.CountChildren) } } mc
                    && mc.Method.DeclaringType == typeof(ServiceClientExtensions))
                {
                    results ??= [];
                    results.Add(new RowAggregateInfo(binding.Member.Name.ToLowerInvariant()));
                }
            }
        }

        return results;
    }

    internal static void ApplyColumns(this FetchXmlQuery query, IReadOnlyList<string> columns)
    {
        query.AllAttributes = false;
        foreach (var col in columns)
            query.Attributes.Add(new FetchAttribute { Name = col });
    }
}

internal record RowAggregateInfo(string Alias);

/// <summary>
/// Rewrites <see cref="ServiceClientExtensions.CountChildren"/> calls in a projection
/// to <see cref="AggregateProjection.ExtractValue{T}"/> calls that read the aliased value
/// from the entity at runtime.
/// </summary>
internal sealed class CountChildrenRewriter(ParameterExpression entityParam) : ExpressionVisitor
{
    private static readonly MethodInfo ExtractMethod =
        typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!;

    protected override Expression VisitNew(NewExpression node)
    {
        if (node.Members is null)
            return base.VisitNew(node);

        var changed = false;
        var newArgs = new Expression[node.Arguments.Count];

        for (var i = 0; i < node.Arguments.Count; i++)
        {
            if (node.Arguments[i] is MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.CountChildren) } } mc
                && mc.Method.DeclaringType == typeof(ServiceClientExtensions))
            {
                var member = node.Members[i];
                var memberName = member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                    ? getter.Name[4..] : member.Name;
                var alias = memberName.ToLowerInvariant();

                newArgs[i] = Expression.Call(
                    ExtractMethod.MakeGenericMethod(typeof(int)),
                    entityParam,
                    Expression.Constant(alias));
                changed = true;
            }
            else
            {
                newArgs[i] = Visit(node.Arguments[i]);
            }
        }

        return changed
            ? Expression.New(node.Constructor!, newArgs, node.Members)
            : base.VisitNew(node);
    }

    protected override Expression VisitMemberInit(MemberInitExpression node)
    {
        var changed = false;
        var newBindings = new List<MemberBinding>(node.Bindings.Count);

        foreach (var binding in node.Bindings)
        {
            if (binding is MemberAssignment assignment
                && assignment.Expression is MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.CountChildren) } } mc
                && mc.Method.DeclaringType == typeof(ServiceClientExtensions))
            {
                var alias = assignment.Member.Name.ToLowerInvariant();
                var extractCall = Expression.Call(
                    ExtractMethod.MakeGenericMethod(typeof(int)),
                    entityParam,
                    Expression.Constant(alias));
                newBindings.Add(Expression.Bind(assignment.Member, extractCall));
                changed = true;
            }
            else
            {
                newBindings.Add(VisitMemberBinding(binding));
            }
        }

        return changed
            ? Expression.MemberInit(Visit(node.NewExpression) as NewExpression ?? node.NewExpression, newBindings)
            : base.VisitMemberInit(node);
    }
}

using PowerPortalsPro.Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using System.Diagnostics.CodeAnalysis;
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
    /// Extracts the attributes a select on the root entity reads, wherever they sit in its body:
    /// the whole body (<c>a =&gt; a.GetAttributeValue&lt;string&gt;("name")</c>, <c>a =&gt; a["name"]</c>,
    /// <c>a =&gt; a.Id</c>), an anonymous type or member-init, or nested in a method call,
    /// operator or conditional (<c>a.Website.ToUpper()</c>, <c>a.NumberOfEmployees + 1</c>).
    /// </summary>
    /// <param name="lambda">The select's lambda; its parameter is the root entity.</param>
    /// <param name="primaryKeyOf">Resolves an entity's primary key, for <see cref="Entity.Id"/>.</param>
    /// <param name="rootEntityLogicalName">
    /// The root entity's logical name, for <see cref="Entity.Id"/> on an unbound entity.
    /// </param>
    /// <returns>
    /// The attributes, or <c>null</c> when the select needs every attribute: it uses the entity
    /// other than through an attribute (selects it whole, passes it to a method, reads
    /// <see cref="Entity.Attributes"/> or <see cref="Entity.FormattedValues"/>, or names an
    /// attribute only the row can supply), or reads no attribute at all.
    /// </returns>
    internal static IReadOnlyList<string>? ExtractRootColumns(
        this LambdaExpression lambda, Func<string, string> primaryKeyOf, string rootEntityLogicalName)
    {
        var collector = new RootColumnCollector(lambda.Parameters[0], primaryKeyOf, rootEntityLogicalName);
        collector.Visit(lambda.Body);

        return collector.NeedsWholeEntity || collector.Columns.Count == 0 ? null : collector.Columns;
    }

    /// <summary>
    /// Returns <c>true</c> when a select on the root entity is one attribute read and nothing
    /// else (<c>a =&gt; a.Name</c>, <c>a =&gt; (decimal?)a.CreditLimit.Value</c>,
    /// <c>a =&gt; a["name"]</c>), so its value is the column's value.
    /// </summary>
    internal static bool IsSingleRootAttributeRead(
        this LambdaExpression lambda, Func<string, string> primaryKeyOf, string rootEntityLogicalName) =>
        RootColumnCollector.TryResolve(lambda.Body, lambda.Parameters[0], primaryKeyOf, rootEntityLogicalName, out _);

    /// <summary>
    /// Walks a root-entity select and collects each attribute it reads on the entity parameter.
    /// Any other use of the parameter means the select needs the whole entity.
    /// </summary>
    private sealed class RootColumnCollector(
        ParameterExpression entity, Func<string, string> primaryKeyOf, string rootEntityLogicalName)
        : ExpressionVisitor
    {
        public List<string> Columns { get; } = [];

        public bool NeedsWholeEntity { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (node is null || NeedsWholeEntity)
                return node;

            if (TryResolve(node, entity, primaryKeyOf, rootEntityLogicalName, out var name))
            {
                if (!Columns.Contains(name))
                    Columns.Add(name);
                return node;
            }

            return base.Visit(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            // Reached only when no attribute read consumed the parameter first.
            if (node == entity)
                NeedsWholeEntity = true;
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Every row returned carries its logical name, whatever the columns.
            if (node.Member.Name == nameof(Entity.LogicalName)
                && node.Member.DeclaringType == typeof(Entity)
                && Unwrap(node.Expression) == entity)
            {
                return node;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // A row aggregate the select adds as its own attribute; it reads no column.
            if (node.Method.Name == nameof(ServiceClientExtensions.CountChildren)
                && node.Method.DeclaringType == typeof(ServiceClientExtensions))
            {
                return node;
            }

            return base.VisitMethodCall(node);
        }

        internal static bool TryResolve(
            Expression expr, ParameterExpression entity, Func<string, string> primaryKeyOf,
            string rootEntityLogicalName, [NotNullWhen(true)] out string? name)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expr = convert.Operand;

            if (expr.IsEntityIndexerCall(out name, out var indexed) && Unwrap(indexed) == entity)
                return true;

            var access = expr.ResolveAttributeAccess(primaryKeyOf, rootEntityLogicalName);
            if (access is not null && Unwrap(access.Value.EntityExpression) == entity)
            {
                name = access.Value.AttributeName;
                return true;
            }

            name = null;
            return false;
        }

        private static Expression? Unwrap(Expression? expr)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.TypeAs } convert)
                expr = convert.Operand;
            return expr;
        }
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
        CollectColumns(lambda.Body, arg =>
        {
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: { } attrExpr }
                && attrExpr.IsOuterEntityAccess(outerPath, param))
            {
                return prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            }

            if (arg.IsGetAttributeValueCall(out var attrName, out var entityExpr)
                && entityExpr.IsOuterEntityAccess(outerPath, param))
            {
                return attrName;
            }

            return null;
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
    /// <param name="resolveIdColumn">
    /// Resolves the inner entity's primary key column. Used for <see cref="Entity.Id"/> on an
    /// unbound inner entity, which has no [AttributeLogicalName]; invoked only when such an
    /// access is found, since resolving may require a metadata request.
    /// </param>
    internal static IReadOnlyList<string>? ExtractInnerColumnsViaProperty(
        this LambdaExpression lambda, string innerPropertyName, Func<string>? resolveIdColumn = null)
    {
        var param = lambda.Parameters[0];
        var columns = new List<string>();
        CollectColumns(lambda.Body, arg =>
        {
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: MemberExpression inner }
                && IsInnerAccess(inner))
            {
                var name = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (name is null && prop.Name == nameof(Entity.Id) && inner.Type == typeof(Entity))
                    name = resolveIdColumn?.Invoke();
                return name;
            }

            if (arg.IsGetAttributeValueCall(out var attrName, out var entityExpr)
                && entityExpr is MemberExpression innerEntity && IsInnerAccess(innerEntity))
            {
                return attrName;
            }

            return null;
        }, columns);

        return columns.Count > 0 ? columns : null;

        bool IsInnerAccess(MemberExpression inner) =>
            inner.Member.Name == innerPropertyName
            && inner.Expression is ParameterExpression p && p == param;
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

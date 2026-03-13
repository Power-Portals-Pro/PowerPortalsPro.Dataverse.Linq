using Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace Dataverse.Linq.Expressions;

/// <summary>
/// Translates a LINQ expression tree into a <see cref="FetchXmlQuery"/> model.
/// Replaces the individual ad-hoc parsers with a single, composable translator
/// that processes the expression chain recursively (source-first) and populates
/// the query model operator by operator.
/// </summary>
internal static class FetchXmlQueryTranslator
{
    /// <summary>
    /// Entry point. Translates the given expression into a <see cref="FetchXmlQuery"/>.
    /// </summary>
    /// <param name="expression">The LINQ expression tree.</param>
    /// <param name="defaultColumns">
    /// Columns specified at queryable creation time (e.g. <c>Queryable&lt;T&gt;("col1","col2")</c>).
    /// Applied only when no <c>Select</c> operator narrows the column list.
    /// </param>
    internal static FetchXmlQuery Translate<T>(
        Expression expression,
        IReadOnlyList<string>? defaultColumns = null,
        string? entityLogicalName = null) where T : Entity
    {
        var query = new FetchXmlQuery
        {
            EntityLogicalName = entityLogicalName ?? GetEntityLogicalName(typeof(T))
        };

        var ctx = new TranslationContext(query, typeof(T));
        TranslateCore(expression, ctx);

        // If no operator narrowed the columns, apply provider-level defaults
        if (query.AllAttributes && defaultColumns is { Count: > 0 })
        {
            query.AllAttributes = false;
            foreach (var col in defaultColumns)
                query.Attributes.Add(new FetchAttribute { Name = col });
        }

        return query;
    }

    // -------------------------------------------------------------------------
    // Recursive expression chain dispatcher
    // -------------------------------------------------------------------------

    private static void TranslateCore(Expression expression, TranslationContext ctx)
    {
        switch (expression)
        {
            case ConstantExpression:
                // Root source — entity name already set in Translate()
                return;

            case MethodCallExpression call when call.Method.DeclaringType == typeof(Queryable):
                switch (call.Method.Name)
                {
                    case nameof(Queryable.Select):
                        TranslateCore(call.Arguments[0], ctx);
                        HandleSelect(call, ctx);
                        return;

                    case nameof(Queryable.Where):
                        TranslateCore(call.Arguments[0], ctx);
                        HandleWhere(call, ctx);
                        return;

                    case nameof(Queryable.Join):
                        HandleInnerJoin(call, ctx);
                        return;

                    case nameof(Queryable.SelectMany):
                        HandleSelectMany(call, ctx);
                        return;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                    case nameof(Queryable.ThenBy):
                    case nameof(Queryable.ThenByDescending):
                        TranslateCore(call.Arguments[0], ctx);
                        HandleOrderBy(call, ctx);
                        return;

                    default:
                        throw new NotSupportedException(
                            $"LINQ operator '{call.Method.Name}' is not supported.");
                }

            default:
                throw new NotSupportedException(
                    $"Unsupported expression type: {expression.NodeType}");
        }
    }

    // -------------------------------------------------------------------------
    // Select — extract attribute columns + compile projector
    // -------------------------------------------------------------------------

    private static void HandleSelect(MethodCallExpression call, TranslationContext ctx)
    {
        var lambda = ExtractLambda(call.Arguments[1]);

        if (ctx.OuterEntityPath is not null)
        {
            // After a left join — resolve member accesses through transparent identifiers
            var columns = ExtractColumnsViaPath(lambda, ctx.OuterEntityPath);
            if (columns is { Count: > 0 })
            {
                ctx.Query.AllAttributes = false;
                foreach (var col in columns)
                    ctx.Query.Attributes.Add(new FetchAttribute { Name = col });
            }

            ctx.Query.Projector = RebuildProjector(lambda, ctx.OuterEntityPath, ctx.OuterEntityType!);
            ctx.Query.ProjectionType = lambda.ReturnType;
        }
        else
        {
            // Simple select on root entity
            var columns = ExtractColumns(lambda.Body);
            if (columns is { Count: > 0 })
            {
                ctx.Query.AllAttributes = false;
                foreach (var col in columns)
                    ctx.Query.Attributes.Add(new FetchAttribute { Name = col });
            }

            ctx.Query.Projector = lambda.Compile();
            ctx.Query.ProjectionType = lambda.ReturnType;
        }
    }

    // -------------------------------------------------------------------------
    // Where — predicate translation
    // -------------------------------------------------------------------------

    private static void HandleWhere(MethodCallExpression call, TranslationContext ctx)
    {
        var lambda = ExtractLambda(call.Arguments[1]);

        // Left-join null filter: where c == null
        if (ctx.InnerEntityProperty is not null && IsNullCheck(lambda, ctx.InnerEntityProperty))
        {
            var link = ctx.Query.Links[^1];
            var filter = ctx.Query.Filter ??= new FetchFilter();
            filter.Conditions.Add(new FetchCondition
            {
                EntityAlias = link.Alias,
                Attribute = link.Name + "id",
                Operator = "null"
            });
            return;
        }

        // General predicate
        var rootFilter = ctx.Query.Filter ??= new FetchFilter();
        TranslatePredicate(lambda.Body, rootFilter);
    }

    private static void TranslatePredicate(Expression expr, FetchFilter filter)
    {
        switch (expr)
        {
            // !expr → negate
            case UnaryExpression { NodeType: ExpressionType.Not, Operand: var operand }:
                TranslateNegatedPredicate(operand, filter);
                return;

            // string.IsNullOrEmpty(x.Attr) → null OR eq ""
            case MethodCallExpression { Method: { Name: "IsNullOrEmpty", DeclaringType: var dt } } isNullCall
                when dt == typeof(string):
            {
                var attr = GetAttributeName(isNullCall.Arguments[0])
                    ?? throw new NotSupportedException(
                        "string.IsNullOrEmpty argument must resolve to an attribute.");
                filter.Type = FilterType.Or;
                filter.Conditions.Add(new FetchCondition { Attribute = attr, Operator = "null" });
                filter.Conditions.Add(new FetchCondition { Attribute = attr, Operator = "eq", Value = "" });
                return;
            }

            // x.Attr == value / x.Attr != value / x.Attr == null / etc.
            case BinaryExpression binary:
                TranslateBinaryPredicate(binary, filter);
                return;

            default:
                throw new NotSupportedException(
                    $"Unsupported Where predicate: {expr.NodeType}");
        }
    }

    private static void TranslateNegatedPredicate(Expression expr, FetchFilter filter)
    {
        switch (expr)
        {
            // !string.IsNullOrEmpty(x.Attr) → not-null AND ne ""
            case MethodCallExpression { Method: { Name: "IsNullOrEmpty", DeclaringType: var dt } } isNullCall
                when dt == typeof(string):
            {
                var attr = GetAttributeName(isNullCall.Arguments[0])
                    ?? throw new NotSupportedException(
                        "string.IsNullOrEmpty argument must resolve to an attribute.");
                filter.Type = FilterType.And;
                filter.Conditions.Add(new FetchCondition { Attribute = attr, Operator = "not-null" });
                filter.Conditions.Add(new FetchCondition { Attribute = attr, Operator = "ne", Value = "" });
                return;
            }

            default:
                throw new NotSupportedException(
                    $"Unsupported negated Where predicate: {expr.NodeType}");
        }
    }

    private static void TranslateBinaryPredicate(BinaryExpression binary, FetchFilter filter)
    {
        var (op, negate) = binary.NodeType switch
        {
            ExpressionType.Equal => ("eq", false),
            ExpressionType.NotEqual => ("ne", false),
            ExpressionType.LessThan => ("lt", false),
            ExpressionType.LessThanOrEqual => ("le", false),
            ExpressionType.GreaterThan => ("gt", false),
            ExpressionType.GreaterThanOrEqual => ("ge", false),
            ExpressionType.AndAlso => ("and", false),
            ExpressionType.OrElse => ("or", false),
            _ => throw new NotSupportedException(
                $"Unsupported binary operator in Where: {binary.NodeType}")
        };

        // && and || → nested filter
        if (op is "and" or "or")
        {
            var subFilter = new FetchFilter
            {
                Type = op == "and" ? FilterType.And : FilterType.Or
            };
            TranslatePredicate(binary.Left, subFilter);
            TranslatePredicate(binary.Right, subFilter);
            filter.Filters.Add(subFilter);
            return;
        }

        // Determine which side is the attribute and which is the value
        var (attrExpr, valueExpr) = ResolveAttributeAndValue(binary.Left, binary.Right);

        var attribute = GetAttributeName(attrExpr)
            ?? throw new NotSupportedException(
                "One side of a comparison must resolve to an attribute.");

        // null comparison → null / not-null
        if (IsNullConstant(valueExpr))
        {
            filter.Conditions.Add(new FetchCondition
            {
                Attribute = attribute,
                Operator = op == "eq" ? "null" : "not-null"
            });
            return;
        }

        var value = EvaluateValue(valueExpr);
        filter.Conditions.Add(new FetchCondition
        {
            Attribute = attribute,
            Operator = op,
            Value = value
        });
    }

    private static (Expression Attr, Expression Value) ResolveAttributeAndValue(
        Expression left, Expression right)
    {
        // Try left as attribute first
        if (GetAttributeName(left) is not null)
            return (left, right);
        if (GetAttributeName(right) is not null)
            return (right, left);

        return (left, right);
    }

    private static bool IsNullConstant(Expression expr) =>
        expr is ConstantExpression { Value: null }
        || (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            && convert.Operand is ConstantExpression { Value: null });

    private static object? EvaluateValue(Expression expr)
    {
        if (expr is ConstantExpression constant)
            return constant.Value;

        // Evaluate captured variables / closures
        return Expression.Lambda(expr).Compile().DynamicInvoke();
    }

    // -------------------------------------------------------------------------
    // OrderBy / ThenBy
    // -------------------------------------------------------------------------

    private static void HandleOrderBy(MethodCallExpression call, TranslationContext ctx)
    {
        var lambda = ExtractLambda(call.Arguments[1]);
        var descending = call.Method.Name is nameof(Queryable.OrderByDescending)
                                           or nameof(Queryable.ThenByDescending);

        var attribute = GetAttributeName(lambda.Body)
            ?? throw new NotSupportedException(
                "OrderBy key must be a property decorated with [AttributeLogicalName].");

        ctx.Query.Orders.Add(new FetchOrder { Attribute = attribute, Descending = descending });
    }

    // -------------------------------------------------------------------------
    // Inner join — Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    // -------------------------------------------------------------------------

    private static void HandleInnerJoin(MethodCallExpression call, TranslationContext ctx)
    {
        var (outerLogicalName, _) = GetSourceInfo(call.Arguments[0]);
        var (innerLogicalName, innerEntityType) = GetSourceInfo(call.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[2]).Body)
            ?? throw new NotSupportedException(
                "Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[3]).Body)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        var resultLambda = ExtractLambda(call.Arguments[4]);
        var (outerColumns, innerColumns) = ExtractJoinColumns(
            resultLambda, resultLambda.Parameters[0], resultLambda.Parameters[1]);

        // Root entity
        ctx.Query.EntityLogicalName = outerLogicalName;

        if (outerColumns is { Count: > 0 })
        {
            ctx.Query.AllAttributes = false;
            foreach (var col in outerColumns)
                ctx.Query.Attributes.Add(new FetchAttribute { Name = col });
        }

        // Link entity — use the LINQ parameter name as the alias
        var link = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyAttr,
            Alias = resultLambda.Parameters[1].Name!,
            LinkType = "inner"
        };

        if (innerColumns is { Count: > 0 })
        {
            foreach (var col in innerColumns)
                link.Attributes.Add(new FetchAttribute { Name = col });
        }
        else
        {
            link.AllAttributes = true;
        }

        ctx.Query.Links.Add(link);
        ctx.Query.Projector = resultLambda.Compile();
        ctx.Query.ProjectionType = resultLambda.ReturnType;
        ctx.Query.InnerEntityType = innerEntityType;
    }

    // -------------------------------------------------------------------------
    // SelectMany — left join when source is GroupJoin
    // -------------------------------------------------------------------------

    private static void HandleSelectMany(MethodCallExpression call, TranslationContext ctx)
    {
        var source = call.Arguments[0];

        if (source is not MethodCallExpression groupJoinCall ||
            groupJoinCall.Method.DeclaringType != typeof(Queryable) ||
            groupJoinCall.Method.Name != nameof(Queryable.GroupJoin))
        {
            throw new NotSupportedException(
                "SelectMany is only supported as part of a left join pattern (GroupJoin + SelectMany).");
        }

        // Extract join keys from GroupJoin
        var (outerLogicalName, outerEntityType) = GetSourceInfo(groupJoinCall.Arguments[0]);
        var (innerLogicalName, _) = GetSourceInfo(groupJoinCall.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[2]).Body)
            ?? throw new NotSupportedException(
                "Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(groupJoinCall.Arguments[3]).Body)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        // Root entity
        ctx.Query.EntityLogicalName = outerLogicalName;

        // Analyse the SelectMany result selector to determine whether the C# compiler
        // folded the final Select into it (no subsequent Where/Select) or created a
        // transparent-identifier wrapper (further operators follow).
        var resultSelector = ExtractLambda(call.Arguments[2]);

        // Link entity — use the LINQ parameter name as the alias
        ctx.Query.Links.Add(new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyAttr,
            Alias = resultSelector.Parameters[1].Name!,
            LinkType = "outer"
        });
        var outerPath = FindOuterPropertyPath(resultSelector.Parameters[0].Type, outerEntityType);

        if (outerPath is null)
            return;

        // Try to extract columns directly from the result selector.
        // If the selector IS the final projection (select folded in), this succeeds.
        // If it's a transparent-identifier wrapper, no columns are found and we
        // set up context for subsequent Select/Where instead.
        var columns = ExtractColumnsViaPath(resultSelector, outerPath);

        if (columns is { Count: > 0 })
        {
            // Select folded into SelectMany — handle projection here
            ctx.Query.AllAttributes = false;
            foreach (var col in columns)
                ctx.Query.Attributes.Add(new FetchAttribute { Name = col });

            ctx.Query.Projector = RebuildProjector(resultSelector, outerPath, outerEntityType);
            ctx.Query.ProjectionType = resultSelector.ReturnType;
        }
        else
        {
            // Transparent-identifier wrapper — set up context for subsequent operators.
            // The outer path must be computed on the RESULT type (the TI), not the
            // parameter type, because subsequent lambdas receive the TI as their parameter.
            ctx.OuterEntityType = outerEntityType;
            ctx.OuterEntityPath = FindOuterPropertyPath(resultSelector.ReturnType, outerEntityType);

            // Identify the inner entity property in the TI for null-check detection.
            var innerParam = resultSelector.Parameters[1];
            foreach (var prop in resultSelector.ReturnType.GetProperties())
            {
                if (prop.PropertyType == innerParam.Type)
                {
                    ctx.InnerEntityProperty = prop.Name;
                    break;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Column extraction
    // -------------------------------------------------------------------------

    /// <summary>
    /// Extracts attribute logical names from a simple select body (anonymous type,
    /// member-init, or single property access).
    /// </summary>
    private static IReadOnlyList<string>? ExtractColumns(Expression body)
    {
        var columns = new List<string>();

        IEnumerable<Expression> args = body switch
        {
            NewExpression ne => ne.Arguments,
            MemberInitExpression init => init.Bindings
                .OfType<MemberAssignment>().Select(b => b.Expression),
            MemberExpression me => [me],
            _ => []
        };

        foreach (var arg in args)
        {
            var name = GetAttributeName(arg);
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
    private static IReadOnlyList<string>? ExtractColumnsViaPath(
        LambdaExpression lambda, string[] outerPath)
    {
        var param = lambda.Parameters[0];
        var columns = new List<string>();

        IEnumerable<Expression> args = lambda.Body switch
        {
            NewExpression ne => ne.Arguments,
            MemberInitExpression init => init.Bindings
                .OfType<MemberAssignment>().Select(b => b.Expression),
            _ => []
        };

        foreach (var arg in args)
        {
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: { } attrExpr }
                && IsOuterEntityAccess(attrExpr, param, outerPath))
            {
                var name = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (name is not null)
                    columns.Add(name);
            }
        }

        return columns.Count > 0 ? columns : null;
    }

    /// <summary>
    /// Extracts outer and inner column lists from an inner-join result selector lambda.
    /// </summary>
    private static (IReadOnlyList<string>? Outer, IReadOnlyList<string>? Inner) ExtractJoinColumns(
        LambdaExpression lambda, ParameterExpression outerParam, ParameterExpression innerParam)
    {
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
    // Transparent identifier helpers
    // -------------------------------------------------------------------------

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

    /// <summary>
    /// Returns <c>true</c> when <paramref name="expr"/> matches the member-access chain
    /// <c>param.path[0].path[1]...path[n-1]</c>.
    /// </summary>
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

    /// <summary>
    /// Rewrites a lambda that accesses the outer entity through a transparent-identifier
    /// chain into a single-parameter lambda that receives the outer entity directly.
    /// E.g. <c>ti => new { ti.ti0.a.Name }</c> → <c>(Account outer) => new { outer.Name }</c>.
    /// </summary>
    private static Delegate RebuildProjector(
        LambdaExpression lambda, string[] outerPath, Type outerEntityType)
    {
        var originalParam = lambda.Parameters[0];
        var outerParam = Expression.Parameter(outerEntityType, "outer");

        var newBody = new OuterEntityRewriter(originalParam, outerParam, outerPath)
            .Visit(lambda.Body);

        return Expression.Lambda(newBody, outerParam).Compile();
    }

    private sealed class OuterEntityRewriter(
        ParameterExpression originalParam,
        ParameterExpression outerParam,
        string[] outerPath) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null
                && IsOuterEntityAccess(node.Expression, originalParam, outerPath))
                return Expression.MakeMemberAccess(outerParam, node.Member);

            return base.VisitMember(node);
        }
    }

    // -------------------------------------------------------------------------
    // Null check detection
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns <c>true</c> when the lambda body is <c>param.property == null</c>
    /// where <paramref name="innerPropertyName"/> is the property holding the inner
    /// entity in the transparent-identifier type.
    /// </summary>
    private static bool IsNullCheck(LambdaExpression lambda, string innerPropertyName)
    {
        if (lambda.Body is not BinaryExpression { NodeType: ExpressionType.Equal } binary)
            return false;

        var (memberSide, constSide) = binary.Left is ConstantExpression
            ? (binary.Right, binary.Left)
            : (binary.Left, binary.Right);

        if (constSide is not ConstantExpression { Value: null })
            return false;

        return memberSide is MemberExpression { Member.Name: var name, Expression: ParameterExpression p }
            && p == lambda.Parameters[0]
            && name == innerPropertyName;
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
                var logicalName = GetEntityLogicalName(entityType);
                return (logicalName, entityType);
            }
        }

        throw new NotSupportedException("Query sources must be DataverseQueryable<T> instances.");
    }

    private static string GetEntityLogicalName(Type entityType) =>
        entityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
        ?? throw new InvalidOperationException(
            $"Type '{entityType.Name}' must be decorated with [EntityLogicalName].");

    private static LambdaExpression ExtractLambda(Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    /// <summary>
    /// Resolves an expression to a Dataverse attribute logical name.
    /// Handles direct property access (<c>a.AccountId</c>), two-level
    /// EntityReference access (<c>c.ParentAccount.Id</c>), and
    /// <see cref="Entity.GetAttributeValue{T}"/> calls with a string-constant argument.
    /// </summary>
    private static string? GetAttributeName(Expression expr)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        // Entity.GetAttributeValue<T>("name")
        if (expr is MethodCallExpression { Method.Name: nameof(Entity.GetAttributeValue) } getAttr
            && getAttr.Arguments.Count == 1
            && getAttr.Arguments[0] is ConstantExpression { Value: string constName })
        {
            return constName;
        }

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

    // -------------------------------------------------------------------------
    // Translation context — carries state across recursive operator processing
    // -------------------------------------------------------------------------

    private sealed class TranslationContext(FetchXmlQuery query, Type rootEntityType)
    {
        public FetchXmlQuery Query { get; } = query;
        public Type RootEntityType { get; } = rootEntityType;

        /// <summary>
        /// After a left join: the CLR type of the outer (root) entity.
        /// </summary>
        public Type? OuterEntityType { get; set; }

        /// <summary>
        /// After a left join: the property-name chain from the transparent-identifier
        /// type to the outer entity (e.g. <c>["&lt;&gt;h__TransparentIdentifier0", "a"]</c>).
        /// When set, subsequent Select/Where lambdas operate on a transparent-identifier
        /// parameter and member accesses must be resolved through this path.
        /// </summary>
        public string[]? OuterEntityPath { get; set; }

        /// <summary>
        /// After a left join: the property name on the transparent-identifier type
        /// that holds the inner (joined) entity. Used by <see cref="HandleWhere"/>
        /// to detect <c>where c == null</c> patterns.
        /// </summary>
        public string? InnerEntityProperty { get; set; }
    }
}

using Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
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
    private static readonly Dictionary<string, ConditionOperator> DateTimeOperatorMap = new()
    {
        ["Last7Days"] = ConditionOperator.Last7Days,
        ["LastFiscalPeriod"] = ConditionOperator.LastFiscalPeriod,
        ["LastFiscalYear"] = ConditionOperator.LastFiscalYear,
        ["LastMonth"] = ConditionOperator.LastMonth,
        ["LastWeek"] = ConditionOperator.LastWeek,
        ["LastYear"] = ConditionOperator.LastYear,
        ["LastXDays"] = ConditionOperator.LastXDays,
        ["LastXFiscalPeriods"] = ConditionOperator.LastXFiscalPeriods,
        ["LastXFiscalYears"] = ConditionOperator.LastXFiscalYears,
        ["LastXHours"] = ConditionOperator.LastXHours,
        ["LastXMonths"] = ConditionOperator.LastXMonths,
        ["LastXWeeks"] = ConditionOperator.LastXWeeks,
        ["LastXYears"] = ConditionOperator.LastXYears,
        ["Next7Days"] = ConditionOperator.Next7Days,
        ["NextFiscalPeriod"] = ConditionOperator.NextFiscalPeriod,
        ["NextFiscalYear"] = ConditionOperator.NextFiscalYear,
        ["NextMonth"] = ConditionOperator.NextMonth,
        ["NextWeek"] = ConditionOperator.NextWeek,
        ["NextYear"] = ConditionOperator.NextYear,
        ["NextXDays"] = ConditionOperator.NextXDays,
        ["NextXFiscalPeriods"] = ConditionOperator.NextXFiscalPeriods,
        ["NextXFiscalYears"] = ConditionOperator.NextXFiscalYears,
        ["NextXHours"] = ConditionOperator.NextXHours,
        ["NextXMonths"] = ConditionOperator.NextXMonths,
        ["NextXWeeks"] = ConditionOperator.NextXWeeks,
        ["NextXYears"] = ConditionOperator.NextXYears,
        ["InFiscalPeriod"] = ConditionOperator.InFiscalPeriod,
        ["InFiscalPeriodAndYear"] = ConditionOperator.InFiscalPeriodAndYear,
        ["InFiscalYear"] = ConditionOperator.InFiscalYear,
        ["InOrAfterFiscalPeriodAndYear"] = ConditionOperator.InOrAfterFiscalPeriodAndYear,
        ["InOrBeforeFiscalPeriodAndYear"] = ConditionOperator.InOrBeforeFiscalPeriodAndYear,
        ["OlderThanXMonths"] = ConditionOperator.OlderThanXMonths,
        ["On"] = ConditionOperator.On,
        ["OnOrAfter"] = ConditionOperator.OnOrAfter,
        ["OnOrBefore"] = ConditionOperator.OnOrBefore,
        ["ThisFiscalPeriod"] = ConditionOperator.ThisFiscalPeriod,
        ["ThisFiscalYear"] = ConditionOperator.ThisFiscalYear,
        ["ThisMonth"] = ConditionOperator.ThisMonth,
        ["ThisWeek"] = ConditionOperator.ThisWeek,
        ["ThisYear"] = ConditionOperator.ThisYear,
        ["Today"] = ConditionOperator.Today,
        ["Tomorrow"] = ConditionOperator.Tomorrow,
        ["Yesterday"] = ConditionOperator.Yesterday,
        ["Between"] = ConditionOperator.Between,
        ["NotBetween"] = ConditionOperator.NotBetween,
    };

    private static readonly Dictionary<string, ConditionOperator> HierarchyOperatorMap = new()
    {
        ["Above"] = ConditionOperator.Above,
        ["AboveOrEqual"] = ConditionOperator.AboveOrEqual,
        ["Under"] = ConditionOperator.Under,
        ["UnderOrEqual"] = ConditionOperator.UnderOrEqual,
        ["NotUnder"] = ConditionOperator.NotUnder,
        ["EqualUserOrUserHierarchy"] = ConditionOperator.EqualUserOrUserHierarchy,
        ["EqualUserOrUserHierarchyAndTeams"] = ConditionOperator.EqualUserOrUserHierarchyAndTeams,
    };

    private static readonly Dictionary<string, ConditionOperator> UserOperatorMap = new()
    {
        ["EqualUserId"] = ConditionOperator.EqualUserId,
        ["NotEqualUserId"] = ConditionOperator.NotEqualUserId,
        ["EqualBusinessId"] = ConditionOperator.EqualBusinessId,
        ["NotEqualBusinessId"] = ConditionOperator.NotEqualBusinessId,
    };

    private static readonly Dictionary<string, ConditionOperator> MultiSelectOperatorMap = new()
    {
        ["ContainValues"] = ConditionOperator.ContainValues,
        ["DoesNotContainValues"] = ConditionOperator.DoesNotContainValues,
    };

    private static bool TryGetExtensionOperator(Type? declaringType, string methodName, out ConditionOperator op)
    {
        if (declaringType == typeof(Extensions.DateTimeExtensions))
            return DateTimeOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(Extensions.HierarchyExtensions))
            return HierarchyOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(Extensions.UserExtensions))
            return UserOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(Extensions.MultiSelectExtensions))
            return MultiSelectOperatorMap.TryGetValue(methodName, out op);
        op = default;
        return false;
    }

    private static ConditionOperator NegateOperator(ConditionOperator op) => op switch
    {
        ConditionOperator.ContainValues => ConditionOperator.DoesNotContainValues,
        ConditionOperator.DoesNotContainValues => ConditionOperator.ContainValues,
        _ => throw new NotSupportedException($"Negation of the '{op}' operator is not supported.")
    };

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

        if (ctx.JoinMappings is not null)
        {
            // After an inner join with transparent identifier
            HandleJoinSelect(lambda, ctx);
        }
        else if (ctx.OuterEntityPath is not null)
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

    private static void HandleJoinSelect(LambdaExpression lambda, TranslationContext ctx)
    {
        var link = ctx.Query.Links[^1];
        var rootColumns = new List<string>();
        var linkColumns = new List<string>();

        IEnumerable<Expression> args = lambda.Body switch
        {
            NewExpression ne => ne.Arguments,
            MemberInitExpression init => init.Bindings
                .OfType<MemberAssignment>().Select(b => b.Expression),
            _ => []
        };

        foreach (var arg in args)
        {
            var resolved = ResolveAttribute(arg, ctx);
            if (resolved is null) continue;

            if (resolved.Value.EntityAlias is null)
                rootColumns.Add(resolved.Value.Name);
            else
                linkColumns.Add(resolved.Value.Name);
        }

        if (rootColumns.Count > 0)
        {
            ctx.Query.AllAttributes = false;
            foreach (var col in rootColumns)
                ctx.Query.Attributes.Add(new FetchAttribute { Name = col });
        }

        if (linkColumns.Count > 0)
        {
            foreach (var col in linkColumns)
                link.Attributes.Add(new FetchAttribute { Name = col });
        }

        ctx.Query.Projector = RebuildJoinProjector(lambda, ctx);
        ctx.Query.ProjectionType = lambda.ReturnType;
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
                Operator = ConditionOperator.Null
            });
            return;
        }

        // General predicate
        var rootFilter = ctx.Query.Filter ??= new FetchFilter();
        TranslatePredicate(lambda.Body, rootFilter, ctx);

        // Collapse unnecessary nesting: if root has exactly one sub-filter and no
        // conditions, promote the sub-filter to root level.
        if (rootFilter.Conditions.Count == 0 && rootFilter.Filters.Count == 1)
        {
            var child = rootFilter.Filters[0];
            rootFilter.Type = child.Type;
            rootFilter.Filters.Clear();
            foreach (var c in child.Conditions)
                rootFilter.Conditions.Add(c);
            foreach (var f in child.Filters)
                rootFilter.Filters.Add(f);
        }
    }

    private static void TranslatePredicate(Expression expr, FetchFilter filter, TranslationContext ctx, bool negated = false)
    {
        switch (expr)
        {
            // !expr → recurse with negated flag
            case UnaryExpression { NodeType: ExpressionType.Not, Operand: var operand }:
                TranslatePredicate(operand, filter, ctx, negated: !negated);
                return;

            // string.IsNullOrEmpty(x.Attr) → null OR eq ""  /  negated → not-null AND ne ""
            case MethodCallExpression { Method: { Name: "IsNullOrEmpty", DeclaringType: var dt } } isNullCall
                when dt == typeof(string):
            {
                var resolved = ResolveMethodAttribute(isNullCall, ctx);
                var (filterType, nullOp, emptyOp) = negated
                    ? (FilterType.And, ConditionOperator.NotNull, ConditionOperator.NotEqual)
                    : (FilterType.Or, ConditionOperator.Null, ConditionOperator.Equal);
                var subFilter = new FetchFilter { Type = filterType };
                subFilter.Conditions.Add(new FetchCondition { Attribute = resolved.Name, EntityAlias = resolved.EntityAlias, Operator = nullOp });
                subFilter.Conditions.Add(new FetchCondition { Attribute = resolved.Name, EntityAlias = resolved.EntityAlias, Operator = emptyOp, Value = "" });
                filter.Filters.Add(subFilter);
                return;
            }

            // collection.Contains(x.Attr) → in  /  negated → not-in
            case MethodCallExpression { Method: { Name: "Contains" } } containsCall
                when TryResolveInPredicate(containsCall, ctx) is { } inResult:
            {
                var condition = new FetchCondition
                {
                    Attribute = inResult.Resolved.Name,
                    EntityAlias = inResult.Resolved.EntityAlias,
                    Operator = negated ? ConditionOperator.NotIn : ConditionOperator.In
                };
                foreach (var val in inResult.Values)
                    condition.Values.Add(val);
                filter.Conditions.Add(condition);
                return;
            }

            // x.Attr.Contains("value") / StartsWith / EndsWith → like  /  negated → not-like
            case MethodCallExpression { Method: { Name: "Contains" or "StartsWith" or "EndsWith" } } stringCall
                when stringCall.Method.DeclaringType == typeof(string) && stringCall.Object is not null:
            {
                var (resolved, pattern) = ResolveStringMethodAttribute(stringCall, ctx);
                filter.Conditions.Add(new FetchCondition
                {
                    Attribute = resolved.Name,
                    EntityAlias = resolved.EntityAlias,
                    Operator = negated ? ConditionOperator.NotLike : ConditionOperator.Like,
                    Value = pattern
                });
                return;
            }

            // Extension method operators (DateTime, hierarchy, multi-select) → condition operators
            case MethodCallExpression { Method.DeclaringType: var declType } dateCall
                when TryGetExtensionOperator(declType, dateCall.Method.Name, out var dateOp):
            {
                var resolved = ResolveMethodAttribute(dateCall, ctx);
                var effectiveOp = negated ? NegateOperator(dateOp) : dateOp;
                var condition = new FetchCondition
                {
                    Attribute = resolved.Name,
                    EntityAlias = resolved.EntityAlias,
                    Operator = effectiveOp
                };
                // arg[0] is the 'this' parameter; remaining args are values
                var extraArgs = dateCall.Arguments.Count - 1;
                if (extraArgs == 1)
                {
                    var value = EvaluateValue(dateCall.Arguments[1]);
                    if (value is System.Collections.IEnumerable enumerable and not string)
                    {
                        foreach (var item in enumerable)
                            condition.Values.Add(item!);
                    }
                    else
                    {
                        condition.Value = value;
                    }
                }
                else if (extraArgs > 1)
                {
                    for (var i = 1; i < dateCall.Arguments.Count; i++)
                        condition.Values.Add(EvaluateValue(dateCall.Arguments[i])!);
                }
                filter.Conditions.Add(condition);
                return;
            }

            // && → AND filter (flatten if parent is already AND)
            case BinaryExpression { NodeType: ExpressionType.AndAlso } andExpr:
                TranslateLogicalPredicate(andExpr, filter, FilterType.And, ctx);
                return;

            // || → OR filter (flatten if parent is already OR)
            case BinaryExpression { NodeType: ExpressionType.OrElse } orExpr:
                TranslateLogicalPredicate(orExpr, filter, FilterType.Or, ctx);
                return;

            // Comparison operators (==, !=, <, <=, >, >=)
            case BinaryExpression binary:
                TranslateComparisonPredicate(binary, filter, ctx);
                return;

            default:
                throw new NotSupportedException(
                    $"Unsupported Where predicate: {expr.NodeType}");
        }
    }

    private static ResolvedAttribute ResolveMethodAttribute(MethodCallExpression call, TranslationContext ctx)
    {
        var attrExpr = call.Object ?? call.Arguments[0];
        return ResolveAttribute(attrExpr, ctx)
            ?? throw new NotSupportedException(
                $"{call.Method.DeclaringType!.Name}.{call.Method.Name} argument must resolve to an attribute.");
    }

    private static (ResolvedAttribute Resolved, string Pattern) ResolveStringMethodAttribute(
        MethodCallExpression call, TranslationContext ctx)
    {
        var resolved = ResolveMethodAttribute(call, ctx);
        var value = EvaluateValue(call.Arguments[0]);
        var pattern = call.Method.Name switch
        {
            "Contains" => $"%{value}%",
            "StartsWith" => $"{value}%",
            "EndsWith" => $"%{value}",
            _ => throw new NotSupportedException()
        };
        return (resolved, pattern);
    }

    private record InPredicateResult(ResolvedAttribute Resolved, List<object> Values);

    private static InPredicateResult? TryResolveInPredicate(MethodCallExpression call, TranslationContext ctx)
    {
        // Two patterns:
        // 1. Static: Enumerable.Contains(collection, entity.Attr) — 2 args, no Object
        // 2. Instance: list.Contains(entity.Attr) — 1 arg, Object is the collection
        Expression? collectionExpr;
        Expression? attrExpr;

        if (call.Object is null && call.Arguments.Count == 2)
        {
            // Static Enumerable.Contains<T>(IEnumerable<T>, T)
            collectionExpr = call.Arguments[0];
            attrExpr = call.Arguments[1];
        }
        else if (call.Object is not null && call.Arguments.Count == 1)
        {
            // Instance List<T>.Contains(T)
            collectionExpr = call.Object;
            attrExpr = call.Arguments[0];
        }
        else
        {
            return null;
        }

        var resolved = ResolveAttribute(attrExpr, ctx);
        if (resolved is null)
            return null;

        var collection = EvaluateValue(collectionExpr);
        if (collection is not System.Collections.IEnumerable enumerable)
            return null;

        var values = new List<object>();
        foreach (var item in enumerable)
            values.Add(item);

        return new InPredicateResult(resolved.Value, values);
    }

    private static void TranslateLogicalPredicate(
        BinaryExpression expr, FetchFilter filter, FilterType type, TranslationContext ctx)
    {
        if (filter.Type == type)
        {
            // Same type as parent — flatten into parent
            TranslatePredicate(expr.Left, filter, ctx);
            TranslatePredicate(expr.Right, filter, ctx);
        }
        else
        {
            // Different type — create sub-filter
            var subFilter = new FetchFilter { Type = type };
            TranslatePredicate(expr.Left, subFilter, ctx);
            TranslatePredicate(expr.Right, subFilter, ctx);
            filter.Filters.Add(subFilter);
        }
    }

    private static void TranslateComparisonPredicate(
        BinaryExpression binary, FetchFilter filter, TranslationContext ctx)
    {
        var op = binary.NodeType switch
        {
            ExpressionType.Equal => ConditionOperator.Equal,
            ExpressionType.NotEqual => ConditionOperator.NotEqual,
            ExpressionType.LessThan => ConditionOperator.LessThan,
            ExpressionType.LessThanOrEqual => ConditionOperator.LessEqual,
            ExpressionType.GreaterThan => ConditionOperator.GreaterThan,
            ExpressionType.GreaterThanOrEqual => ConditionOperator.GreaterEqual,
            _ => throw new NotSupportedException(
                $"Unsupported comparison operator in Where: {binary.NodeType}")
        };

        // Determine which side is the attribute and which is the value
        var leftResolved = ResolveAttribute(binary.Left, ctx);
        var rightResolved = ResolveAttribute(binary.Right, ctx);

        ResolvedAttribute attr;
        Expression valueExpr;

        if (leftResolved is not null)
        {
            attr = leftResolved.Value;
            valueExpr = binary.Right;
        }
        else if (rightResolved is not null)
        {
            attr = rightResolved.Value;
            valueExpr = binary.Left;
        }
        else
        {
            throw new NotSupportedException(
                "One side of a comparison must resolve to an attribute.");
        }

        // null comparison → null / not-null
        if (IsNullConstant(valueExpr))
        {
            filter.Conditions.Add(new FetchCondition
            {
                Attribute = attr.Name,
                EntityAlias = attr.EntityAlias,
                Operator = op == ConditionOperator.Equal ? ConditionOperator.Null : ConditionOperator.NotNull
            });
            return;
        }

        var value = EvaluateValue(valueExpr);
        filter.Conditions.Add(new FetchCondition
        {
            Attribute = attr.Name,
            EntityAlias = attr.EntityAlias,
            Operator = op,
            Value = value
        });
    }

    private static bool IsNullConstant(Expression expr) =>
        expr is ConstantExpression { Value: null }
        || (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            && convert.Operand is ConstantExpression { Value: null });

    private static object? EvaluateValue(Expression expr)
    {
        if (expr is ConstantExpression constant)
            return constant.Value;

        // Closure field/property access: closureInstance.field (may be nested)
        if (expr is MemberExpression memberExpr)
        {
            var target = memberExpr.Expression is not null
                ? EvaluateValue(memberExpr.Expression)
                : null;
            return memberExpr.Member switch
            {
                FieldInfo fi => fi.GetValue(target),
                PropertyInfo pi => pi.GetValue(target),
                _ => throw new NotSupportedException($"Unsupported member type: {memberExpr.Member.GetType()}")
            };
        }

        // new[] { ... } inline array
        if (expr is NewArrayExpression newArray)
        {
            var items = newArray.Expressions.Select(e => EvaluateValue(e)).ToArray();
            var array = Array.CreateInstance(newArray.Type.GetElementType()!, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }

        // Convert / cast
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return EvaluateValue(convert.Operand);

        // Method call (e.g. implicit conversions, static methods)
        if (expr is MethodCallExpression methodCall)
        {
            // Implicit/explicit conversion operators with a single argument — just evaluate the argument
            if (methodCall.Method is { IsSpecialName: true, Name: "op_Implicit" or "op_Explicit" }
                && methodCall.Arguments.Count == 1)
            {
                return EvaluateValue(methodCall.Arguments[0]);
            }

            var target = methodCall.Object is not null ? EvaluateValue(methodCall.Object) : null;
            var args = methodCall.Arguments.Select(a => EvaluateValue(a)).ToArray();
            return methodCall.Method.Invoke(target, args);
        }

        // Constructor call (e.g. new DateTime(2020, 1, 1))
        if (expr is NewExpression newExpr && newExpr.Constructor is not null)
        {
            var args = newExpr.Arguments.Select(a => EvaluateValue(a)).ToArray();
            return newExpr.Constructor.Invoke(args);
        }

        throw new NotSupportedException(
            $"Unable to evaluate expression of type {expr.GetType().Name} (NodeType={expr.NodeType}): {expr}");
    }

    // -------------------------------------------------------------------------
    // Attribute resolution
    // -------------------------------------------------------------------------

    private record struct ResolvedAttribute(string Name, string? EntityAlias);

    /// <summary>
    /// Resolves an expression to a Dataverse attribute name and optional entity alias.
    /// Supports direct property access, EntityReference.Id, GetAttributeValue, and
    /// two-level access through join transparent identifiers.
    /// </summary>
    private static ResolvedAttribute? ResolveAttribute(Expression expr, TranslationContext ctx)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        // Join transparent identifier: x.entity.Property
        if (ctx.JoinMappings is not null)
        {
            var joinResult = ResolveJoinAttribute(expr, ctx.JoinMappings);
            if (joinResult is not null)
                return joinResult;
        }

        // Simple (non-join) resolution via GetAttributeName
        var simple = GetAttributeName(expr);
        if (simple is not null)
            return new ResolvedAttribute(simple, null);

        return null;
    }

    private static ResolvedAttribute? ResolveJoinAttribute(
        Expression expr, Dictionary<string, JoinEntityInfo> joinMappings)
    {
        // x.entity.Property (property with [AttributeLogicalName])
        if (expr is MemberExpression { Member: PropertyInfo prop } me
            && me.Expression is MemberExpression { Expression: ParameterExpression } entityAccess
            && joinMappings.TryGetValue(entityAccess.Member.Name, out var mapping))
        {
            var attrName = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is not null)
                return new ResolvedAttribute(attrName, mapping.LinkAlias);
        }

        // x.entity.RefProp.Id (EntityReference.Id through TI)
        if (expr is MemberExpression { Member.Name: "Id" } idExpr
            && idExpr.Expression is MemberExpression { Member: PropertyInfo refProp } refExpr
            && refExpr.Expression is MemberExpression { Expression: ParameterExpression } entityAccess2
            && joinMappings.TryGetValue(entityAccess2.Member.Name, out var mapping2))
        {
            var attrName = refProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is not null)
                return new ResolvedAttribute(attrName, mapping2.LinkAlias);
        }

        // x.entity.GetAttributeValue<T>("name")
        if (expr is MethodCallExpression { Method.Name: nameof(Entity.GetAttributeValue) } getAttr
            && getAttr.Arguments.Count == 1
            && getAttr.Arguments[0] is ConstantExpression { Value: string constName }
            && getAttr.Object is MemberExpression { Expression: ParameterExpression } entityAccess3
            && joinMappings.TryGetValue(entityAccess3.Member.Name, out var mapping3))
        {
            return new ResolvedAttribute(constName, mapping3.LinkAlias);
        }

        return null;
    }

    // -------------------------------------------------------------------------
    // OrderBy / ThenBy
    // -------------------------------------------------------------------------

    private static void HandleOrderBy(MethodCallExpression call, TranslationContext ctx)
    {
        var lambda = ExtractLambda(call.Arguments[1]);
        var descending = call.Method.Name is nameof(Queryable.OrderByDescending)
                                           or nameof(Queryable.ThenByDescending);

        var resolved = ResolveAttribute(lambda.Body, ctx)
            ?? throw new NotSupportedException(
                "OrderBy key must resolve to an attribute.");

        ctx.Query.Orders.Add(new FetchOrder
        {
            Attribute = resolved.Name,
            EntityAlias = resolved.EntityAlias,
            Descending = descending
        });
    }

    // -------------------------------------------------------------------------
    // Inner join — Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    // -------------------------------------------------------------------------

    private static void HandleInnerJoin(MethodCallExpression call, TranslationContext ctx)
    {
        var (outerLogicalName, outerEntityType) = GetSourceInfo(call.Arguments[0]);
        var (innerLogicalName, innerEntityType) = GetSourceInfo(call.Arguments[1]);

        var outerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[2]).Body)
            ?? throw new NotSupportedException(
                "Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKeyAttr = GetAttributeName(ExtractLambda(call.Arguments[3]).Body)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        var resultLambda = ExtractLambda(call.Arguments[4]);

        // Root entity
        ctx.Query.EntityLogicalName = outerLogicalName;

        // Link entity — use the LINQ parameter name as the alias
        var link = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyAttr,
            Alias = resultLambda.Parameters[1].Name!,
            LinkType = "inner"
        };
        ctx.Query.Links.Add(link);

        // Detect transparent identifier: (c, a) => new { c, a }
        // When subsequent operators (Where/OrderBy/Select) follow the join,
        // the compiler wraps the result in a TI rather than projecting directly.
        if (IsTransparentIdentifier(resultLambda))
        {
            ctx.JoinMappings = new Dictionary<string, JoinEntityInfo>
            {
                [resultLambda.Parameters[0].Name!] = new() { EntityType = outerEntityType, LinkAlias = null },
                [resultLambda.Parameters[1].Name!] = new() { EntityType = innerEntityType, LinkAlias = link.Alias }
            };
            ctx.Query.InnerEntityType = innerEntityType;
            return;
        }

        // Final projection in the join result selector
        var (outerColumns, innerColumns) = ExtractJoinColumns(
            resultLambda, resultLambda.Parameters[0], resultLambda.Parameters[1]);

        if (outerColumns is { Count: > 0 })
        {
            ctx.Query.AllAttributes = false;
            foreach (var col in outerColumns)
                ctx.Query.Attributes.Add(new FetchAttribute { Name = col });
        }

        if (innerColumns is { Count: > 0 })
        {
            foreach (var col in innerColumns)
                link.Attributes.Add(new FetchAttribute { Name = col });
        }
        else
        {
            link.AllAttributes = true;
        }

        ctx.Query.Projector = resultLambda.Compile();
        ctx.Query.ProjectionType = resultLambda.ReturnType;
        ctx.Query.InnerEntityType = innerEntityType;
    }

    private static bool IsTransparentIdentifier(LambdaExpression lambda) =>
        lambda.Body is NewExpression ne
        && ne.Arguments.Count > 0
        && ne.Arguments.All(a => a is ParameterExpression);

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

    /// <summary>
    /// Rewrites a lambda that accesses entities through a join transparent identifier
    /// into a two-parameter lambda <c>(outer, inner) => ...</c> for use by
    /// <see cref="DataverseQueryProvider{T}.ProjectEntities{TElement}"/>.
    /// </summary>
    private static Delegate RebuildJoinProjector(LambdaExpression lambda, TranslationContext ctx)
    {
        var tiParam = lambda.Parameters[0];

        Type outerType = null!, innerType = null!;
        string outerPropName = null!, innerPropName = null!;

        foreach (var (name, info) in ctx.JoinMappings!)
        {
            if (info.LinkAlias is null)
            {
                outerType = info.EntityType;
                outerPropName = name;
            }
            else
            {
                innerType = info.EntityType;
                innerPropName = name;
            }
        }

        var outerParam = Expression.Parameter(outerType, "outer");
        var innerParam = Expression.Parameter(innerType, "inner");

        var rewriter = new JoinProjectorRewriter(tiParam, outerParam, innerParam, outerPropName, innerPropName);
        var newBody = rewriter.Visit(lambda.Body);

        return Expression.Lambda(newBody, outerParam, innerParam).Compile();
    }

    private sealed class JoinProjectorRewriter(
        ParameterExpression tiParam,
        ParameterExpression outerParam,
        ParameterExpression innerParam,
        string outerPropName,
        string innerPropName) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            // x.entity.Property → outerParam.Property or innerParam.Property
            if (node.Expression is MemberExpression { Expression: ParameterExpression p } entityAccess
                && p == tiParam)
            {
                if (entityAccess.Member.Name == outerPropName)
                    return Expression.MakeMemberAccess(outerParam, node.Member);
                if (entityAccess.Member.Name == innerPropName)
                    return Expression.MakeMemberAccess(innerParam, node.Member);
            }

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

        /// <summary>
        /// After an inner join with subsequent operators: maps property names on the
        /// transparent-identifier type to their entity info (root or link entity).
        /// </summary>
        public Dictionary<string, JoinEntityInfo>? JoinMappings { get; set; }
    }

    private sealed class JoinEntityInfo
    {
        public required Type EntityType { get; init; }
        public string? LinkAlias { get; init; }
    }
}

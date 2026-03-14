using Dataverse.Linq.Extensions;
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
        ["ContainsValues"] = ConditionOperator.ContainValues,
    };

    private static readonly Dictionary<string, string> AggregateFunctionMap = new()
    {
        [nameof(Queryable.Count)] = "count",
        [nameof(Queryable.LongCount)] = "count",
        [nameof(Queryable.Sum)] = "sum",
        [nameof(Queryable.Average)] = "avg",
        [nameof(Queryable.Min)] = "min",
        [nameof(Queryable.Max)] = "max",
        [nameof(ServiceClientExtensions.CountColumn)] = "countcolumn",
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
        ConditionOperator.Between => ConditionOperator.NotBetween,
        ConditionOperator.NotBetween => ConditionOperator.Between,
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

                    case nameof(Queryable.First):
                    case nameof(Queryable.FirstOrDefault):
                    case nameof(Queryable.Single):
                    case nameof(Queryable.SingleOrDefault):
                        HandleTerminalOperator(call, ctx);
                        return;

                    case nameof(Queryable.Distinct):
                        TranslateCore(call.Arguments[0], ctx);
                        ctx.Query.Distinct = true;
                        return;

                    case nameof(Queryable.Take):
                        TranslateCore(call.Arguments[0], ctx);
                        ctx.Query.Top = (int)((ConstantExpression)call.Arguments[1]).Value!;
                        return;

                    case nameof(Queryable.GroupBy):
                        HandleGroupBy(call, ctx);
                        return;

                    case nameof(Queryable.Min):
                    case nameof(Queryable.Max):
                    case nameof(Queryable.Sum):
                    case nameof(Queryable.Average):
                    case nameof(Queryable.Count):
                    case nameof(Queryable.LongCount):
                        HandleAggregateOperator(call, ctx);
                        return;

                    default:
                        throw new NotSupportedException(
                            $"LINQ operator '{call.Method.Name}' is not supported.");
                }

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithPageSize),
                    DeclaringType: var dt } } pageSizeCall
                when dt == typeof(ServiceClientExtensions):
                TranslateCore(pageSizeCall.Arguments[0], ctx);
                ctx.Query.PageSize = (int)((ConstantExpression)pageSizeCall.Arguments[1]).Value!;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithPage),
                    DeclaringType: var dtP } } pageCall
                when dtP == typeof(ServiceClientExtensions):
                TranslateCore(pageCall.Arguments[0], ctx);
                ctx.Query.Page = (int)((ConstantExpression)pageCall.Arguments[1]).Value!;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithAggregateLimit),
                    DeclaringType: var dtAL } } aggLimitCall
                when dtAL == typeof(ServiceClientExtensions):
                TranslateCore(aggLimitCall.Arguments[0], ctx);
                ctx.Query.AggregateLimit = (int)((ConstantExpression)aggLimitCall.Arguments[1]).Value!;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithDatasource),
                    DeclaringType: var dtDS } } datasourceCall
                when dtDS == typeof(ServiceClientExtensions):
                TranslateCore(datasourceCall.Arguments[0], ctx);
                ctx.Query.Datasource = (FetchDatasource)((ConstantExpression)datasourceCall.Arguments[1]).Value!;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithLateMaterialize),
                    DeclaringType: var dtLM } } lateMaterializeCall
                when dtLM == typeof(ServiceClientExtensions):
                TranslateCore(lateMaterializeCall.Arguments[0], ctx);
                ctx.Query.LateMaterialize = true;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithNoLock),
                    DeclaringType: var dtNL } } noLockCall
                when dtNL == typeof(ServiceClientExtensions):
                TranslateCore(noLockCall.Arguments[0], ctx);
                ctx.Query.NoLock = true;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithQueryHints),
                    DeclaringType: var dtQH } } queryHintsCall
                when dtQH == typeof(ServiceClientExtensions):
                TranslateCore(queryHintsCall.Arguments[0], ctx);
                var hintsArray = (NewArrayExpression)queryHintsCall.Arguments[1];
                ctx.Query.QueryHints = hintsArray.Expressions
                    .Select(e => (SqlQueryHint)((ConstantExpression)e).Value!)
                    .ToList();
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.WithUseRawOrderBy),
                    DeclaringType: var dtRO } } rawOrderByCall
                when dtRO == typeof(ServiceClientExtensions):
                TranslateCore(rawOrderByCall.Arguments[0], ctx);
                ctx.Query.UseRawOrderBy = true;
                return;

            case MethodCallExpression { Method: { Name: nameof(ServiceClientExtensions.CountColumn),
                    DeclaringType: var dtCC } } countColumnCall
                when dtCC == typeof(ServiceClientExtensions):
                HandleAggregateOperator(countColumnCall, ctx);
                return;

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

        if (ctx.IsGrouped)
        {
            HandleGroupedSelect(lambda, ctx);
            return;
        }

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
                ApplyColumns(ctx.Query, columns);

            ctx.Query.Projector = RebuildProjector(lambda, ctx.OuterEntityPath, ctx.OuterEntityType!);
            ctx.Query.ProjectionType = lambda.ReturnType;
        }
        else
        {
            // Simple select on root entity
            var columns = ExtractColumns(lambda.Body);
            if (columns is { Count: > 0 })
                ApplyColumns(ctx.Query, columns);

            // Check for CountChildren() calls in the projection
            var rowAggregates = ExtractRowAggregates(lambda.Body);
            if (rowAggregates is { Count: > 0 })
            {
                foreach (var ra in rowAggregates)
                    ctx.Query.Attributes.Add(new FetchAttribute
                    {
                        Name = ctx.Query.EntityLogicalName + "id",
                        Alias = ra.Alias,
                        RowAggregate = "CountChildren"
                    });

                // Rewrite the projector to extract aliased values for CountChildren() calls
                var rewriter = new CountChildrenRewriter(lambda.Parameters[0]);
                var rewritten = rewriter.Visit(lambda.Body);
                ctx.Query.Projector = Expression.Lambda(rewritten, lambda.Parameters).Compile();
            }
            else
            {
                ctx.Query.Projector = lambda.Compile();
            }

            ctx.Query.ProjectionType = lambda.ReturnType;
        }
    }

    private static void HandleJoinSelect(LambdaExpression lambda, TranslationContext ctx)
    {
        // Collect columns keyed by entity alias ("" = root entity)
        var columnsByAlias = new Dictionary<string, List<string>>();

        foreach (var arg in GetProjectionArguments(lambda.Body))
            CollectJoinAttributesByAlias(arg, ctx, columnsByAlias);

        foreach (var (alias, columns) in columnsByAlias)
        {
            if (alias == "")
            {
                ApplyColumns(ctx.Query, columns);
            }
            else
            {
                var link = FindLinkByAlias(ctx.Query.Links, alias);
                if (link is not null)
                {
                    foreach (var col in columns)
                        link.Attributes.Add(new FetchAttribute { Name = col });
                }
            }
        }

        ctx.Query.Projector = RebuildJoinProjector(lambda, ctx);
        ctx.Query.ProjectionType = lambda.ReturnType;
    }

    /// <summary>
    /// Recursively walks an expression to find all attribute references in a join
    /// projection, keyed by entity alias (null for root entity).
    /// </summary>
    private static void CollectJoinAttributesByAlias(
        Expression expr, TranslationContext ctx,
        Dictionary<string, List<string>> columnsByAlias)
    {
        var resolved = ResolveAttribute(expr, ctx);
        if (resolved is not null)
        {
            var key = resolved.Value.EntityAlias ?? "";
            if (!columnsByAlias.TryGetValue(key, out var list))
            {
                list = [];
                columnsByAlias[key] = list;
            }
            list.Add(resolved.Value.Name);
            return;
        }

        // Walk into sub-expressions to find nested attribute references
        switch (expr)
        {
            case MethodCallExpression mc:
                if (mc.Object is not null)
                    CollectJoinAttributesByAlias(mc.Object, ctx, columnsByAlias);
                foreach (var a in mc.Arguments)
                    CollectJoinAttributesByAlias(a, ctx, columnsByAlias);
                break;

            case BinaryExpression binary:
                CollectJoinAttributesByAlias(binary.Left, ctx, columnsByAlias);
                CollectJoinAttributesByAlias(binary.Right, ctx, columnsByAlias);
                break;

            case UnaryExpression unary:
                CollectJoinAttributesByAlias(unary.Operand, ctx, columnsByAlias);
                break;

            case NewArrayExpression newArray:
                foreach (var element in newArray.Expressions)
                    CollectJoinAttributesByAlias(element, ctx, columnsByAlias);
                break;
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
                Operator = ConditionOperator.Null
            });
            return;
        }

        // General predicate
        var rootFilter = ctx.Query.Filter ??= new FetchFilter();
        TranslatePredicate(lambda.Body, rootFilter, ctx);

        // Collapse unnecessary nesting: if root has exactly one sub-filter and no
        // conditions or links, promote the sub-filter to root level.
        if (rootFilter.Conditions.Count == 0 && rootFilter.Links.Count == 0 && rootFilter.Filters.Count == 1)
        {
            var child = rootFilter.Filters[0];
            rootFilter.Type = child.Type;
            rootFilter.Filters.Clear();
            foreach (var c in child.Conditions)
                rootFilter.Conditions.Add(c);
            foreach (var f in child.Filters)
                rootFilter.Filters.Add(f);
            foreach (var l in child.Links)
                rootFilter.Links.Add(l);
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

            // x.MultiSelectProp.Contains(enumValue) → contain-values / negated → not-contain-values
            case MethodCallExpression { Method: { Name: "Contains" }, Object: { } obj } multiSelectCall
                when multiSelectCall.Arguments.Count == 1
                && ResolveAttribute(obj, ctx) is { } msResolved
                && obj.Type.IsGenericType
                && obj.Type != typeof(string):
            {
                var value = EvaluateValue(multiSelectCall.Arguments[0]);
                var condition = new FetchCondition
                {
                    Attribute = msResolved.Name,
                    EntityAlias = msResolved.EntityAlias,
                    Operator = negated ? ConditionOperator.DoesNotContainValues : ConditionOperator.ContainValues
                };
                condition.Values.Add(value!);
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

            // x.MultiSelectProp.Equals(value) or .Equals(collection) → eq/ne or in/not-in
            // C# resolves this to object.Equals(object) since instance methods shadow extension methods.
            // Also handles explicit MultiSelectExtensions.Equals(collection, values) calls.
            case MethodCallExpression { Method: { Name: "Equals" } } equalsCall
                when (equalsCall.Method.DeclaringType == typeof(Extensions.MultiSelectExtensions)
                    || (equalsCall.Method.DeclaringType == typeof(object)
                        && equalsCall.Object is { } eqObj
                        && eqObj.Type.IsGenericType
                        && eqObj.Type != typeof(string)
                        && ResolveAttribute(eqObj, ctx) is not null)):
            {
                // Extension method: args[0] = collection, args[1] = value
                // Instance method: Object = collection, args[0] = value
                var isExtension = equalsCall.Method.DeclaringType == typeof(Extensions.MultiSelectExtensions);
                var attrExpr = isExtension ? equalsCall.Arguments[0] : equalsCall.Object!;
                var valueExpr = isExtension ? equalsCall.Arguments[1] : equalsCall.Arguments[0];

                var resolved = ResolveAttribute(attrExpr, ctx)
                    ?? throw new NotSupportedException("Equals target must resolve to an attribute.");
                var value = EvaluateValue(valueExpr);

                if (value is System.Collections.IEnumerable enumerable and not string)
                {
                    var condition = new FetchCondition
                    {
                        Attribute = resolved.Name,
                        EntityAlias = resolved.EntityAlias,
                        Operator = negated ? ConditionOperator.NotIn : ConditionOperator.In
                    };
                    foreach (var item in enumerable)
                        condition.Values.Add(item!);
                    filter.Conditions.Add(condition);
                }
                else
                {
                    filter.Conditions.Add(new FetchCondition
                    {
                        Attribute = resolved.Name,
                        EntityAlias = resolved.EntityAlias,
                        Operator = negated ? ConditionOperator.NotEqual : ConditionOperator.Equal,
                        Value = value
                    });
                }
                return;
            }

            // Queryable.Any(source, predicate) → link-type="any" / negated → "not any"
            case MethodCallExpression { Method: { Name: "Any", DeclaringType: var anyDeclType } } anyCall
                when anyDeclType == typeof(Queryable) && anyCall.Arguments.Count == 2:
                HandleAnyPredicate(anyCall, filter, negated);
                return;

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

        // string.Length comparison → like / not-like with underscore patterns
        if (TryTranslateStringLength(binary, op, filter, ctx))
            return;

        // Determine which side is the attribute and which is the value
        var leftResolved = ResolveAttribute(binary.Left, ctx);
        var rightResolved = ResolveAttribute(binary.Right, ctx);

        // Both sides are attributes → column-to-column comparison (valueof)
        if (leftResolved is not null && rightResolved is not null)
        {
            var leftType = UnwrapExpressionType(binary.Left);
            var rightType = UnwrapExpressionType(binary.Right);
            if (leftType != rightType)
                throw new NotSupportedException(
                    $"Column-to-column comparison requires both sides to have the same type. " +
                    $"Left '{leftResolved.Value.Name}' is {leftType.Name}, " +
                    $"right '{rightResolved.Value.Name}' is {rightType.Name}.");

            var left = leftResolved.Value;
            var right = rightResolved.Value;
            var valueOfRef = right.EntityAlias is not null
                ? $"{right.EntityAlias}.{right.Name}"
                : right.Name;

            filter.Conditions.Add(new FetchCondition
            {
                Attribute = left.Name,
                EntityAlias = left.EntityAlias,
                Operator = op,
                ValueOf = valueOfRef
            });
            return;
        }

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

    /// <summary>
    /// Detects <c>x.StringProp.Length op value</c> patterns and translates them to
    /// FetchXml <c>like</c> / <c>not-like</c> with underscore patterns, since FetchXml
    /// has no native string length operator.
    /// </summary>
    private static bool TryTranslateStringLength(
        BinaryExpression binary, ConditionOperator op, FetchFilter filter, TranslationContext ctx)
    {
        // Identify which side is string.Length and which is the constant value
        Expression? lengthExpr = null;
        Expression? valueExpr = null;
        var reversed = false;

        if (IsStringLength(binary.Left))
        {
            lengthExpr = binary.Left;
            valueExpr = binary.Right;
        }
        else if (IsStringLength(binary.Right))
        {
            lengthExpr = binary.Right;
            valueExpr = binary.Left;
            reversed = true;
        }

        if (lengthExpr is null)
            return false;

        // Resolve the string attribute from the Length member's parent expression
        var memberExpr = (MemberExpression)lengthExpr;
        var resolved = ResolveAttribute(memberExpr.Expression!, ctx)
            ?? throw new NotSupportedException("Could not resolve attribute for string.Length comparison.");

        var lengthValue = Convert.ToInt32(EvaluateValue(valueExpr!));

        // When the constant is on the left (e.g. 5 >= x.Name.Length), flip the operator
        if (reversed)
        {
            op = op switch
            {
                ConditionOperator.GreaterThan => ConditionOperator.LessThan,
                ConditionOperator.GreaterEqual => ConditionOperator.LessEqual,
                ConditionOperator.LessThan => ConditionOperator.GreaterThan,
                ConditionOperator.LessEqual => ConditionOperator.GreaterEqual,
                _ => op
            };
        }

        // Build the underscore pattern and determine the like/not-like operator
        var pattern = new string('_', lengthValue);
        ConditionOperator condOp;

        if (op == ConditionOperator.Equal)
        {
            condOp = ConditionOperator.Like;
        }
        else if (op == ConditionOperator.NotEqual)
        {
            condOp = ConditionOperator.NotLike;
        }
        else if (op == ConditionOperator.GreaterThan)
        {
            pattern += "_%";
            condOp = ConditionOperator.Like;
        }
        else if (op == ConditionOperator.GreaterEqual)
        {
            pattern += "%";
            condOp = ConditionOperator.Like;
        }
        else if (op == ConditionOperator.LessThan)
        {
            pattern += "%";
            condOp = ConditionOperator.NotLike;
        }
        else if (op == ConditionOperator.LessEqual)
        {
            pattern += "_%";
            condOp = ConditionOperator.NotLike;
        }
        else
        {
            throw new NotSupportedException($"Unsupported operator for string.Length: {op}");
        }

        filter.Conditions.Add(new FetchCondition
        {
            Attribute = resolved.Name,
            EntityAlias = resolved.EntityAlias,
            Operator = condOp,
            Value = pattern
        });
        return true;
    }

    private static bool IsStringLength(Expression expr) =>
        expr is MemberExpression { Member: { Name: "Length", DeclaringType: var dt } }
        && dt == typeof(string);

    /// <summary>
    /// Unwraps Convert nodes and Nullable&lt;T&gt; to get the underlying CLR type of an expression.
    /// </summary>
    private static Type UnwrapExpressionType(Expression expr)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;
        var type = expr.Type;
        return Nullable.GetUnderlyingType(type) ?? type;
    }

    // -------------------------------------------------------------------------
    // Any() → link-type="any" / "not any"
    // -------------------------------------------------------------------------

    /// <summary>
    /// Translates a <c>Queryable.Any(source, predicate)</c> call into a
    /// <see cref="FetchLinkEntity"/> with <c>link-type="any"</c> (or <c>"not any"</c>
    /// when negated) nested inside the current filter.
    /// </summary>
    private static void HandleAnyPredicate(
        MethodCallExpression anyCall, FetchFilter filter, bool negated)
    {
        var (innerLogicalName, _) = GetSourceInfoFromType(anyCall.Arguments[0]);
        var lambda = ExtractLambda(anyCall.Arguments[1]);
        var innerParam = lambda.Parameters[0];

        // Flatten AndAlso chain and separate join vs filter conditions.
        var allConditions = FlattenAndAlso(lambda.Body);
        string? from = null, to = null;
        var filterConditions = new List<Expression>();

        foreach (var condition in allConditions)
        {
            if (from is null && TryExtractJoinCondition(condition, innerParam, out var f, out var t))
            {
                from = f;
                to = t;
            }
            else
            {
                filterConditions.Add(condition);
            }
        }

        if (from is null || to is null)
            throw new NotSupportedException(
                "Any() predicate must contain a join condition that compares an inner entity " +
                "attribute to an outer entity attribute (e.g. a.PrimaryContactId.Id == contact.ContactId).");

        var linkEntity = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = from,
            To = to,
            Alias = innerParam.Name!,
            LinkType = negated ? "not any" : "any"
        };

        // Translate remaining conditions as filters on the link-entity.
        if (filterConditions.Count > 0)
        {
            linkEntity.Filter = new FetchFilter { Type = FilterType.And };
            // Create a minimal context for the inner entity so that
            // attribute resolution works against the inner parameter.
            var innerQuery = new FetchXmlQuery { EntityLogicalName = innerLogicalName };
            var innerCtx = new TranslationContext(innerQuery, innerParam.Type);
            foreach (var fc in filterConditions)
                TranslatePredicate(fc, linkEntity.Filter, innerCtx);
        }

        filter.Links.Add(linkEntity);
    }

    /// <summary>
    /// Flattens a chain of <see cref="ExpressionType.AndAlso"/> nodes into a flat list.
    /// </summary>
    private static List<Expression> FlattenAndAlso(Expression expr)
    {
        var result = new List<Expression>();
        if (expr is BinaryExpression { NodeType: ExpressionType.AndAlso } binary)
        {
            result.AddRange(FlattenAndAlso(binary.Left));
            result.AddRange(FlattenAndAlso(binary.Right));
        }
        else
        {
            result.Add(expr);
        }
        return result;
    }

    /// <summary>
    /// Checks whether a binary Equal expression represents a join condition
    /// (one side references the inner parameter, the other references the outer).
    /// Returns the inner attribute as <paramref name="from"/> and the outer as <paramref name="to"/>.
    /// </summary>
    private static bool TryExtractJoinCondition(
        Expression condition, ParameterExpression innerParam,
        out string from, out string to)
    {
        from = null!;
        to = null!;

        if (condition is not BinaryExpression { NodeType: ExpressionType.Equal } binary)
            return false;

        var leftAttr = GetAttributeName(binary.Left);
        var rightAttr = GetAttributeName(binary.Right);
        if (leftAttr is null || rightAttr is null)
            return false;

        var leftRefsInner = ReferencesParameter(binary.Left, innerParam);
        var rightRefsInner = ReferencesParameter(binary.Right, innerParam);

        if (leftRefsInner && !rightRefsInner)
        {
            from = leftAttr;
            to = rightAttr;
            return true;
        }
        if (rightRefsInner && !leftRefsInner)
        {
            from = rightAttr;
            to = leftAttr;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="expr"/> contains a reference
    /// to the given <paramref name="param"/>.
    /// </summary>
    private static bool ReferencesParameter(Expression expr, ParameterExpression param)
    {
        return expr switch
        {
            _ when expr == param => true,
            UnaryExpression u => ReferencesParameter(u.Operand, param),
            MemberExpression m => m.Expression is not null && ReferencesParameter(m.Expression, param),
            MethodCallExpression mc =>
                (mc.Object is not null && ReferencesParameter(mc.Object, param))
                || mc.Arguments.Any(a => ReferencesParameter(a, param)),
            BinaryExpression b =>
                ReferencesParameter(b.Left, param) || ReferencesParameter(b.Right, param),
            _ => false
        };
    }

    /// <summary>
    /// Gets entity info from the source expression's generic type argument
    /// without requiring a runtime <see cref="DataverseQueryable{T}"/> instance.
    /// Used for <c>Any()</c> where the source may be captured in a closure.
    /// </summary>
    private static (string EntityLogicalName, Type EntityType) GetSourceInfoFromType(Expression sourceExpr)
    {
        // Try the existing runtime approach first (works for direct DataverseQueryable constants).
        if (sourceExpr is ConstantExpression { Value: { } val })
        {
            var type = val.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(DataverseQueryable<>))
            {
                var entityType = type.GetGenericArguments()[0];
                return (GetEntityLogicalName(entityType), entityType);
            }
        }

        // Fall back to the expression's declared type (e.g. IQueryable<T>).
        var exprType = sourceExpr.Type;
        if (exprType.IsGenericType)
        {
            var entityType = exprType.GetGenericArguments()[0];
            if (entityType.GetCustomAttribute<EntityLogicalNameAttribute>() is not null)
                return (GetEntityLogicalName(entityType), entityType);
        }

        throw new NotSupportedException(
            "Any() source must be a DataverseQueryable<T> or IQueryable<T> where T has [EntityLogicalName].");
    }

    private static bool IsNullConstant(Expression expr) =>
        expr is ConstantExpression { Value: null }
        || (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            && convert.Operand is ConstantExpression { Value: null });

    private static object? EvaluateValue(Expression expr)
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
                ? EvaluateValue(memberExpr.Expression)
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
            var items = newArray.Expressions.Select(e => EvaluateValueCore(e)).ToArray();
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
        var result = ResolveAttributeAccess(expr);
        if (result is not { } access)
            return null;

        // Walk up the member chain to find an entity name in joinMappings.
        // Simple join: ti.a.Name → entityExpression is ti.a
        // Chained join: ti2.ti1.a.Name → entityExpression is ti2.ti1.a
        // Direct parameter: o.Name → entityExpression is o (ParameterExpression)
        var current = access.EntityExpression;
        while (current is MemberExpression me)
        {
            if (joinMappings.TryGetValue(me.Member.Name, out var mapping))
                return new ResolvedAttribute(access.AttributeName, mapping.LinkAlias);
            current = me.Expression;
        }

        // Direct parameter access (e.g., (ti, o) => ... o.Name where o is a parameter)
        if (current is ParameterExpression param
            && joinMappings.TryGetValue(param.Name!, out var directMapping))
        {
            return new ResolvedAttribute(access.AttributeName, directMapping.LinkAlias);
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

        // Grouped aggregate ordering by g.Key — defer until Select assigns aliases
        if (ctx.IsGrouped && lambda.Body is MemberExpression { Member.Name: "Key" })
        {
            ctx.DeferredGroupOrders ??= [];
            ctx.DeferredGroupOrders.Add(descending);
            return;
        }

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
    // Terminal operators — First / FirstOrDefault / Single / SingleOrDefault
    // -------------------------------------------------------------------------

    private static void HandleTerminalOperator(MethodCallExpression call, TranslationContext ctx)
    {
        // Recurse into the source expression
        TranslateCore(call.Arguments[0], ctx);

        // If the overload includes a predicate (2 arguments), apply it as a Where filter
        if (call.Arguments.Count == 2)
        {
            var lambda = ExtractLambda(call.Arguments[1]);
            var filter = ctx.Query.Filter ??= new FetchFilter { Type = FilterType.And };
            TranslatePredicate(lambda.Body, filter, ctx);
        }

        ctx.Query.TerminalOperator = call.Method.Name switch
        {
            nameof(Queryable.First) => QueryTerminalOperator.First,
            nameof(Queryable.FirstOrDefault) => QueryTerminalOperator.FirstOrDefault,
            nameof(Queryable.Single) => QueryTerminalOperator.Single,
            nameof(Queryable.SingleOrDefault) => QueryTerminalOperator.SingleOrDefault,
            _ => throw new NotSupportedException($"Unsupported terminal operator '{call.Method.Name}'.")
        };

        // Single needs at least 2 rows to validate uniqueness; First only needs 1
        ctx.Query.Top = ctx.Query.TerminalOperator is QueryTerminalOperator.Single
                                                    or QueryTerminalOperator.SingleOrDefault ? 2 : 1;
    }

    // -------------------------------------------------------------------------
    // Aggregate operators — Min / Max / Sum / Average / Count / LongCount
    // -------------------------------------------------------------------------

    private static void HandleAggregateOperator(MethodCallExpression call, TranslationContext ctx)
    {
        var methodName = call.Method.Name;
        var isCount = methodName is nameof(Queryable.Count) or nameof(Queryable.LongCount);
        var isCountColumn = methodName is nameof(ServiceClientExtensions.CountColumn);

        ctx.Query.TerminalOperator = methodName switch
        {
            nameof(Queryable.Min) => QueryTerminalOperator.Min,
            nameof(Queryable.Max) => QueryTerminalOperator.Max,
            nameof(Queryable.Sum) => QueryTerminalOperator.Sum,
            nameof(Queryable.Average) => QueryTerminalOperator.Average,
            nameof(Queryable.Count) => QueryTerminalOperator.Count,
            nameof(Queryable.LongCount) => QueryTerminalOperator.LongCount,
            nameof(ServiceClientExtensions.CountColumn) => QueryTerminalOperator.CountColumn,
            _ => throw new NotSupportedException($"Unsupported aggregate method '{methodName}'.")
        };

        var fetchXmlFunction = AggregateFunctionMap[methodName];

        // Recurse into the source expression
        TranslateCore(call.Arguments[0], ctx);

        if (isCount)
        {
            // Count(predicate) — apply as Where filter
            if (call.Arguments.Count == 2)
            {
                var lambda = ExtractLambda(call.Arguments[1]);
                var filter = ctx.Query.Filter ??= new FetchFilter { Type = FilterType.And };
                TranslatePredicate(lambda.Body, filter, ctx);
            }

            // Count uses the entity's primary ID attribute
            ctx.Query.AllAttributes = false;
            ctx.Query.Attributes.Clear();
            ctx.Query.Attributes.Add(new FetchAttribute
            {
                Name = ctx.Query.EntityLogicalName + "id",
                Alias = fetchXmlFunction,
                Aggregate = fetchXmlFunction
            });
        }
        else if (call.Arguments.Count == 2)
        {
            // Min(selector), Max(selector), etc. — extract the attribute from the selector
            var lambda = ExtractLambda(call.Arguments[1]);
            var attrName = GetAttributeName(lambda.Body)
                ?? throw new NotSupportedException(
                    $"Could not resolve attribute for '{methodName}'.");

            ctx.Query.AllAttributes = false;
            ctx.Query.Attributes.Clear();
            ctx.Query.Attributes.Add(new FetchAttribute
            {
                Name = attrName,
                Alias = fetchXmlFunction,
                Aggregate = fetchXmlFunction
            });
        }
        else
        {
            // No-selector (e.g. .Select(a => a.Prop).Min())
            // The preceding Select should have populated Attributes with a single column.
            if (ctx.Query.Attributes.Count != 1)
                throw new NotSupportedException(
                    $"'{methodName}' without a selector requires exactly one column. " +
                    "Use a Select to project a single column, or use the overload with a selector.");

            var attr = ctx.Query.Attributes[0];
            attr.Alias = fetchXmlFunction;
            attr.Aggregate = fetchXmlFunction;
        }

        ctx.Query.Aggregate = true;
        ctx.Query.Projector = null;
        ctx.Query.ProjectionType = null;
    }

    // -------------------------------------------------------------------------
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

    private static void HandleGroupBy(MethodCallExpression call, TranslationContext ctx)
    {
        // Recurse into the source expression
        TranslateCore(call.Arguments[0], ctx);

        // Extract key selector
        var keySelector = ExtractLambda(call.Arguments[1]);

        ctx.IsGrouped = true;
        ctx.Query.Aggregate = true;
        ctx.Query.AllAttributes = false;
        ctx.Query.Attributes.Clear();

        // Constant key (e.g. GroupBy(a => 1)) — aggregate-only query, no groupby attribute
        if (keySelector.Body is ConstantExpression)
        {
            ctx.GroupKeyAttributeName = null;
            ctx.GroupKeyDateGrouping = null;
        }
        else
        {
            var (attrName, dateGrouping) = ResolveGroupKey(keySelector.Body, ctx);
            ctx.GroupKeyAttributeName = attrName;
            ctx.GroupKeyDateGrouping = dateGrouping;
        }

        // Clear join mappings — subsequent Select/OrderBy operate on IGrouping, not TI
        ctx.JoinMappings = null;
    }

    private static void HandleGroupedSelect(LambdaExpression lambda, TranslationContext ctx)
    {
        var body = lambda.Body;

        if (body is not NewExpression ne || ne.Members is null)
            throw new NotSupportedException("Grouped query must project into an anonymous type.");

        var entityParam = Expression.Parameter(typeof(Entity), "e");
        var extractMethod = typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!;
        var constructorArgs = new List<Expression>();
        string? groupKeyAlias = null;

        for (var i = 0; i < ne.Arguments.Count; i++)
        {
            var arg = ne.Arguments[i];
            var member = ne.Members[i];
            // Anonymous type members may be PropertyInfo or getter MethodInfo (get_Xxx)
            var memberName = member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                ? getter.Name[4..] : member.Name;
            var alias = memberName.ToLowerInvariant();
            var memberType = member is PropertyInfo pi ? pi.PropertyType
                : member is MethodInfo mi ? mi.ReturnType
                : throw new NotSupportedException($"Unexpected member type: {member.GetType().Name}");

            if (arg is MemberExpression { Member.Name: "Key" })
            {
                // Constant group key (e.g. GroupBy(a => 1)) — no groupby attribute needed,
                // inject the constant value directly into the projector
                if (ctx.GroupKeyAttributeName is null)
                {
                    constructorArgs.Add(Expression.Default(memberType));
                    continue;
                }

                // Group key
                groupKeyAlias = alias;
                ctx.Query.Attributes.Add(new FetchAttribute
                {
                    Name = ctx.GroupKeyAttributeName,
                    Alias = alias,
                    GroupBy = true,
                    DateGrouping = ctx.GroupKeyDateGrouping
                });
            }
            else if (arg is MethodCallExpression mc)
            {
                var (attrName, aggregateFunc) = ResolveGroupAggregate(mc, ctx);
                ctx.Query.Attributes.Add(new FetchAttribute
                {
                    Name = attrName,
                    Alias = alias,
                    Aggregate = aggregateFunc
                });
            }
            else
            {
                throw new NotSupportedException(
                    $"Unsupported expression in grouped projection at member '{ne.Members[i].Name}'.");
            }

            // Build projector argument
            constructorArgs.Add(
                Expression.Call(
                    extractMethod.MakeGenericMethod(memberType),
                    entityParam,
                    Expression.Constant(alias)));
        }

        // Add deferred orders (from OrderBy on g.Key processed before this Select)
        if (ctx.DeferredGroupOrders is not null && groupKeyAlias is not null)
        {
            foreach (var descending in ctx.DeferredGroupOrders)
            {
                ctx.Query.Orders.Add(new FetchOrder
                {
                    Alias = groupKeyAlias,
                    Descending = descending
                });
            }
        }

        // Compile projector: (Entity e) => new AnonType(ExtractValue<T>(e, "alias"), ...)
        var newExpr = Expression.New(ne.Constructor!, constructorArgs, ne.Members);
        ctx.Query.Projector = Expression.Lambda(newExpr, entityParam).Compile();
        ctx.Query.ProjectionType = ne.Type;
    }

    /// <summary>
    /// Resolves a group key expression to an attribute name and optional date grouping.
    /// Handles <c>o.Date.Value.Year</c>, <c>o.Date.Week()</c>, etc.
    /// </summary>
    private static (string AttrName, string? DateGrouping) ResolveGroupKey(
        Expression expr, TranslationContext ctx)
    {
        string? dateGrouping = null;

        // Standard DateTime properties: Year, Month, Day
        if (expr is MemberExpression { Member.Name: var propName } me
            && propName is "Year" or "Month" or "Day"
            && (me.Expression?.Type == typeof(DateTime) || me.Expression?.Type == typeof(DateTime?)))
        {
            dateGrouping = propName.ToLowerInvariant();
            expr = me.Expression;
        }
        // Extension method date grouping: Week(), Quarter(), FiscalPeriod(), FiscalYear()
        else if (expr is MethodCallExpression
                 {
                     Method: { Name: var methodName, DeclaringType: var dt }
                 } mc
                 && dt == typeof(DateTimeExtensions)
                 && methodName is "Week" or "Quarter" or "FiscalPeriod" or "FiscalYear")
        {
            dateGrouping = methodName switch
            {
                "Week" => "week",
                "Quarter" => "quarter",
                "FiscalPeriod" => "fiscal-period",
                "FiscalYear" => "fiscal-year",
                _ => throw new NotSupportedException()
            };
            expr = mc.Arguments[0]; // 'this' parameter of extension method
        }

        // Unwrap Nullable<T>.Value
        if (expr is MemberExpression { Member.Name: "Value" } valueAccess
            && valueAccess.Expression is not null
            && Nullable.GetUnderlyingType(valueAccess.Expression.Type) is not null)
        {
            expr = valueAccess.Expression;
        }

        // Resolve to attribute name
        var resolved = ResolveAttribute(expr, ctx)
            ?? throw new NotSupportedException(
                "Group key must resolve to an entity attribute.");

        return (resolved.Name, dateGrouping);
    }

    /// <summary>
    /// Resolves an aggregate method call within a grouped select (e.g. <c>g.Count()</c>,
    /// <c>g.Sum(x =&gt; x.Revenue)</c>) to an attribute name and FetchXml aggregate function.
    /// </summary>
    private static (string AttrName, string AggregateFunc) ResolveGroupAggregate(
        MethodCallExpression mc, TranslationContext ctx)
    {
        var methodName = mc.Method.Name;
        if (!AggregateFunctionMap.TryGetValue(methodName, out var aggregateFunc))
            throw new NotSupportedException($"Unsupported group aggregate '{methodName}'.");

        // Count() / LongCount() with no selector — use entity primary key
        if (methodName is "Count" or "LongCount" && mc.Arguments.Count == 1)
        {
            return (ctx.Query.EntityLogicalName + "id", aggregateFunc);
        }

        // Aggregate with selector — extract attribute from the lambda
        var selectorLambda = ExtractLambda(mc.Arguments[1]);
        var attrName = GetAttributeName(selectorLambda.Body)
            ?? throw new NotSupportedException(
                $"Could not resolve attribute for grouped {methodName}.");

        return (attrName, aggregateFunc);
    }

    // -------------------------------------------------------------------------
    // Inner join — Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    // -------------------------------------------------------------------------

    private static void HandleInnerJoin(MethodCallExpression call, TranslationContext ctx)
    {
        // Recurse into the outer source first — for chained joins this processes
        // the prior Join and populates JoinMappings before we handle this one.
        TranslateCore(call.Arguments[0], ctx);

        var (innerLogicalName, innerEntityType) = GetSourceInfoFromType(call.Arguments[1]);
        var resultLambda = ExtractLambda(call.Arguments[4]);

        // Chained join — the outer source is a prior join's transparent identifier
        if (ctx.JoinMappings is not null)
        {
            HandleChainedInnerJoin(call, innerLogicalName, innerEntityType, resultLambda, ctx);
            return;
        }

        var (outerLogicalName, outerEntityType) = GetSourceInfoFromType(call.Arguments[0]);
        var (outerKeyAttr, innerKeyAttr) = ExtractJoinKeys(call);

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
            ApplyColumns(ctx.Query, outerColumns);

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

    /// <summary>
    /// Handles a join whose outer source is the result of a prior join (transparent identifier).
    /// Resolves the outer key through the TI to determine which link entity to nest under,
    /// then extends JoinMappings with the new entity.
    /// </summary>
    private static void HandleChainedInnerJoin(
        MethodCallExpression call,
        string innerLogicalName,
        Type innerEntityType,
        LambdaExpression resultLambda,
        TranslationContext ctx)
    {
        var outerKeyLambda = ExtractLambda(call.Arguments[2]);
        var innerKeyLambda = ExtractLambda(call.Arguments[3]);

        var innerKeyAttr = GetAttributeName(innerKeyLambda.Body)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        // Resolve the outer key through the transparent identifier to find which
        // entity (and therefore which link) the key belongs to.
        var outerKeyResolved = ResolveChainedJoinKey(outerKeyLambda.Body, ctx.JoinMappings!)
            ?? throw new NotSupportedException(
                "Could not resolve outer join key through transparent identifier.");

        var link = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyResolved.Name,
            Alias = resultLambda.Parameters[1].Name!,
            LinkType = "inner"
        };

        // Nest under the parent link entity, or the root if the key belongs to the root entity
        if (outerKeyResolved.EntityAlias is null)
        {
            ctx.Query.Links.Add(link);
        }
        else
        {
            var parentLink = FindLinkByAlias(ctx.Query.Links, outerKeyResolved.EntityAlias)
                ?? throw new NotSupportedException(
                    $"Could not find parent link entity with alias '{outerKeyResolved.EntityAlias}'.");
            parentLink.Links.Add(link);
        }

        // Extend JoinMappings with the new entity.
        var updatedMappings = new Dictionary<string, JoinEntityInfo>(ctx.JoinMappings!);
        updatedMappings[resultLambda.Parameters[1].Name!] = new JoinEntityInfo
        {
            EntityType = innerEntityType,
            LinkAlias = link.Alias
        };

        if (IsTransparentIdentifier(resultLambda))
        {
            // Transparent identifier — further operators (Where/Select) follow.
            // Keep flattened mappings for downstream resolution.
            ctx.JoinMappings = updatedMappings;
            ctx.Query.InnerEntityType = innerEntityType;
        }
        else
        {
            // Final projection folded into the join result selector.
            // Extract columns and build the multi-join projector.
            ctx.JoinMappings = updatedMappings;
            ctx.Query.InnerEntityType = innerEntityType;
            HandleJoinSelect(resultLambda, ctx);
        }
    }

    /// <summary>
    /// Resolves an attribute access through a transparent identifier for chained join keys.
    /// E.g. <c>ti => ti.c.ContactId</c> resolves to attribute "contactid" on link alias "c".
    /// </summary>
    private static ResolvedAttribute? ResolveChainedJoinKey(
        Expression expr, Dictionary<string, JoinEntityInfo> joinMappings)
    {
        var access = ResolveAttributeAccess(expr);
        if (access is null)
            return null;

        // Walk up: the entity expression should be ti.memberName (member access on the TI parameter)
        var entityExpr = access.Value.EntityExpression;

        // May be nested: ti.memberName or ti.innerTI.memberName — walk to find the mapping key
        while (entityExpr is MemberExpression me)
        {
            if (me.Expression is ParameterExpression
                && joinMappings.TryGetValue(me.Member.Name, out var mapping))
            {
                return new ResolvedAttribute(access.Value.AttributeName, mapping.LinkAlias);
            }

            entityExpr = me.Expression;
        }

        return null;
    }

    /// <summary>
    /// Recursively searches link entities to find one with the given alias.
    /// </summary>
    private static FetchLinkEntity? FindLinkByAlias(List<FetchLinkEntity> links, string alias)
    {
        foreach (var link in links)
        {
            if (link.Alias == alias)
                return link;

            var nested = FindLinkByAlias(link.Links, alias);
            if (nested is not null)
                return nested;
        }

        return null;
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
        var (outerLogicalName, outerEntityType) = GetSourceInfoFromType(groupJoinCall.Arguments[0]);
        var (innerLogicalName, _) = GetSourceInfoFromType(groupJoinCall.Arguments[1]);
        var (outerKeyAttr, innerKeyAttr) = ExtractJoinKeys(groupJoinCall);

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
            ApplyColumns(ctx.Query, columns);

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
    // Shared join helpers
    // -------------------------------------------------------------------------

    private static (string OuterKey, string InnerKey) ExtractJoinKeys(MethodCallExpression joinCall)
    {
        var outerKey = GetAttributeName(ExtractLambda(joinCall.Arguments[2]).Body)
            ?? throw new NotSupportedException(
                "Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKey = GetAttributeName(ExtractLambda(joinCall.Arguments[3]).Body)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        return (outerKey, innerKey);
    }

    private static void ApplyColumns(FetchXmlQuery query, IReadOnlyList<string> columns)
    {
        query.AllAttributes = false;
        foreach (var col in columns)
            query.Attributes.Add(new FetchAttribute { Name = col });
    }

    // -------------------------------------------------------------------------
    // Column extraction
    // -------------------------------------------------------------------------

    /// <summary>
    /// Extracts the member-argument expressions from a projection body
    /// (<see cref="NewExpression"/>, <see cref="MemberInitExpression"/>, or single
    /// <see cref="MemberExpression"/>).
    /// </summary>
    private static IEnumerable<Expression> GetProjectionArguments(Expression body) => body switch
    {
        NewExpression ne => ne.Arguments,
        MemberInitExpression init => init.Bindings
            .OfType<MemberAssignment>().Select(b => b.Expression),
        MemberExpression me => [me],
        _ => []
    };

    private record RowAggregateInfo(string Alias);

    /// <summary>
    /// Scans a projection body for <see cref="ServiceClientExtensions.CountChildren"/> calls
    /// and returns the alias (lowercased member name) for each one found.
    /// </summary>
    private static IReadOnlyList<RowAggregateInfo>? ExtractRowAggregates(Expression body)
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

    /// <summary>
    /// Rewrites <see cref="ServiceClientExtensions.CountChildren"/> calls in a projection
    /// to <see cref="AggregateProjection.ExtractValue{T}"/> calls that read the aliased value
    /// from the entity at runtime.
    /// </summary>
    private sealed class CountChildrenRewriter(ParameterExpression entityParam) : ExpressionVisitor
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

    /// <summary>
    /// Extracts attribute logical names from a simple select body (anonymous type,
    /// member-init, or single property access).
    /// </summary>
    private static IReadOnlyList<string>? ExtractColumns(Expression body)
    {
        var columns = new List<string>();

        foreach (var arg in GetProjectionArguments(body))
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

        foreach (var arg in GetProjectionArguments(lambda.Body))
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

        foreach (var arg in GetProjectionArguments(lambda.Body))
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

        // Multi-entity join (3+): build a single-parameter (Entity) projector
        // that extracts all values from the flat entity with aliased attributes.
        if (ctx.JoinMappings!.Count > 2)
            return RebuildMultiJoinProjector(lambda, ctx);

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

    /// <summary>
    /// Builds a single-parameter <c>(Entity e) => new { ... }</c> projector for multi-entity
    /// joins (3+ entities). Reads root entity attributes directly and linked entity attributes
    /// via <c>AliasedValue</c> extraction, similar to aggregate projections.
    /// </summary>
    private static Delegate RebuildMultiJoinProjector(LambdaExpression lambda, TranslationContext ctx)
    {
        var entityParam = Expression.Parameter(typeof(Entity), "e");

        var rewriter = new MultiJoinProjectorRewriter(entityParam, ctx.JoinMappings!);
        var newBody = rewriter.Visit(lambda.Body);

        return Expression.Lambda(newBody, entityParam).Compile();
    }

    /// <summary>
    /// Rewrites property accesses on joined entities to extract values from a flat Entity.
    /// Root entity attributes: <c>e.GetAttributeValue&lt;T&gt;("attrName")</c>.
    /// Linked entity attributes: extracts from <c>AliasedValue</c> at <c>"alias.attrName"</c>.
    /// </summary>
    private sealed class MultiJoinProjectorRewriter(
        ParameterExpression entityParam,
        Dictionary<string, JoinEntityInfo> joinMappings) : ExpressionVisitor
    {
        private static readonly MethodInfo GetAttributeValueMethod =
            typeof(Entity).GetMethod(nameof(Entity.GetAttributeValue))!;

        protected override Expression VisitMember(MemberExpression node)
        {
            // Match: ti.entityName.Property or ti2.ti1.entityName.Property (chained TI)
            // Walk up the member chain to find an entity name in joinMappings.
            if (FindJoinMapping(node.Expression, out var mapping))
            {
                var attrName = (node.Member as PropertyInfo)
                    ?.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;

                if (attrName is not null)
                {
                    var resultType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;

                    if (mapping.LinkAlias is null)
                    {
                        return Expression.Call(entityParam,
                            GetAttributeValueMethod.MakeGenericMethod(resultType),
                            Expression.Constant(attrName));
                    }

                    var aliasedKey = $"{mapping.LinkAlias}.{attrName}";
                    return Expression.Call(
                        typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!
                            .MakeGenericMethod(resultType),
                        entityParam,
                        Expression.Constant(aliasedKey));
                }
            }

            // Match: *.NavigationProperty.Id/.Value
            if (node.Member.Name is "Id" or "Value"
                && node.Expression is MemberExpression parentAccess
                && FindJoinMapping(parentAccess.Expression, out var mapping2))
            {
                var parentProp = parentAccess.Member as PropertyInfo;
                var attrName = parentProp?.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;

                if (attrName is not null)
                {
                    var resultType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;

                    if (mapping2.LinkAlias is null)
                    {
                        var parentType = parentProp!.PropertyType;
                        var getParent = Expression.Call(entityParam,
                            GetAttributeValueMethod.MakeGenericMethod(parentType),
                            Expression.Constant(attrName));
                        return Expression.MakeMemberAccess(getParent, node.Member);
                    }

                    var aliasedKey = $"{mapping2.LinkAlias}.{attrName}";
                    return Expression.Call(
                        typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!
                            .MakeGenericMethod(resultType),
                        entityParam,
                        Expression.Constant(aliasedKey));
                }
            }

            return base.VisitMember(node);
        }

        private bool FindJoinMapping(Expression? expr, out JoinEntityInfo mapping)
        {
            mapping = default!;
            var current = expr;
            while (current is MemberExpression me)
            {
                if (joinMappings.TryGetValue(me.Member.Name, out mapping))
                    return true;
                current = me.Expression;
            }

            // Direct parameter access (e.g., o.Name where o is a join parameter)
            if (current is ParameterExpression param
                && joinMappings.TryGetValue(param.Name!, out mapping))
            {
                return true;
            }

            return false;
        }
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

    private static string GetEntityLogicalName(Type entityType) =>
        entityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
        ?? throw new InvalidOperationException(
            $"Type '{entityType.Name}' must be decorated with [EntityLogicalName].");

    private static LambdaExpression ExtractLambda(Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    /// <summary>
    /// Resolves an expression to an attribute name and the entity expression it's accessed on.
    /// Handles direct property access, EntityReference.Id, Money.Value, OptionSetValue.Value,
    /// and <see cref="Entity.GetAttributeValue{T}"/> calls with a string-constant argument.
    /// Used by both <see cref="GetAttributeName"/> (simple) and
    /// <see cref="ResolveJoinAttribute"/> (through transparent identifiers).
    /// </summary>
    private static (string AttributeName, Expression EntityExpression)? ResolveAttributeAccess(Expression expr)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        // Entity.GetAttributeValue<T>("name")
        if (expr is MethodCallExpression { Method.Name: nameof(Entity.GetAttributeValue) } getAttr
            && getAttr.Arguments.Count == 1
            && getAttr.Arguments[0] is ConstantExpression { Value: string constName }
            && getAttr.Object is not null)
        {
            return (constName, getAttr.Object);
        }

        if (expr is not MemberExpression memberExpr || memberExpr.Expression is null)
            return null;

        // Direct property with [AttributeLogicalName]
        if (memberExpr.Member is PropertyInfo directProp)
        {
            var attrName = directProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
            if (attrName is not null)
                return (attrName, memberExpr.Expression);
        }

        // Two-level: unwrap .Id (EntityReference) or .Value (Money/OptionSetValue)
        // to resolve the [AttributeLogicalName] on the parent property
        if (memberExpr.Expression is MemberExpression { Member: PropertyInfo parentProp, Expression: { } parentContainer })
        {
            var isUnwrap = memberExpr.Member.Name switch
            {
                "Id" => true,
                "Value" when memberExpr.Member.DeclaringType == typeof(Money)
                          || memberExpr.Member.DeclaringType == typeof(OptionSetValue) => true,
                _ => false
            };

            if (isUnwrap)
            {
                var attrName = parentProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (attrName is not null)
                    return (attrName, parentContainer);
            }
        }

        return null;
    }

    private static string? GetAttributeName(Expression expr) =>
        ResolveAttributeAccess(expr)?.AttributeName;

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

        /// <summary>
        /// After a GroupBy: indicates that subsequent Select/OrderBy operate on
        /// <c>IGrouping&lt;TKey, TElement&gt;</c> rather than entity types.
        /// </summary>
        public bool IsGrouped { get; set; }

        /// <summary>The Dataverse attribute name used as the group key.</summary>
        public string? GroupKeyAttributeName { get; set; }

        /// <summary>FetchXml date grouping value (year, month, day, week, quarter, fiscal-period, fiscal-year).</summary>
        public string? GroupKeyDateGrouping { get; set; }

        /// <summary>
        /// Orders requested on <c>g.Key</c> before the Select assigns aliases.
        /// Each entry is the <c>descending</c> flag.
        /// </summary>
        public List<bool>? DeferredGroupOrders { get; set; }
    }

    private sealed class JoinEntityInfo
    {
        public required Type EntityType { get; init; }
        public string? LinkAlias { get; init; }
    }
}

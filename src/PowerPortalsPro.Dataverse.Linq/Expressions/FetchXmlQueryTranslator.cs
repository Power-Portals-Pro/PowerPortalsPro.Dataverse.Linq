using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using PowerPortalsPro.Dataverse.Linq.Model;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Expressions;

/// <summary>
/// Translates a LINQ expression tree into a <see cref="FetchXmlQuery"/> model.
/// Replaces the individual ad-hoc parsers with a single, composable translator
/// that processes the expression chain recursively (source-first) and populates
/// the query model operator by operator.
/// </summary>
internal static class FetchXmlQueryTranslator
{
    private static readonly Dictionary<string, ConditionOperator> _dateTimeOperatorMap = new()
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

    private static readonly Dictionary<string, ConditionOperator> _hierarchyOperatorMap = new()
    {
        ["Above"] = ConditionOperator.Above,
        ["AboveOrEqual"] = ConditionOperator.AboveOrEqual,
        ["Under"] = ConditionOperator.Under,
        ["UnderOrEqual"] = ConditionOperator.UnderOrEqual,
        ["NotUnder"] = ConditionOperator.NotUnder,
        ["EqualUserOrUserHierarchy"] = ConditionOperator.EqualUserOrUserHierarchy,
        ["EqualUserOrUserHierarchyAndTeams"] = ConditionOperator.EqualUserOrUserHierarchyAndTeams,
    };

    private static readonly Dictionary<string, ConditionOperator> _userOperatorMap = new()
    {
        ["EqualUserId"] = ConditionOperator.EqualUserId,
        ["NotEqualUserId"] = ConditionOperator.NotEqualUserId,
        ["EqualBusinessId"] = ConditionOperator.EqualBusinessId,
        ["NotEqualBusinessId"] = ConditionOperator.NotEqualBusinessId,
    };

    private static readonly Dictionary<string, ConditionOperator> _multiSelectOperatorMap = new()
    {
        ["ContainsValues"] = ConditionOperator.ContainValues,
    };

    private static readonly Dictionary<string, string> _aggregateFunctionMap = new()
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
        if (declaringType == typeof(DateTimeExtensions))
            return _dateTimeOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(HierarchyExtensions))
            return _hierarchyOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(UserExtensions))
            return _userOperatorMap.TryGetValue(methodName, out op);
        if (declaringType == typeof(MultiSelectExtensions))
            return _multiSelectOperatorMap.TryGetValue(methodName, out op);
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
    /// <param name="entityLogicalName">
    /// The Dataverse entity logical name. If <c>null</c>, derived from the <typeparamref name="T"/> attribute.
    /// </param>
    /// <param name="service">
    /// Optional organization service used for metadata lookups during translation.
    /// </param>
    internal static FetchXmlQuery Translate<T>(
        Expression expression,
        IReadOnlyList<string>? defaultColumns = null,
        string? entityLogicalName = null,
        IOrganizationService? service = null) where T : Entity
    {
        var query = new FetchXmlQuery
        {
            EntityLogicalName = entityLogicalName ?? typeof(T).GetEntityLogicalName()
        };

        var ctx = new TranslationContext(query, typeof(T)) { Service = service };
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

            case MethodCallExpression { Method.DeclaringType: var sceType } sceCall
                when sceType == typeof(ServiceClientExtensions):
                switch (sceCall.Method.Name)
                {
                    case nameof(ServiceClientExtensions.WithPageSize):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.PageSize = (int)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    case nameof(ServiceClientExtensions.WithPage):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.Page = (int)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    case nameof(ServiceClientExtensions.WithAggregateLimit):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.AggregateLimit = (int)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    case nameof(ServiceClientExtensions.WithDatasource):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.Datasource = (FetchDatasource)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    case nameof(ServiceClientExtensions.WithLateMaterialize):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.LateMaterialize = true;
                        return;
                    case nameof(ServiceClientExtensions.WithNoLock):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.NoLock = true;
                        return;
                    case nameof(ServiceClientExtensions.WithQueryHints):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        var hintsArray = (NewArrayExpression)sceCall.Arguments[1];
                        ctx.Query.QueryHints = hintsArray.Expressions
                            .Select(e => (SqlQueryHint)((ConstantExpression)e).Value!)
                            .ToList();
                        return;
                    case nameof(ServiceClientExtensions.WithUseRawOrderBy):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.UseRawOrderBy = true;
                        return;
                    case nameof(ServiceClientExtensions.CountColumn):
                        HandleAggregateOperator(sceCall, ctx);
                        return;
                    case nameof(ServiceClientExtensions.WithFirstRow):
                        // Marker only — the join handler reads this via HasWithFirstRow().
                        // Just recurse into the source.
                        TranslateCore(sceCall.Arguments[0], ctx);
                        return;
                    case nameof(ServiceClientExtensions.ReturnRecordCount):
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.ReturnTotalRecordCount = true;
                        ctx.Query.OnRecordCount = (Action<RecordCountArguments>)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    case "ReturnRecordCountAsync":
                        TranslateCore(sceCall.Arguments[0], ctx);
                        ctx.Query.ReturnTotalRecordCount = true;
                        ctx.Query.OnRecordCountAsync = (Func<RecordCountArguments, Task>)((ConstantExpression)sceCall.Arguments[1]).Value!;
                        return;
                    default:
                        throw new NotSupportedException(
                            $"ServiceClientExtensions method '{sceCall.Method.Name}' is not supported.");
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
        var lambda = call.Arguments[1].ExtractLambda();

        if (ctx.IsGrouped)
        {
            HandleGroupedSelect(lambda, ctx);
            return;
        }

        if (ctx.OuterEntityPath is not null)
        {
            // After a single left join — resolve member accesses through transparent identifiers.
            // OuterEntityPath is cleared by HandleChainedLeftJoin, so this path is only
            // reached for single (non-chained) left joins.
            var outerColumns = lambda.ExtractColumnsViaPath(ctx.OuterEntityPath);
            IReadOnlyList<string>? innerColumns = ctx.InnerEntityProperty is not null
                ? lambda.ExtractInnerColumnsViaProperty(ctx.InnerEntityProperty)
                : null;
            var wholeInner = ctx.InnerEntityProperty is not null
                && lambda.ReferencesWholeInnerEntity(ctx.InnerEntityProperty);
            ApplyLeftJoinColumns(ctx.Query, outerColumns, innerColumns, wholeInner);

            var leftJoinResolver = CreateLeftJoinResolver(
                lambda.Parameters[0], ctx.OuterEntityPath!, ctx.OuterEntityType!,
                ctx.InnerEntityProperty, ctx.Query.Links[^1].Alias!,
                ctx.InnerEntityProperty is not null
                    ? lambda.Parameters[0].Type.GetProperty(ctx.InnerEntityProperty)?.PropertyType
                    : null);
            // Build materializer (new path)
            ctx.Query.Materializer = MaterializerBuilder.BuildJoinMaterializer(
                lambda,
                expr => leftJoinResolver(expr) is { } r ? (r.LinkAlias, r.EntityType) : null,
                ctx.PrimaryKeyResolver);
        }
        else if (ctx.JoinMappings is not null)
        {
            // After an inner join or chained left join with transparent identifier
            HandleJoinSelect(lambda, ctx);
        }
        else
        {
            // Simple select on root entity
            var columns = lambda.Body.ExtractColumns();
            if (columns is { Count: > 0 })
                ctx.Query.ApplyColumns(columns);

            // Check for CountChildren() calls in the projection
            var rowAggregates = lambda.Body.ExtractRowAggregates();
            if (rowAggregates is { Count: > 0 })
            {
                foreach (var ra in rowAggregates)
                    ctx.Query.Attributes.Add(new FetchAttribute
                    {
                        Name = $"{ctx.Query.EntityLogicalName}id",
                        Alias = ra.Alias,
                        RowAggregate = "CountChildren"
                    });

                // Rewrite the projector to extract aliased values for CountChildren() calls
                var rewriter = new CountChildrenRewriter(lambda.Parameters[0]);
                var rewritten = rewriter.Visit(lambda.Body);
                var compiled = Expression.Lambda(rewritten, lambda.Parameters).Compile();
                ctx.Query.Materializer = MaterializerBuilder.BuildSimpleMaterializer(
                    compiled, lambda.ReturnType, ctx.RootEntityType);
            }
            else
            {
                ctx.Query.Materializer = MaterializerBuilder.BuildSimpleMaterializer(
                    lambda.Compile(), lambda.ReturnType, ctx.RootEntityType);
            }
        }
    }

    private static void HandleJoinSelect(LambdaExpression lambda, TranslationContext ctx)
    {
        // Collect columns keyed by entity alias ("" = root entity)
        var columnsByAlias = new Dictionary<string, List<string>>();
        var wholeEntityAliases = new HashSet<string>();

        foreach (var arg in lambda.Body.GetProjectionArguments())
        {
            // Try attribute resolution first — if it succeeds, it's a property access
            var resolved = ResolveAttribute(arg, ctx);
            if (resolved is not null)
            {
                var key = resolved.Value.EntityAlias ?? string.Empty;
                if (!columnsByAlias.TryGetValue(key, out var list))
                {
                    list = [];
                    columnsByAlias[key] = list;
                }
                list.Add(resolved.Value.Name);
                continue;
            }

            // Check if it's a whole entity reference (not a property on it)
            var wholeEntity = ResolveWholeJoinEntity(arg, ctx.JoinMappings!);
            if (wholeEntity is not null)
            {
                wholeEntityAliases.Add(wholeEntity);
                continue;
            }

            // Walk into sub-expressions for nested projections
            CollectJoinAttributesByAlias(arg, ctx, columnsByAlias);
        }

        foreach (var (alias, columns) in columnsByAlias)
        {
            if (alias == string.Empty)
                ctx.Query.ApplyColumns(columns);
            else
            {
                var link = ctx.Query.Links.FindLinkByAlias(alias);
                if (link is not null)
                    foreach (var col in columns)
                        link.Attributes.Add(new FetchAttribute { Name = col });
            }
        }

        // Set all-attributes for whole entity references
        foreach (var alias in wholeEntityAliases)
        {
            if (alias == string.Empty)
            {
                // Root entity — keep all-attributes (don't call ApplyColumns)
            }
            else
            {
                var link = ctx.Query.Links.FindLinkByAlias(alias);
                if (link is not null)
                    link.AllAttributes = true;
            }
        }

        // Build materializer (new path)
        var joinResolver = CreateJoinResolver(ctx.JoinMappings!);
        ctx.Query.Materializer = MaterializerBuilder.BuildJoinMaterializer(
            lambda,
            expr => joinResolver(expr) is { } r ? (r.LinkAlias, r.EntityType) : null,
            ctx.PrimaryKeyResolver);
    }

    /// <summary>
    /// Returns the join entity alias if the expression is a whole entity reference
    /// (e.g. <c>ti.pa</c> in a join projection), or null if it's a property access.
    /// Returns "" for the root entity.
    /// </summary>
    private static string? ResolveWholeJoinEntity(
        Expression expr, Dictionary<string, JoinEntityInfo> joinMappings)
    {
        var current = expr;
        while (current is MemberExpression me)
        {
            if (joinMappings.TryGetValue(me.Member.Name, out var mapping))
                return mapping.LinkAlias ?? string.Empty;
            current = me.Expression;
        }

        if (current is ParameterExpression param
            && joinMappings.TryGetValue(param.Name!, out var directMapping))
            return directMapping.LinkAlias ?? string.Empty;

        return null;
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
            var key = resolved.Value.EntityAlias ?? string.Empty;
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
        var lambda = call.Arguments[1].ExtractLambda();

        // Left-join null filter: where c == null
        if (ctx.InnerEntityProperty is not null && lambda.IsNullCheck(ctx.InnerEntityProperty))
        {
            var link = ctx.Query.Links[^1];
            var filter = ctx.Query.Filter ??= new FetchFilter();
            filter.Conditions.Add(new FetchCondition
            {
                EntityAlias = link.Alias,
                Attribute = $"{link.Name}id",
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
                    subFilter.Conditions.Add(new FetchCondition { Attribute = resolved.Name, EntityAlias = resolved.EntityAlias, Operator = emptyOp, Value = string.Empty });
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
                    var value = multiSelectCall.Arguments[0].EvaluateValue();
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
                        var value = dateCall.Arguments[1].EvaluateValue();
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
                            condition.Values.Add(dateCall.Arguments[i].EvaluateValue()!);
                    }
                    filter.Conditions.Add(condition);
                    return;
                }

            // x.MultiSelectProp.Equals(value) or .Equals(collection) → eq/ne or in/not-in
            // C# resolves this to object.Equals(object) since instance methods shadow extension methods.
            // Also handles explicit MultiSelectExtensions.Equals(collection, values) calls.
            case MethodCallExpression { Method: { Name: "Equals" } } equalsCall
                when (equalsCall.Method.DeclaringType == typeof(MultiSelectExtensions)
                    || (equalsCall.Method.DeclaringType == typeof(object)
                        && equalsCall.Object is { } eqObj
                        && eqObj.Type.IsGenericType
                        && eqObj.Type != typeof(string)
                        && ResolveAttribute(eqObj, ctx) is not null)):
                {
                    // Extension method: args[0] = collection, args[1] = value
                    // Instance method: Object = collection, args[0] = value
                    var isExtension = equalsCall.Method.DeclaringType == typeof(MultiSelectExtensions);
                    var attrExpr = isExtension ? equalsCall.Arguments[0] : equalsCall.Object!;
                    var valueExpr = isExtension ? equalsCall.Arguments[1] : equalsCall.Arguments[0];

                    var resolved = ResolveAttribute(attrExpr, ctx)
                        ?? throw new NotSupportedException("Equals target must resolve to an attribute.");
                    var value = valueExpr.EvaluateValue();

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
                filter.Links.Add(BuildSubqueryLink(anyCall, negated ? "not any" : "any", negateConditions: false, ctx.PrimaryKeyResolver));
                return;

            // Queryable.All(source, predicate) → link-type="all" / negated → "not all"
            case MethodCallExpression { Method: { Name: "All", DeclaringType: var allDeclType } } allCall
                when allDeclType == typeof(Queryable) && allCall.Arguments.Count == 2:
                filter.Links.Add(BuildSubqueryLink(allCall, negated ? "not all" : "all", negateConditions: true, ctx.PrimaryKeyResolver));
                return;

            // ServiceClientExtensions.Exists(source, predicate) → link-type="exists"
            // When negated, falls back to link-type="not any" (inside filter) since
            // Dataverse does not support link-type="not exists".
            case MethodCallExpression { Method: { Name: "Exists", DeclaringType: var existsDeclType } } existsCall
                when existsDeclType == typeof(ServiceClientExtensions) && existsCall.Arguments.Count == 2:
                if (negated)
                    filter.Links.Add(BuildSubqueryLink(existsCall, "not any", negateConditions: false, ctx.PrimaryKeyResolver));
                else
                    HandleEntityLevelLinkPredicate(existsCall, ctx, "exists");
                return;

            // ServiceClientExtensions.In(source, predicate) → link-type="in"
            // When negated, falls back to link-type="not any" (inside filter) since
            // Dataverse does not support link-type="not in" for link-entities.
            case MethodCallExpression { Method: { Name: "In", DeclaringType: var inDeclType } } inCall
                when inDeclType == typeof(ServiceClientExtensions) && inCall.Arguments.Count == 2:
                if (negated)
                    filter.Links.Add(BuildSubqueryLink(inCall, "not any", negateConditions: false, ctx.PrimaryKeyResolver));
                else
                    HandleEntityLevelLinkPredicate(inCall, ctx, "in");
                return;

            // && → AND filter (flatten if parent is already AND)
            // When negated, DeMorgan: ¬(a ∧ b) = ¬a ∨ ¬b
            case BinaryExpression { NodeType: ExpressionType.AndAlso } andExpr:
                TranslateLogicalPredicate(andExpr, filter, negated ? FilterType.Or : FilterType.And, ctx, negated);
                return;

            // || → OR filter (flatten if parent is already OR)
            // When negated, DeMorgan: ¬(a ∨ b) = ¬a ∧ ¬b
            case BinaryExpression { NodeType: ExpressionType.OrElse } orExpr:
                TranslateLogicalPredicate(orExpr, filter, negated ? FilterType.And : FilterType.Or, ctx, negated);
                return;

            // Comparison operators (==, !=, <, <=, >, >=)
            case BinaryExpression binary:
                TranslateComparisonPredicate(binary, filter, ctx, negated);
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
        var value = call.Arguments[0].EvaluateValue();
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

        var collection = collectionExpr.EvaluateValue();
        if (collection is not System.Collections.IEnumerable enumerable)
            return null;

        var values = new List<object>();
        foreach (var item in enumerable)
            values.Add(item);

        return new InPredicateResult(resolved.Value, values);
    }

    private static void TranslateLogicalPredicate(
        BinaryExpression expr, FetchFilter filter, FilterType type, TranslationContext ctx, bool negated = false)
    {
        if (filter.Type == type)
        {
            // Same type as parent — flatten into parent
            TranslatePredicate(expr.Left, filter, ctx, negated);
            TranslatePredicate(expr.Right, filter, ctx, negated);
        }
        else
        {
            // Different type — create sub-filter
            var subFilter = new FetchFilter { Type = type };
            TranslatePredicate(expr.Left, subFilter, ctx, negated);
            TranslatePredicate(expr.Right, subFilter, ctx, negated);
            filter.Filters.Add(subFilter);
        }
    }

    private static void TranslateComparisonPredicate(
        BinaryExpression binary, FetchFilter filter, TranslationContext ctx, bool negated = false)
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

        if (negated)
        {
            op = op switch
            {
                ConditionOperator.Equal => ConditionOperator.NotEqual,
                ConditionOperator.NotEqual => ConditionOperator.Equal,
                ConditionOperator.LessThan => ConditionOperator.GreaterEqual,
                ConditionOperator.LessEqual => ConditionOperator.GreaterThan,
                ConditionOperator.GreaterThan => ConditionOperator.LessEqual,
                ConditionOperator.GreaterEqual => ConditionOperator.LessThan,
                _ => op
            };
        }

        // Left-join inner entity null check: ti.c == null (inside compound predicate)
        if (ctx.InnerEntityProperty is not null
            && op is ConditionOperator.Equal or ConditionOperator.NotEqual
            && TryTranslateInnerEntityNullCheck(binary, filter, ctx, op == ConditionOperator.Equal
                ? ConditionOperator.Null : ConditionOperator.NotNull))
            return;

        // string.Length comparison → like / not-like with underscore patterns
        if (TryTranslateStringLength(binary, op, filter, ctx))
            return;

        // Determine which side is the attribute and which is the value
        var leftResolved = ResolveAttribute(binary.Left, ctx);
        var rightResolved = ResolveAttribute(binary.Right, ctx);

        // Both sides are attributes → column-to-column comparison (valueof)
        if (leftResolved is not null && rightResolved is not null)
        {
            var leftType = binary.Left.UnwrapExpressionType();
            var rightType = binary.Right.UnwrapExpressionType();
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
        if (valueExpr.IsNullConstant())
        {
            filter.Conditions.Add(new FetchCondition
            {
                Attribute = attr.Name,
                EntityAlias = attr.EntityAlias,
                Operator = op == ConditionOperator.Equal ? ConditionOperator.Null : ConditionOperator.NotNull
            });
            return;
        }

        var value = valueExpr.EvaluateValue();
        filter.Conditions.Add(new FetchCondition
        {
            Attribute = attr.Name,
            EntityAlias = attr.EntityAlias,
            Operator = op,
            Value = value
        });
    }

    /// <summary>
    /// Detects <c>ti.innerEntity == null</c> patterns inside compound predicates
    /// and translates them to a null/not-null condition on the inner link entity's primary key.
    /// </summary>
    private static bool TryTranslateInnerEntityNullCheck(
        BinaryExpression binary, FetchFilter filter, TranslationContext ctx, ConditionOperator nullOp)
    {
        var (memberSide, constSide) = binary.Left is ConstantExpression
            ? (binary.Right, binary.Left)
            : (binary.Left, binary.Right);

        if (constSide is not ConstantExpression { Value: null })
            return false;

        if (memberSide is not MemberExpression { Member.Name: var name, Expression: ParameterExpression }
            || name != ctx.InnerEntityProperty)
            return false;

        var link = ctx.Query.Links[^1];
        filter.Conditions.Add(new FetchCondition
        {
            EntityAlias = link.Alias,
            Attribute = $"{link.Name}id",
            Operator = nullOp
        });
        return true;
    }

    private static bool TryTranslateStringLength(
        BinaryExpression binary, ConditionOperator op, FetchFilter filter, TranslationContext ctx)
    {
        // Identify which side is string.Length and which is the constant value
        Expression? lengthExpr = null;
        Expression? valueExpr = null;
        var reversed = false;

        if (binary.Left.IsStringLength())
        {
            lengthExpr = binary.Left;
            valueExpr = binary.Right;
        }
        else if (binary.Right.IsStringLength())
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

        var lengthValue = Convert.ToInt32(valueExpr!.EvaluateValue());

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
        var (condOp, suffix) = op switch
        {
            ConditionOperator.Equal => (ConditionOperator.Like, string.Empty),
            ConditionOperator.NotEqual => (ConditionOperator.NotLike, string.Empty),
            ConditionOperator.GreaterThan => (ConditionOperator.Like, "_%"),
            ConditionOperator.GreaterEqual => (ConditionOperator.Like, "%"),
            ConditionOperator.LessThan => (ConditionOperator.NotLike, "%"),
            ConditionOperator.LessEqual => (ConditionOperator.NotLike, "_%"),
            _ => throw new NotSupportedException($"Unsupported operator for string.Length: {op}")
        };
        var pattern = $"{(new string('_', lengthValue))}{suffix}";

        filter.Conditions.Add(new FetchCondition
        {
            Attribute = resolved.Name,
            EntityAlias = resolved.EntityAlias,
            Operator = condOp,
            Value = pattern
        });
        return true;
    }

    /// <summary>
    /// Translates a subquery call (e.g. <c>Exists</c> or <c>In</c>) into a
    /// <see cref="FetchLinkEntity"/> with the specified <paramref name="linkType"/>
    /// added as a direct child of the entity (not inside a filter).
    /// </summary>
    private static void HandleEntityLevelLinkPredicate(
        MethodCallExpression call, TranslationContext ctx, string linkType)
    {
        var linkEntity = BuildSubqueryLink(
            call, linkType,
            negateConditions: false, ctx.PrimaryKeyResolver);
        ctx.Query.Links.Add(linkEntity);
    }

    /// <summary>
    /// Shared builder for subquery link-entities (Any, All, Exists, In).
    /// Extracts inner entity info, splits conditions into join vs filter,
    /// creates the link entity, and translates filter conditions.
    /// </summary>
    private static FetchLinkEntity BuildSubqueryLink(
        MethodCallExpression call, string linkType,
        bool negateConditions, Func<string, string>? primaryKeyResolver)
    {
        var (innerLogicalName, _) = call.Arguments[0].GetSourceInfoFromType();
        var lambda = call.Arguments[1].ExtractLambda();
        var innerParam = lambda.Parameters[0];

        // Flatten AndAlso chain and separate join vs filter conditions.
        var allConditions = lambda.Body.FlattenAndAlso();
        string? from = null, to = null;
        var filterConditions = new List<Expression>();

        foreach (var condition in allConditions)
        {
            if (from is null && TryExtractJoinCondition(condition, innerParam, out var f, out var t, primaryKeyResolver))
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
                $"{call.Method.Name}() predicate must contain a join condition that compares an inner entity " +
                "attribute to an outer entity attribute (e.g. a.PrimaryContactId.Id == contact.ContactId).");

        var linkEntity = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = from,
            To = to,
            Alias = innerParam.Name!,
            LinkType = linkType
        };

        // Translate remaining conditions as filters on the link-entity.
        if (filterConditions.Count > 0)
        {
            linkEntity.Filter = new FetchFilter { Type = FilterType.And };
            var innerQuery = new FetchXmlQuery { EntityLogicalName = innerLogicalName };
            var innerCtx = new TranslationContext(innerQuery, innerParam.Type);
            foreach (var fc in filterConditions)
                TranslatePredicate(fc, linkEntity.Filter, innerCtx, negated: negateConditions);
        }

        return linkEntity;
    }

    /// <summary>
    /// Checks whether a binary Equal expression represents a join condition
    /// (one side references the inner parameter, the other references the outer).
    /// Returns the inner attribute as <paramref name="from"/> and the outer as <paramref name="to"/>.
    /// </summary>
    private static bool TryExtractJoinCondition(
        Expression condition, ParameterExpression innerParam,
        out string from, out string to,
        Func<string, string>? primaryKeyResolver = null)
    {
        from = null!;
        to = null!;

        if (condition is not BinaryExpression { NodeType: ExpressionType.Equal } binary)
            return false;

        var leftAttr = binary.Left.GetAttributeName(primaryKeyResolver);
        var rightAttr = binary.Right.GetAttributeName(primaryKeyResolver);
        if (leftAttr is null || rightAttr is null)
            return false;

        var leftRefsInner = binary.Left.ReferencesParameter(innerParam);
        var rightRefsInner = binary.Right.ReferencesParameter(innerParam);

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
            var joinResult = ResolveJoinAttribute(expr, ctx.JoinMappings, ctx.PrimaryKeyResolver);
            if (joinResult is not null)
                return joinResult;
        }

        // Left-join transparent identifier: resolve through OuterEntityPath / InnerEntityProperty
        if (ctx.OuterEntityPath is not null)
        {
            var leftResult = ResolveLeftJoinAttribute(expr, ctx);
            if (leftResult is not null)
                return leftResult;
        }

        // Simple (non-join) resolution via GetAttributeName (includes Entity.Id when resolver is available)
        var simple = expr.GetAttributeName(ctx.PrimaryKeyResolver);
        if (simple is not null)
            return new ResolvedAttribute(simple, null);

        return null;
    }

    /// <summary>
    /// Resolves an attribute access through a left-join transparent identifier.
    /// The outer entity (accessed via <see cref="TranslationContext.OuterEntityPath"/>)
    /// maps to the root entity (null alias). The inner entity (accessed via
    /// <see cref="TranslationContext.InnerEntityProperty"/>) maps to the last link entity.
    /// </summary>
    private static ResolvedAttribute? ResolveLeftJoinAttribute(Expression expr, TranslationContext ctx)
    {
        var access = expr.ResolveAttributeAccess(ctx.PrimaryKeyResolver);
        if (access is null)
            return null;

        var entityExpr = access.Value.EntityExpression;

        // Check if the access goes through the outer entity path (root entity)
        if (ctx.OuterEntityPath is not null && entityExpr.IsOuterEntityAccess(ctx.OuterEntityPath))
            return new ResolvedAttribute(access.Value.AttributeName, null);

        // Check if the access is on the inner entity (link entity)
        if (ctx.InnerEntityProperty is not null && entityExpr.IsInnerEntityAccess(ctx.InnerEntityProperty))
        {
            var linkAlias = ctx.Query.Links[^1].Alias;
            return new ResolvedAttribute(access.Value.AttributeName, linkAlias);
        }

        return null;
    }

    private static ResolvedAttribute? ResolveJoinAttribute(
        Expression expr, Dictionary<string, JoinEntityInfo> joinMappings,
        Func<string, string>? primaryKeyResolver = null)
    {
        var result = expr.ResolveAttributeAccess(primaryKeyResolver);
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
        var lambda = call.Arguments[1].ExtractLambda();
        var descending = call.Method.Name is nameof(Queryable.OrderByDescending)
                                           or nameof(Queryable.ThenByDescending);

        if (ctx.IsGrouped)
        {
            // Grouped ordering by g.Key — defer until Select assigns aliases
            if (lambda.Body is MemberExpression { Member.Name: "Key" })
            {
                ctx.DeferredGroupOrders ??= [];
                ctx.DeferredGroupOrders.Add(new DeferredGroupOrder(descending));
                return;
            }

            // Grouped ordering by aggregate — g.Count(), g.Max(x => x.Prop), etc.
            if (lambda.Body is MethodCallExpression mc && _aggregateFunctionMap.ContainsKey(mc.Method.Name))
            {
                ctx.DeferredGroupOrders ??= [];
                ctx.DeferredGroupOrders.Add(new DeferredGroupOrder(descending, mc));
                return;
            }
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
            var lambda = call.Arguments[1].ExtractLambda();
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

        var fetchXmlFunction = _aggregateFunctionMap[methodName];

        // Recurse into the source expression
        TranslateCore(call.Arguments[0], ctx);

        if (isCount)
        {
            // Count(predicate) — apply as Where filter
            if (call.Arguments.Count == 2)
            {
                var lambda = call.Arguments[1].ExtractLambda();
                var filter = ctx.Query.Filter ??= new FetchFilter { Type = FilterType.And };
                TranslatePredicate(lambda.Body, filter, ctx);
            }

            // Count uses the entity's primary ID attribute
            ctx.Query.AllAttributes = false;
            ctx.Query.Attributes.Clear();
            ctx.Query.Attributes.Add(new FetchAttribute
            {
                Name = $"{ctx.Query.EntityLogicalName}id",
                Alias = fetchXmlFunction,
                Aggregate = fetchXmlFunction
            });
        }
        else if (call.Arguments.Count == 2)
        {
            // Min(selector), Max(selector), etc. — extract the attribute from the selector
            var lambda = call.Arguments[1].ExtractLambda();
            var attrName = lambda.Body.GetAttributeName(ctx.PrimaryKeyResolver)
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
        ctx.Query.Materializer = null;
    }

    // -------------------------------------------------------------------------
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

    private static void HandleGroupBy(MethodCallExpression call, TranslationContext ctx)
    {
        // Recurse into the source expression
        TranslateCore(call.Arguments[0], ctx);

        // Extract key selector
        var keySelector = call.Arguments[1].ExtractLambda();

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
        else if (keySelector.Body is NewExpression compositeKey
                 && compositeKey.Members is not null)
        {
            // Composite key (e.g. group by new { Year = ip.Date.Value.Year, Participant = ip.Participant })
            ctx.GroupKeyProperties = [];
            for (var i = 0; i < compositeKey.Arguments.Count; i++)
            {
                var member = compositeKey.Members[i];
                var propName = member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                    ? getter.Name[4..] : member.Name;
                var (attrName, dateGrouping) = ResolveGroupKey(compositeKey.Arguments[i], ctx);
                var resolved = ResolveAttribute(compositeKey.Arguments[i], ctx);
                ctx.GroupKeyProperties.Add(new GroupKeyProperty(propName, attrName, dateGrouping, resolved?.EntityAlias));
            }
        }
        else
        {
            var (attrName, dateGrouping) = ResolveGroupKey(keySelector.Body, ctx);
            ctx.GroupKeyAttributeName = attrName;
            ctx.GroupKeyDateGrouping = dateGrouping;

            // Capture entity alias so the groupby attribute goes on the correct entity/link
            if (ctx.JoinMappings is not null)
            {
                var keyResolved = ResolveAttribute(keySelector.Body, ctx);
                ctx.GroupKeyEntityAlias = keyResolved?.EntityAlias;
            }
        }

        // Analyze element selector for join + GroupBy
        if (ctx.JoinMappings is not null && call.Arguments.Count > 2)
        {
            var elementSelector = call.Arguments[2].ExtractLambda();
            ctx.GroupElementMappings = AnalyzeGroupElement(elementSelector, ctx.JoinMappings);
        }

        // Clear join mappings — subsequent Select/OrderBy operate on IGrouping, not TI
        ctx.JoinMappings = null;
    }

    /// <summary>
    /// Analyzes a GroupBy element selector to build a mapping from element type
    /// members to join entity info, preserving entity-routing for aggregate resolution.
    /// </summary>
    private static Dictionary<string, JoinEntityInfo> AnalyzeGroupElement(
        LambdaExpression elementSelector, Dictionary<string, JoinEntityInfo> joinMappings)
    {
        var body = elementSelector.Body;
        var result = new Dictionary<string, JoinEntityInfo>();

        if (body is NewExpression ne && ne.Members is not null)
        {
            // Composite element: group new { account, contact, participant } by key
            for (var i = 0; i < ne.Arguments.Count; i++)
            {
                var member = ne.Members[i];
                var propName = member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                    ? getter.Name[4..] : member.Name;

                var mapping = ResolveElementToJoinEntity(ne.Arguments[i], joinMappings);
                if (mapping is not null)
                    result[propName] = mapping;
            }
        }
        else
        {
            // Simple element: group participant by key → element is a single entity
            var mapping = ResolveElementToJoinEntity(body, joinMappings);
            if (mapping is not null)
                result[string.Empty] = mapping;
        }

        return result;
    }

    private static JoinEntityInfo? ResolveElementToJoinEntity(
        Expression expr, Dictionary<string, JoinEntityInfo> joinMappings)
    {
        var current = expr;
        while (current is MemberExpression me)
        {
            if (me.Expression is ParameterExpression
                && joinMappings.TryGetValue(me.Member.Name, out var mapping))
                return mapping;
            current = me.Expression;
        }

        if (current is ParameterExpression param
            && joinMappings.TryGetValue(param.Name!, out var directMapping))
            return directMapping;

        return null;
    }

    private static void HandleGroupedSelect(LambdaExpression lambda, TranslationContext ctx)
    {
        var body = lambda.Body;

        // Normalize MemberInitExpression to work with the same loop as NewExpression.
        // Extract member names and argument expressions from either form.
        NewExpression ne;
        string[] memberNames;
        Type[] memberTypes;
        Expression[] argExpressions;

        if (body is MemberInitExpression init)
        {
            ne = init.NewExpression;
            var bindings = init.Bindings.OfType<MemberAssignment>().ToList();
            memberNames = bindings.Select(b => b.Member.Name).ToArray();
            memberTypes = bindings.Select(b => ((PropertyInfo)b.Member).PropertyType).ToArray();
            argExpressions = bindings.Select(b => b.Expression).ToArray();
        }
        else if (body is NewExpression newExpr)
        {
            ne = newExpr;
            argExpressions = ne.Arguments.ToArray();
            if (ne.Members is not null)
            {
                memberNames = ne.Members.Select(m => m is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter
                    ? getter.Name[4..] : m.Name).ToArray();
                memberTypes = ne.Members.Select(m => m is PropertyInfo pi ? pi.PropertyType
                    : m is MethodInfo mi ? mi.ReturnType
                    : throw new NotSupportedException($"Unexpected member type: {m.GetType().Name}")).ToArray();
            }
            else
            {
                var ctorParams = ne.Constructor!.GetParameters();
                memberNames = ctorParams.Select(p => p.Name!).ToArray();
                memberTypes = ctorParams.Select(p => p.ParameterType).ToArray();
            }
        }
        else
        {
            throw new NotSupportedException("Grouped query must project into a new expression or member initializer.");
        }

        var entityParam = Expression.Parameter(typeof(Entity), "e");
        var extractMethod = typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!;
        var extractArgs = new List<Expression>();
        string? groupKeyAlias = null;

        for (var i = 0; i < argExpressions.Length; i++)
        {
            var arg = argExpressions[i];
            // Unwrap Convert (e.g. (int)g.Count() when assigned to a typed property)
            if (arg is UnaryExpression { NodeType: ExpressionType.Convert } convert)
                arg = convert.Operand;
            var alias = memberNames[i].ToLowerInvariant();
            var memberType = memberTypes[i];

            if (arg is MemberExpression { Member.Name: "Key" })
            {
                // Constant group key (e.g. GroupBy(a => 1)) — no groupby attribute needed,
                // inject the constant value directly into the projector
                if (ctx.GroupKeyAttributeName is null && ctx.GroupKeyProperties is null)
                {
                    extractArgs.Add(Expression.Default(memberType));
                    continue;
                }

                // Scalar group key
                groupKeyAlias = alias;
                AddGroupAttribute(ctx, new FetchAttribute
                {
                    Name = ctx.GroupKeyAttributeName!,
                    Alias = alias,
                    GroupBy = true,
                    DateGrouping = ctx.GroupKeyDateGrouping
                }, ctx.GroupKeyEntityAlias);
            }
            else if (TryResolveCompositeKeyAccess(arg, ctx, out var keyProp))
            {
                // Composite key property: g.Key.PropertyName or g.Key.Participant.Id
                groupKeyAlias = alias;
                AddGroupAttribute(ctx, new FetchAttribute
                {
                    Name = keyProp.AttributeName,
                    Alias = alias,
                    GroupBy = true,
                    DateGrouping = keyProp.DateGrouping
                }, keyProp.EntityAlias);
            }
            else if (arg is MethodCallExpression mc && _aggregateFunctionMap.ContainsKey(mc.Method.Name))
            {
                var (attrName, aggregateFunc, entityAlias) = ResolveGroupAggregate(mc, ctx);
                AddGroupAttribute(ctx, new FetchAttribute
                {
                    Name = attrName,
                    Alias = alias,
                    Aggregate = aggregateFunc
                }, entityAlias);
            }
            else if (ContainsAggregate(argExpressions[i]))
            {
                // Expression wraps aggregates (e.g. CalculateValue(g.Count()))
                // — rewrite nested aggregates to ExtractValue calls
                var rewritten = RewriteNestedAggregates(argExpressions[i], alias, entityParam, extractMethod, ctx);
                extractArgs.Add(rewritten);
                continue;
            }
            else
            {
                // Purely computed expression (e.g. Guid.NewGuid(), constants)
                // — pass through to the projector as-is
                extractArgs.Add(argExpressions[i]);
                continue;
            }

            // Build projector argument that extracts from the entity result
            // (for key, composite key, and aggregate branches that didn't continue)
            extractArgs.Add(
                Expression.Call(
                    extractMethod.MakeGenericMethod(memberType),
                    entityParam,
                    Expression.Constant(alias)));
        }

        // Apply deferred orders (from OrderBy processed before this Select)
        if (ctx.DeferredGroupOrders is not null)
        {
            foreach (var order in ctx.DeferredGroupOrders)
            {
                if (order.IsKeyOrder && groupKeyAlias is not null)
                {
                    AddGroupOrder(ctx, new FetchOrder { Alias = groupKeyAlias, Descending = order.Descending },
                        ctx.GroupKeyEntityAlias);
                }
                else if (order.AggregateCall is not null)
                {
                    var result = ResolveAggregateOrderAlias(order.AggregateCall, argExpressions, memberNames, ctx);
                    if (result is not null)
                    {
                        AddGroupOrder(ctx, new FetchOrder { Alias = result.Value.Alias, Descending = order.Descending },
                            result.Value.EntityAlias);
                    }
                }
            }
        }

        // Compile projector
        Expression projectorBody;
        if (body is MemberInitExpression)
        {
            var bindings = memberNames.Select((name, i) =>
                Expression.Bind(ne.Type.GetProperty(name)!, extractArgs[i])).ToArray();
            projectorBody = Expression.MemberInit(Expression.New(ne.Constructor!), bindings);
        }
        else if (ne.Members is not null)
        {
            projectorBody = Expression.New(ne.Constructor!, extractArgs, ne.Members);
        }
        else
        {
            projectorBody = Expression.New(ne.Constructor!, extractArgs);
        }

        // Build materializer by walking the projectorBody and replacing ExtractValue calls
        // with parameter placeholders. Pass-through expressions (Guid.NewGuid(), constants, etc.)
        // are kept as-is inside the compiled projector.
        var aliasMap = new Dictionary<string, MaterializerBuilder.GroupedSlotInfo>();
        CollectExtractValueAliases(projectorBody, entityParam, aliasMap);
        ctx.Query.Materializer = MaterializerBuilder.BuildGroupedMaterializer(
            lambda, projectorBody, entityParam, aliasMap);
    }

    /// <summary>
    /// Walks an expression tree and collects all alias strings used in
    /// <c>AggregateProjection.ExtractValue&lt;T&gt;(entity, alias)</c> calls.
    /// </summary>
    private static void CollectExtractValueAliases(
        Expression expr, ParameterExpression entityParam,
        Dictionary<string, MaterializerBuilder.GroupedSlotInfo> aliasMap)
    {
        switch (expr)
        {
            case MethodCallExpression mc
                when mc.Method.IsGenericMethod
                && mc.Method.GetGenericMethodDefinition() == typeof(AggregateProjection)
                    .GetMethod(nameof(AggregateProjection.ExtractValue))!
                && mc.Arguments.Count == 2
                && mc.Arguments[0] == entityParam
                && mc.Arguments[1] is ConstantExpression { Value: string alias }:
                if (!aliasMap.ContainsKey(alias))
                {
                    aliasMap[alias] = new MaterializerBuilder.GroupedSlotInfo
                    {
                        Alias = alias,
                        ValueType = mc.Method.GetGenericArguments()[0],
                    };
                }
                break;

            case NewExpression ne:
                foreach (var arg in ne.Arguments)
                    CollectExtractValueAliases(arg, entityParam, aliasMap);
                break;

            case MemberInitExpression init:
                foreach (var binding in init.Bindings.OfType<MemberAssignment>())
                    CollectExtractValueAliases(binding.Expression, entityParam, aliasMap);
                break;

            case UnaryExpression unary:
                CollectExtractValueAliases(unary.Operand, entityParam, aliasMap);
                break;

            case BinaryExpression binary:
                CollectExtractValueAliases(binary.Left, entityParam, aliasMap);
                CollectExtractValueAliases(binary.Right, entityParam, aliasMap);
                break;

            case MethodCallExpression mc:
                if (mc.Object is not null)
                    CollectExtractValueAliases(mc.Object, entityParam, aliasMap);
                foreach (var arg in mc.Arguments)
                    CollectExtractValueAliases(arg, entityParam, aliasMap);
                break;

            case ConditionalExpression cond:
                CollectExtractValueAliases(cond.Test, entityParam, aliasMap);
                CollectExtractValueAliases(cond.IfTrue, entityParam, aliasMap);
                CollectExtractValueAliases(cond.IfFalse, entityParam, aliasMap);
                break;
        }
    }

    /// <summary>
    /// Tries to resolve a composite key property access from a grouped select argument.
    /// Handles <c>g.Key.PropertyName</c> and <c>g.Key.PropertyName.Id/.Value</c>
    /// (navigation property unwrap) by stripping the unwrap first, then matching
    /// the property name against <see cref="TranslationContext.GroupKeyProperties"/>.
    /// </summary>
    private static bool TryResolveCompositeKeyAccess(
        Expression arg, TranslationContext ctx, out GroupKeyProperty keyProp)
    {
        keyProp = default!;
        if (ctx.GroupKeyProperties is null)
            return false;

        // Strip .Id/.Value unwrap if present: g.Key.Prop.Id → g.Key.Prop
        if (arg is MemberExpression { Member.Name: "Id" or "Value", Expression: MemberExpression inner })
            arg = inner;

        // Match g.Key.PropertyName
        if (arg is MemberExpression keyAccess
            && keyAccess.Expression is MemberExpression { Member.Name: "Key" })
        {
            keyProp = ctx.GroupKeyProperties.FirstOrDefault(p => p.MemberName == keyAccess.Member.Name)
                ?? throw new NotSupportedException(
                    $"Group key property '{keyAccess.Member.Name}' not found in composite key.");
            return true;
        }

        return false;
    }

    /// <summary>
    /// Routes a FetchAttribute to the correct entity or link-entity based on alias.
    /// </summary>
    /// <summary>
    /// Matches a deferred aggregate order (e.g. <c>g.Count()</c>) to the alias assigned
    /// to the matching aggregate in the Select projection. Returns the alias and the
    /// entity alias where the attribute was placed.
    /// </summary>
    private static (string Alias, string? EntityAlias)? ResolveAggregateOrderAlias(
        MethodCallExpression orderCall, Expression[] selectArgs, string[] selectMemberNames, TranslationContext ctx)
    {
        var orderMethodName = orderCall.Method.Name;

        // Resolve the attribute from the order's selector (if any)
        string? orderAttrName = null;
        if (orderCall.Arguments.Count == 2)
        {
            var orderSelector = orderCall.Arguments[1].ExtractLambda();
            orderAttrName = orderSelector.Body.GetAttributeName(ctx.PrimaryKeyResolver);
        }

        for (var i = 0; i < selectArgs.Length; i++)
        {
            var selectArg = selectArgs[i];
            if (selectArg is UnaryExpression { NodeType: ExpressionType.Convert } conv)
                selectArg = conv.Operand;
            if (selectArg is not MethodCallExpression selectMc
                || selectMc.Method.Name != orderMethodName)
                continue;

            // For aggregates with selectors, verify the attribute matches
            if (orderAttrName is not null && selectMc.Arguments.Count == 2)
            {
                var selectSelector = selectMc.Arguments[1].ExtractLambda();
                var selectAttrName = selectSelector.Body.GetAttributeName(ctx.PrimaryKeyResolver);
                if (selectAttrName != orderAttrName)
                    continue;
            }

            var alias = selectMemberNames[i].ToLowerInvariant();
            var (_, _, entityAlias) = ResolveGroupAggregate(selectMc, ctx);
            return (alias, entityAlias);
        }

        return null;
    }

    /// <summary>
    /// Routes a FetchOrder to the correct entity or link-entity based on alias.
    /// </summary>
    /// <summary>
    /// Returns true if the expression contains any aggregate method calls
    /// (e.g. <c>CalculateValue(g.Count())</c> contains <c>Count</c>).
    /// </summary>
    private static bool ContainsAggregate(Expression expr)
    {
        return expr switch
        {
            UnaryExpression { Operand: var op } => ContainsAggregate(op),
            MethodCallExpression mc when _aggregateFunctionMap.ContainsKey(mc.Method.Name) => true,
            MethodCallExpression mc => mc.Arguments.Any(ContainsAggregate)
                || (mc.Object is not null && ContainsAggregate(mc.Object)),
            _ => false
        };
    }

    /// <summary>
    /// Rewrites an expression by replacing any nested aggregate method calls
    /// (e.g. <c>g.Count()</c>) with <c>ExtractValue</c> calls, and registering
    /// the aggregates as FetchXml attributes. Non-aggregate expressions are left as-is.
    /// </summary>
    private static Expression RewriteNestedAggregates(
        Expression expr, string baseAlias, ParameterExpression entityParam,
        MethodInfo extractMethod, TranslationContext ctx)
    {
        var rewriter = new AggregateSubExpressionRewriter(baseAlias, entityParam, extractMethod, ctx);
        return rewriter.Visit(expr);
    }

    private sealed class AggregateSubExpressionRewriter : ExpressionVisitor
    {
        private readonly string _baseAlias;
        private readonly ParameterExpression _entityParam;
        private readonly MethodInfo _extractMethod;
        private readonly TranslationContext _ctx;
        private int _counter;

        public AggregateSubExpressionRewriter(
            string baseAlias, ParameterExpression entityParam,
            MethodInfo extractMethod, TranslationContext ctx)
        {
            _baseAlias = baseAlias;
            _entityParam = entityParam;
            _extractMethod = extractMethod;
            _ctx = ctx;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (_aggregateFunctionMap.ContainsKey(node.Method.Name))
            {
                var subAlias = $"{_baseAlias}_agg{_counter++}";
                var (attrName, aggregateFunc, entityAlias) = ResolveGroupAggregate(node, _ctx);
                AddGroupAttribute(_ctx, new FetchAttribute
                {
                    Name = attrName,
                    Alias = subAlias,
                    Aggregate = aggregateFunc
                }, entityAlias);

                return Expression.Call(
                    _extractMethod.MakeGenericMethod(node.Type),
                    _entityParam,
                    Expression.Constant(subAlias));
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Convert
                && node.Operand is MethodCallExpression mc
                && _aggregateFunctionMap.ContainsKey(mc.Method.Name))
            {
                var inner = VisitMethodCall(mc);
                return inner.Type != node.Type
                    ? Expression.Convert(inner, node.Type)
                    : inner;
            }

            return base.VisitUnary(node);
        }
    }

    private static void AddGroupOrder(TranslationContext ctx, FetchOrder order, string? entityAlias)
    {
        if (entityAlias is null)
        {
            ctx.Query.Orders.Add(order);
        }
        else
        {
            var link = ctx.Query.Links.FindLinkByAlias(entityAlias);
            link?.Orders.Add(order);
        }
    }

    private static void AddGroupAttribute(TranslationContext ctx, FetchAttribute attr, string? entityAlias)
    {
        if (entityAlias is null)
        {
            ctx.Query.Attributes.Add(attr);
        }
        else
        {
            var link = ctx.Query.Links.FindLinkByAlias(entityAlias)
                ?? throw new NotSupportedException(
                    $"Link entity with alias '{entityAlias}' not found for grouped attribute.");
            link.Attributes.Add(attr);
        }
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
    /// <c>g.Sum(x =&gt; x.Revenue)</c>) to an attribute name, FetchXml aggregate function,
    /// and the entity alias where the attribute should be placed.
    /// </summary>
    private static (string AttrName, string AggregateFunc, string? EntityAlias) ResolveGroupAggregate(
        MethodCallExpression mc, TranslationContext ctx)
    {
        var methodName = mc.Method.Name;
        if (!_aggregateFunctionMap.TryGetValue(methodName, out var aggregateFunc))
            throw new NotSupportedException($"Unsupported group aggregate '{methodName}'.");

        // Count() / LongCount() with no selector — use element entity primary key
        if (methodName is "Count" or "LongCount" && mc.Arguments.Count == 1)
        {
            var elementInfo = GetPrimaryGroupElement(ctx);
            if (elementInfo is not null)
            {
                var entityName = elementInfo.EntityType.GetEntityLogicalName();
                return ($"{entityName}id", aggregateFunc, elementInfo.LinkAlias);
            }

            return ($"{ctx.Query.EntityLogicalName}id", aggregateFunc, null);
        }

        // Aggregate with selector — extract attribute from the lambda
        var selectorLambda = mc.Arguments[1].ExtractLambda();

        // Resolve through group element mappings for join + GroupBy
        if (ctx.GroupElementMappings is not null)
        {
            var resolved = ResolveGroupElementAttribute(selectorLambda, ctx.GroupElementMappings, ctx.PrimaryKeyResolver);
            if (resolved is not null)
                return (resolved.Value.Name, aggregateFunc, resolved.Value.EntityAlias);
        }

        var attrName = selectorLambda.Body.GetAttributeName(ctx.PrimaryKeyResolver)
            ?? throw new NotSupportedException(
                $"Could not resolve attribute for grouped {methodName}.");

        return (attrName, aggregateFunc, null);
    }

    /// <summary>
    /// Returns the primary group element entity info for Count() resolution.
    /// For simple elements (key ""), returns that entity directly.
    /// For composite elements, picks a link entity (or any entity).
    /// </summary>
    private static JoinEntityInfo? GetPrimaryGroupElement(TranslationContext ctx)
    {
        if (ctx.GroupElementMappings is null)
            return null;

        // Simple element (Form 1: group entity by key)
        if (ctx.GroupElementMappings.TryGetValue(string.Empty, out var direct))
            return direct;

        // Composite element: prefer link entities over root
        return ctx.GroupElementMappings.Values.FirstOrDefault(m => m.LinkAlias is not null)
            ?? ctx.GroupElementMappings.Values.FirstOrDefault();
    }

    /// <summary>
    /// Resolves an aggregate selector lambda through group element mappings to find
    /// the attribute name and entity alias.
    /// </summary>
    private static ResolvedAttribute? ResolveGroupElementAttribute(
        LambdaExpression selectorLambda, Dictionary<string, JoinEntityInfo> elementMappings,
        Func<string, string>? primaryKeyResolver = null)
    {
        var body = selectorLambda.Body;
        var param = selectorLambda.Parameters[0];

        var access = body.ResolveAttributeAccess(primaryKeyResolver);
        if (access is null)
            return null;

        var entityExpr = access.Value.EntityExpression;

        // Simple element (Form 1): ip => ip.attr — entityExpr is the parameter itself
        if (entityExpr == param && elementMappings.TryGetValue(string.Empty, out var directEntity))
            return new ResolvedAttribute(access.Value.AttributeName, directEntity.LinkAlias);

        // Composite element (Form 2): x => x.participant.attr — entityExpr is param.propName
        if (entityExpr is MemberExpression me && me.Expression == param
            && elementMappings.TryGetValue(me.Member.Name, out var propEntity))
            return new ResolvedAttribute(access.Value.AttributeName, propEntity.LinkAlias);

        return null;
    }

    // -------------------------------------------------------------------------
    // Inner join — Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Walks the inner source expression looking for a <c>WithFirstRow()</c> marker call.
    /// </summary>
    private static bool HasWithFirstRow(Expression innerSource)
    {
        var current = innerSource;
        while (current is MethodCallExpression mc)
        {
            if (mc.Method.Name == nameof(ServiceClientExtensions.WithFirstRow)
                && mc.Method.DeclaringType == typeof(ServiceClientExtensions))
                return true;
            current = mc.Arguments[0];
        }
        return false;
    }

    private static void HandleInnerJoin(MethodCallExpression call, TranslationContext ctx)
    {
        // Recurse into the outer source first — for chained joins this processes
        // the prior Join and populates JoinMappings before we handle this one.
        TranslateCore(call.Arguments[0], ctx);

        var (innerLogicalName, innerEntityType) = call.Arguments[1].GetSourceInfoFromType();
        var resultLambda = call.Arguments[4].ExtractLambda();

        // Chained join — the outer source is a prior join's transparent identifier
        if (ctx.JoinMappings is not null)
        {
            // Detect WithFirstRow() marker on the inner source
            var chainedLinkType = HasWithFirstRow(call.Arguments[1])
                ? "matchfirstrowusingcrossapply"
                : "inner";
            HandleChainedJoin(
                call.Arguments[2], call.Arguments[3],
                innerLogicalName, innerEntityType, resultLambda,
                chainedLinkType, ctx);
            return;
        }

        var (outerLogicalName, outerEntityType) = call.Arguments[0].GetSourceInfoFromType();
        var (outerKeyAttr, innerKeyAttr) = ExtractJoinKeys(call, ctx.PrimaryKeyResolver);

        // Detect WithFirstRow() marker on the inner source
        var linkType = HasWithFirstRow(call.Arguments[1])
            ? "matchfirstrowusingcrossapply"
            : "inner";

        // Root entity
        ctx.Query.EntityLogicalName = outerLogicalName;

        // Link entity — use the LINQ parameter name as the alias
        var link = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyAttr,
            Alias = resultLambda.Parameters[1].Name!,
            LinkType = linkType
        };
        ctx.Query.Links.Add(link);

        ctx.JoinMappings = new Dictionary<string, JoinEntityInfo>
        {
            [resultLambda.Parameters[0].Name!] = new() { EntityType = outerEntityType, LinkAlias = null },
            [resultLambda.Parameters[1].Name!] = new() { EntityType = innerEntityType, LinkAlias = link.Alias }
        };
        if (!resultLambda.IsTransparentIdentifier())
            HandleJoinSelect(resultLambda, ctx);
    }

    /// <summary>
    /// Handles a join whose outer source is the result of a prior join (transparent identifier).
    /// Resolves the outer key through the TI to determine which link entity to nest under,
    /// then extends JoinMappings with the new entity.
    /// Used for both chained inner joins and chained left joins.
    /// </summary>
    private static void HandleChainedJoin(
        Expression outerKeyArg,
        Expression innerKeyArg,
        string innerLogicalName,
        Type innerEntityType,
        LambdaExpression resultLambda,
        string linkType,
        TranslationContext ctx)
    {
        var outerKeyLambda = outerKeyArg.ExtractLambda();
        var innerKeyLambda = innerKeyArg.ExtractLambda();

        var innerKeyAttr = innerKeyLambda.Body.GetAttributeName(ctx.PrimaryKeyResolver)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        // Resolve the outer key through the transparent identifier to find which
        // entity (and therefore which link) the key belongs to.
        var outerKeyResolved = ResolveJoinAttribute(outerKeyLambda.Body, ctx.JoinMappings!, ctx.PrimaryKeyResolver)
            ?? throw new NotSupportedException(
                "Could not resolve outer join key through transparent identifier.");

        var link = new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyResolved.Name,
            Alias = resultLambda.Parameters[1].Name!,
            LinkType = linkType
        };

        // Nest under the parent link entity, or the root if the key belongs to the root entity
        if (outerKeyResolved.EntityAlias is null)
        {
            ctx.Query.Links.Add(link);
        }
        else
        {
            var parentLink = ctx.Query.Links.FindLinkByAlias(outerKeyResolved.EntityAlias)
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

        ctx.JoinMappings = updatedMappings;

        // Clear single-left-join context — chained joins use JoinMappings exclusively
        ctx.OuterEntityPath = null;
        ctx.OuterEntityType = null;
        ctx.InnerEntityProperty = null;

        if (!resultLambda.IsTransparentIdentifier())
            HandleJoinSelect(resultLambda, ctx);
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

        // Chained left join: outer source of the GroupJoin is another join result
        var outerSource = groupJoinCall.Arguments[0];
        if (outerSource is MethodCallExpression)
        {
            TranslateCore(outerSource, ctx);
            var (innerLogicalName2, innerEntityType2) = groupJoinCall.Arguments[1].GetSourceInfoFromType();
            var resultSelector2 = call.Arguments[2].ExtractLambda();
            HandleChainedJoin(
                groupJoinCall.Arguments[2], groupJoinCall.Arguments[3],
                innerLogicalName2, innerEntityType2, resultSelector2,
                "outer", ctx);
            return;
        }

        // --- First / single left join ---

        // Extract join keys from GroupJoin
        var (outerLogicalName, outerEntityType) = outerSource.GetSourceInfoFromType();
        var (innerLogicalName, innerEntityType) = groupJoinCall.Arguments[1].GetSourceInfoFromType();
        var (outerKeyAttr, innerKeyAttr) = ExtractJoinKeys(groupJoinCall, ctx.PrimaryKeyResolver);

        // Root entity
        ctx.Query.EntityLogicalName = outerLogicalName;

        // Analyse the SelectMany result selector to determine whether the C# compiler
        // folded the final Select into it (no subsequent Where/Select) or created a
        // transparent-identifier wrapper (further operators follow).
        var resultSelector = call.Arguments[2].ExtractLambda();

        // Link entity — use the LINQ parameter name as the alias
        ctx.Query.Links.Add(new FetchLinkEntity
        {
            Name = innerLogicalName,
            From = innerKeyAttr,
            To = outerKeyAttr,
            Alias = resultSelector.Parameters[1].Name!,
            LinkType = "outer"
        });
        var outerPath = resultSelector.Parameters[0].Type.FindOuterPropertyPath(outerEntityType);

        if (outerPath is null)
            return;

        // Try to extract columns directly from the result selector.
        // If the selector IS the final projection (select folded in), this succeeds.
        // If it's a transparent-identifier wrapper, no columns are found and we
        // set up context for subsequent Select/Where instead.
        var columns = resultSelector.ExtractColumnsViaPath(outerPath);
        var innerParam = resultSelector.Parameters[1];

        // A transparent-identifier wrapper (all arguments are parameters) means further
        // operators follow. A real projection (not a TI) means the select was folded in.
        var isFoldedProjection = !resultSelector.IsTransparentIdentifier();
        var referencesInner = isFoldedProjection && resultSelector.Body.GetProjectionArguments()
            .Any(a => ReferencesParameter(a, innerParam));

        if (isFoldedProjection)
        {
            // Select folded into SelectMany — handle projection here
            var foldedInnerColumns = referencesInner
                ? ExtractColumnsFromParameter(resultSelector.Body, innerParam)
                : null;
            ApplyLeftJoinColumns(ctx.Query, columns,
                foldedInnerColumns is { Count: > 0 } ? foldedInnerColumns : null,
                wholeInner: referencesInner && (foldedInnerColumns is null or { Count: 0 }));

            var foldedResolver = CreateFoldedLeftJoinResolver(
                resultSelector.Parameters[0], outerPath, outerEntityType,
                innerParam, ctx.Query.Links[^1].Alias!);
            // Build materializer (new path)
            ctx.Query.Materializer = MaterializerBuilder.BuildJoinMaterializer(
                resultSelector,
                expr => foldedResolver(expr) is { } r ? (r.LinkAlias, r.EntityType) : null,
                innerDirectParam: innerParam);
        }
        else
        {
            // Transparent-identifier wrapper — set up context for subsequent operators.
            // The outer path must be computed on the RESULT type (the TI), not the
            // parameter type, because subsequent lambdas receive the TI as their parameter.
            ctx.OuterEntityType = outerEntityType;
            ctx.OuterEntityPath = resultSelector.ReturnType.FindOuterPropertyPath(outerEntityType);

            // Identify the inner entity property in the TI for null-check detection.
            var tiInnerParam = resultSelector.Parameters[1];
            foreach (var prop in resultSelector.ReturnType.GetProperties())
            {
                if (prop.PropertyType == tiInnerParam.Type)
                {
                    ctx.InnerEntityProperty = prop.Name;
                    break;
                }
            }

            // Build JoinMappings so that subsequent chained joins can resolve
            // keys through the transparent identifier. The outer entity name comes
            // from the GroupJoin result selector's first parameter (e.g. "a" in
            // (a, contacts) => new { a, contacts }).
            var groupJoinResultSelector = groupJoinCall.Arguments[4].ExtractLambda();
            ctx.JoinMappings = new Dictionary<string, JoinEntityInfo>
            {
                [groupJoinResultSelector.Parameters[0].Name!] = new() { EntityType = outerEntityType, LinkAlias = null },
                [resultSelector.Parameters[1].Name!] = new() { EntityType = innerEntityType, LinkAlias = ctx.Query.Links[^1].Alias }
            };
        }
    }

    // -------------------------------------------------------------------------
    // Shared join helpers
    // -------------------------------------------------------------------------

    private static (string OuterKey, string InnerKey) ExtractJoinKeys(
        MethodCallExpression joinCall, Func<string, string>? primaryKeyResolver = null)
    {
        var outerKey = ResolveJoinKey(joinCall.Arguments[2], joinCall.Arguments[0], primaryKeyResolver)
            ?? throw new NotSupportedException(
                "Outer join key must be a property decorated with [AttributeLogicalName].");

        var innerKey = ResolveJoinKey(joinCall.Arguments[3], joinCall.Arguments[1], primaryKeyResolver)
            ?? throw new NotSupportedException(
                "Inner join key must be a property decorated with [AttributeLogicalName].");

        return (outerKey, innerKey);
    }

    /// <summary>
    /// Resolves a join key expression to an attribute name. Handles proxy entity properties,
    /// <c>GetAttributeValue</c> calls, and <c>Entity.Id</c> for both typed and unbound entities.
    /// </summary>
    private static string? ResolveJoinKey(
        Expression keyArg, Expression sourceExpr, Func<string, string>? primaryKeyResolver)
    {
        var keyExpr = keyArg.ExtractLambda().Body;

        // Standard resolution handles [AttributeLogicalName] properties, GetAttributeValue,
        // .Id/.Value unwrap, and Entity.Id on typed proxies
        var resolved = keyExpr.GetAttributeName(primaryKeyResolver);
        if (resolved is not null)
            return resolved;

        // Unbound Entity.Id — resolve via the source's entity logical name
        var unwrapped = keyExpr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            ? convert.Operand : keyExpr;

        if (unwrapped is MemberExpression { Member.Name: "Id", Expression: ParameterExpression { Type: var paramType } }
            && paramType == typeof(Entity)
            && sourceExpr is ConstantExpression { Value: DataverseQueryable<Entity> queryable })
        {
            return primaryKeyResolver is not null
                ? primaryKeyResolver(queryable.EntityLogicalName)
                : $"{queryable.EntityLogicalName}id";
        }

        return null;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="expr"/> or any nested projection argument
    /// references <paramref name="param"/> (directly or via property access).
    /// </summary>
    private static bool ReferencesParameter(Expression expr, ParameterExpression param)
    {
        if (expr is ParameterExpression p && p == param) return true;
        if (expr is MemberExpression me && me.Expression is ParameterExpression mp && mp == param) return true;
        if (expr is NewExpression or MemberInitExpression)
            return expr.GetProjectionArguments().Any(a => ReferencesParameter(a, param));
        return false;
    }

    /// <summary>
    /// Extracts attribute logical names from direct property accesses on a parameter
    /// (e.g. <c>d.FirstName</c> where <c>d</c> is the inner entity parameter).
    /// Recurses into nested projections.
    /// </summary>
    private static List<string> ExtractColumnsFromParameter(Expression body, ParameterExpression param)
    {
        var columns = new List<string>();
        foreach (var arg in body.GetProjectionArguments())
        {
            if (arg is MemberExpression { Member: PropertyInfo prop, Expression: ParameterExpression p }
                && p == param)
            {
                var name = prop.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (name is not null)
                    columns.Add(name);
            }
            else if (arg is NewExpression or MemberInitExpression)
            {
                columns.AddRange(ExtractColumnsFromParameter(arg, param));
            }
        }
        return columns;
    }

    /// <summary>
    /// Applies outer and inner column lists to the query and its last link entity.
    /// Shared between HandleSelect (left-join branch) and HandleSelectMany (folded branch).
    /// </summary>
    private static void ApplyLeftJoinColumns(
        FetchXmlQuery query,
        IReadOnlyList<string>? outerColumns,
        IReadOnlyList<string>? innerColumns,
        bool wholeInner)
    {
        if (outerColumns is { Count: > 0 })
            query.ApplyColumns(outerColumns);

        var link = query.Links[^1];
        if (wholeInner)
        {
            link.AllAttributes = true;
        }
        else if (innerColumns is { Count: > 0 })
        {
            foreach (var col in innerColumns)
                link.Attributes.Add(new FetchAttribute { Name = col });
        }
    }

    // -------------------------------------------------------------------------
    // Entity resolution helpers for materializer building
    // -------------------------------------------------------------------------

    /// <summary>
    /// Identifies which entity (root or linked) an expression refers to.
    /// <c>LinkAlias</c> is <c>null</c> for the root entity.
    /// </summary>
    private readonly record struct EntityResolution(string? LinkAlias, Type EntityType);

    /// <summary>
    /// Creates an entity resolver for inner-join scenarios using the JoinMappings dictionary.
    /// Walks the member chain to find a mapping key.
    /// </summary>
    private static Func<Expression, EntityResolution?> CreateJoinResolver(
        Dictionary<string, JoinEntityInfo> joinMappings)
    {
        return expr =>
        {
            var current = expr;
            while (current is MemberExpression me)
            {
                if (joinMappings.TryGetValue(me.Member.Name, out var mapping))
                    return new EntityResolution(mapping.LinkAlias, mapping.EntityType);
                current = me.Expression;
            }

            if (current is ParameterExpression param
                && joinMappings.TryGetValue(param.Name!, out var directMapping))
                return new EntityResolution(directMapping.LinkAlias, directMapping.EntityType);

            return null;
        };
    }

    /// <summary>
    /// Creates an entity resolver for left-join scenarios (TI path for outer, property for inner).
    /// </summary>
    private static Func<Expression, EntityResolution?> CreateLeftJoinResolver(
        ParameterExpression originalParam,
        string[] outerPath,
        Type outerEntityType,
        string? innerPropertyName,
        string innerAlias,
        Type? innerEntityType)
    {
        return expr =>
        {
            // Outer entity via TI path
            if (expr.IsOuterEntityAccess(outerPath, originalParam))
                return new EntityResolution(null, outerEntityType);

            // Inner entity via property: ti.innerProp
            if (innerPropertyName is not null
                && expr is MemberExpression { Member.Name: var name, Expression: ParameterExpression p }
                && p == originalParam
                && name == innerPropertyName)
                return new EntityResolution(innerAlias, innerEntityType ?? expr.Type);

            return null;
        };
    }

    /// <summary>
    /// Creates an entity resolver for folded left-join scenarios (TI path for outer,
    /// direct parameter for inner).
    /// </summary>
    private static Func<Expression, EntityResolution?> CreateFoldedLeftJoinResolver(
        ParameterExpression outerTiParam,
        string[] outerPath,
        Type outerEntityType,
        ParameterExpression innerParam,
        string innerAlias)
    {
        return expr =>
        {
            // Outer entity via TI path
            if (expr.IsOuterEntityAccess(outerPath, outerTiParam))
                return new EntityResolution(null, outerEntityType);

            // Inner entity via direct parameter
            if (expr is ParameterExpression p && p == innerParam)
                return new EntityResolution(innerAlias, innerParam.Type);

            return null;
        };
    }

    // -------------------------------------------------------------------------
    // Translation context — carries state across recursive operator processing
    // -------------------------------------------------------------------------

    private sealed class TranslationContext(FetchXmlQuery query, Type rootEntityType)
    {
        public FetchXmlQuery Query { get; } = query;
        public Type RootEntityType { get; } = rootEntityType;

        /// <summary>
        /// Optional service reference for resolving metadata (e.g., primary key attribute names
        /// when <see cref="Entity.Id"/> is used in expressions).
        /// </summary>
        public IOrganizationService? Service { get; init; }

        /// <summary>
        /// Returns a resolver function that maps entity logical names to their primary key
        /// attribute name via <see cref="EntityMetadataCache"/>. Returns null when no service
        /// is available (e.g., unit test scenarios).
        /// </summary>
        public Func<string, string>? PrimaryKeyResolver =>
            Service is not null
                ? entityLogicalName => EntityMetadataCache.GetPrimaryIdAttribute(Service, entityLogicalName)
                : null;

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
        public List<DeferredGroupOrder>? DeferredGroupOrders { get; set; }

        /// <summary>
        /// Entity alias for the group key attribute (null = root entity).
        /// Set when GroupBy follows a join so the groupby attribute can be placed
        /// on the correct entity/link-entity.
        /// </summary>
        public string? GroupKeyEntityAlias { get; set; }

        /// <summary>
        /// For composite group keys (<c>group by new { a.Id, b.Name }</c>):
        /// resolved info for each key property. Null for scalar keys.
        /// </summary>
        public List<GroupKeyProperty>? GroupKeyProperties { get; set; }

        /// <summary>
        /// After a join + GroupBy: maps element type members to entity info.
        /// Key <c>""</c> means the element is a single entity (Form 1).
        /// Otherwise, keys are property names on the composite element type (Form 2).
        /// </summary>
        public Dictionary<string, JoinEntityInfo>? GroupElementMappings { get; set; }
    }

    private sealed class JoinEntityInfo
    {
        public required Type EntityType { get; init; }
        public string? LinkAlias { get; init; }
    }

    private record GroupKeyProperty(string MemberName, string AttributeName, string? DateGrouping, string? EntityAlias);

    /// <summary>
    /// A deferred order in a grouped query. Key orders have <see cref="AggregateCall"/> as null;
    /// aggregate orders store the method call expression for matching against Select aliases.
    /// </summary>
    private record DeferredGroupOrder(bool Descending, MethodCallExpression? AggregateCall = null)
    {
        public bool IsKeyOrder => AggregateCall is null;
    }
}

using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Builds <see cref="MaterializerInfo"/> by walking a select expression,
/// replacing data-source sub-expressions (entity property accesses, whole entity
/// references, aggregate calls, key accesses) with <see cref="ParameterExpression"/>
/// placeholders, recording a <see cref="MaterializerSlot"/> for each placeholder,
/// and compiling the rewritten expression into a delegate.
/// </summary>
internal static class MaterializerBuilder
{
    /// <summary>
    /// Resolution callback: given an expression that potentially references an entity,
    /// returns the link alias (null = root) and entity CLR type, or null if not an entity ref.
    /// </summary>
    internal delegate (string? LinkAlias, Type EntityType)? EntityResolver(Expression expr);

    /// <summary>
    /// Builds a <see cref="MaterializerInfo"/> for inner-join and left-join selects.
    /// Walks the lambda body, replacing entity property accesses and whole entity
    /// references with parameter placeholders.
    /// </summary>
    internal static MaterializerInfo BuildJoinMaterializer(
        LambdaExpression lambda,
        EntityResolver resolveEntity,
        Func<string, string>? primaryKeyResolver = null,
        ParameterExpression? innerDirectParam = null)
    {
        var walker = new JoinMaterializerWalker(resolveEntity, primaryKeyResolver, innerDirectParam);
        var rewrittenBody = walker.Visit(lambda.Body);

        var slots = walker.Slots.ToArray();
        var parameters = walker.Parameters.ToArray();

        var projector = Expression.Lambda(rewrittenBody, parameters).Compile();
        return new MaterializerInfo
        {
            CompiledProjector = projector,
            ResultType = lambda.ReturnType,
            Slots = slots,
        };
    }

    /// <summary>
    /// Builds a <see cref="MaterializerInfo"/> for grouped (aggregate) selects.
    /// Uses a pre-built dictionary mapping alias names to their slot kinds.
    /// </summary>
    internal static MaterializerInfo BuildGroupedMaterializer(
        LambdaExpression originalLambda,
        Expression rewrittenBody,
        ParameterExpression entityParam,
        Dictionary<string, GroupedSlotInfo> aliasMap)
    {
        var walker = new GroupedMaterializerWalker(entityParam, aliasMap);
        var finalBody = walker.Visit(rewrittenBody);

        var slots = walker.Slots.ToArray();
        var parameters = walker.Parameters.ToArray();

        var projector = Expression.Lambda(finalBody, parameters).Compile();
        return new MaterializerInfo
        {
            CompiledProjector = projector,
            ResultType = originalLambda.ReturnType,
            Slots = slots,
        };
    }

    /// <summary>
    /// Builds a <see cref="MaterializerInfo"/> for simple (non-join, non-grouped) selects.
    /// The materializer has a single <see cref="SlotKind.TypedEntity"/> slot for the root entity.
    /// </summary>
    internal static MaterializerInfo BuildSimpleMaterializer(
        Delegate compiledProjector, Type resultType, Type entityType)
    {
        return new MaterializerInfo
        {
            CompiledProjector = compiledProjector,
            ResultType = resultType,
            Slots =
            [
                new MaterializerSlot
                {
                    Kind = SlotKind.TypedEntity,
                    ValueType = entityType,
                }
            ],
        };
    }

    /// <summary>
    /// Info about a grouped slot: its alias and whether it came from a key or aggregate.
    /// All grouped slots are <see cref="SlotKind.AliasedValue"/>.
    /// </summary>
    internal sealed class GroupedSlotInfo
    {
        public required string Alias { get; init; }
        public required Type ValueType { get; init; }
    }

    // =========================================================================
    // Join / left-join walker
    // =========================================================================

    private sealed class JoinMaterializerWalker : ExpressionVisitor
    {
        private readonly EntityResolver _resolveEntity;
        private readonly Func<string, string>? _primaryKeyResolver;
        private readonly ParameterExpression? _innerDirectParam;

        internal readonly List<MaterializerSlot> Slots = [];
        internal readonly List<ParameterExpression> Parameters = [];

        // Cache: maps the original expression string to its parameter placeholder
        // to avoid creating duplicate slots for the same entity/property.
        private readonly Dictionary<string, ParameterExpression> _cache = [];

        public JoinMaterializerWalker(
            EntityResolver resolveEntity,
            Func<string, string>? primaryKeyResolver,
            ParameterExpression? innerDirectParam)
        {
            _resolveEntity = resolveEntity;
            _primaryKeyResolver = primaryKeyResolver;
            _innerDirectParam = innerDirectParam;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // 1. Property access with [AttributeLogicalName]: entity.Property
            if (node.Expression is not null && _resolveEntity(node.Expression) is { } res)
            {
                var attrName = (node.Member as PropertyInfo)
                    ?.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;

                if (attrName is not null)
                {
                    var resultType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;
                    return CreatePropertySlot(res.LinkAlias, attrName, resultType, node);
                }
            }

            // 2. Two-level unwrap: entity.NavProp.Id/.Value
            if (node.Member.Name is "Id" or "Value"
                && node.Expression is MemberExpression parentAccess
                && parentAccess.Expression is not null
                && _resolveEntity(parentAccess.Expression) is { } res2)
            {
                var parentProp = parentAccess.Member as PropertyInfo;
                var attrName = parentProp?.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;

                if (attrName is not null)
                {
                    var resultType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;
                    return CreatePropertySlot(res2.LinkAlias, attrName, resultType, node);
                }
            }

            // 3. Entity.Id — resolve via primary key lookup
            if (node.Member.Name == "Id"
                && typeof(Entity).IsAssignableFrom(node.Member.DeclaringType)
                && node.Expression is not null
                && _resolveEntity(node.Expression) is { } idRes
                && _primaryKeyResolver is not null)
            {
                var entityLogicalName = node.Expression.Type
                    .GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName;

                if (entityLogicalName is not null)
                {
                    var primaryKey = _primaryKeyResolver(entityLogicalName);
                    var resultType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;
                    return CreatePropertySlot(idRes.LinkAlias, primaryKey, resultType, node);
                }
            }

            // 4. Whole entity reference (the node itself resolves to an entity)
            if (node.Expression is not null && _resolveEntity(node) is { } wholeRes)
            {
                var entityType = (node.Member as PropertyInfo)?.PropertyType ?? node.Type;
                return CreateEntitySlot(wholeRes.LinkAlias, entityType, node);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Handle Entity.GetAttributeValue<T>("name")
            if (node.Method.Name == nameof(Entity.GetAttributeValue)
                && node.Arguments.Count == 1
                && node.Arguments[0] is ConstantExpression { Value: string attrName }
                && node.Object is not null
                && _resolveEntity(node.Object) is { } res)
            {
                var resultType = node.Type;
                return CreatePropertySlot(res.LinkAlias, attrName, resultType, node);
            }

            // Handle GetAttributeValue<T>("name").Id / .Value (two-level unwrap via method)
            // This is already handled by VisitMember since it walks inside

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            // Direct inner parameter reference (folded left join): d => ... d ...
            if (_innerDirectParam is not null && node == _innerDirectParam)
            {
                var directRes = _resolveEntity(node);
                if (directRes is not null)
                    return CreateEntitySlot(directRes.Value.LinkAlias, node.Type, node);
            }

            // Whole entity as a direct parameter reference
            if (_resolveEntity(node) is { } paramRes)
            {
                return CreateEntitySlot(paramRes.LinkAlias, node.Type, node);
            }

            return base.VisitParameter(node);
        }

        private ParameterExpression CreatePropertySlot(
            string? linkAlias, string attrName, Type valueType, Expression originalExpr)
        {
            var cacheKey = linkAlias is null
                ? $"attr:{attrName}:{valueType.FullName}"
                : $"aliased:{linkAlias}.{attrName}:{valueType.FullName}";

            if (_cache.TryGetValue(cacheKey, out var existing))
                return existing;

            var param = Expression.Parameter(valueType, $"p{Slots.Count}");

            MaterializerSlot slot;
            if (linkAlias is null)
            {
                slot = new MaterializerSlot
                {
                    Kind = SlotKind.RootAttribute,
                    ValueType = valueType,
                    AttributeName = attrName,
                };
            }
            else
            {
                slot = new MaterializerSlot
                {
                    Kind = SlotKind.AliasedValue,
                    ValueType = valueType,
                    Alias = $"{linkAlias}.{attrName}",
                };
            }

            Slots.Add(slot);
            Parameters.Add(param);
            _cache[cacheKey] = param;
            return param;
        }

        private ParameterExpression CreateEntitySlot(
            string? linkAlias, Type entityType, Expression originalExpr)
        {
            var cacheKey = linkAlias is null
                ? $"entity:root:{entityType.FullName}"
                : $"entity:{linkAlias}:{entityType.FullName}";

            if (_cache.TryGetValue(cacheKey, out var existing))
                return existing;

            var param = Expression.Parameter(entityType, $"p{Slots.Count}");

            MaterializerSlot slot;
            if (linkAlias is null)
            {
                slot = new MaterializerSlot
                {
                    Kind = SlotKind.TypedEntity,
                    ValueType = entityType,
                };
            }
            else
            {
                slot = new MaterializerSlot
                {
                    Kind = SlotKind.LinkedEntity,
                    ValueType = entityType,
                    Alias = linkAlias,
                };
            }

            Slots.Add(slot);
            Parameters.Add(param);
            _cache[cacheKey] = param;
            return param;
        }
    }

    // =========================================================================
    // Grouped (aggregate) walker
    // =========================================================================

    /// <summary>
    /// Walks a rewritten grouped select body, replacing <c>ExtractValue&lt;T&gt;(entity, alias)</c>
    /// calls with parameter placeholders and recording AliasedValue slots.
    /// Also handles <c>Expression.Default(type)</c> nodes from constant group keys.
    /// </summary>
    private sealed class GroupedMaterializerWalker : ExpressionVisitor
    {
        private static readonly MethodInfo ExtractValueMethod =
            typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractValue))!;

        private readonly ParameterExpression _entityParam;
        private readonly Dictionary<string, GroupedSlotInfo> _aliasMap;

        internal readonly List<MaterializerSlot> Slots = [];
        internal readonly List<ParameterExpression> Parameters = [];
        private readonly Dictionary<string, ParameterExpression> _cache = [];

        public GroupedMaterializerWalker(
            ParameterExpression entityParam,
            Dictionary<string, GroupedSlotInfo> aliasMap)
        {
            _entityParam = entityParam;
            _aliasMap = aliasMap;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Match: AggregateProjection.ExtractValue<T>(entityParam, "alias")
            if (node.Method.IsGenericMethod
                && node.Method.GetGenericMethodDefinition() == ExtractValueMethod
                && node.Arguments.Count == 2
                && node.Arguments[0] == _entityParam
                && node.Arguments[1] is ConstantExpression { Value: string alias })
            {
                var valueType = node.Method.GetGenericArguments()[0];
                return GetOrCreateAliasedSlot(alias, valueType);
            }

            return base.VisitMethodCall(node);
        }

        private ParameterExpression GetOrCreateAliasedSlot(string alias, Type valueType)
        {
            var cacheKey = $"aliased:{alias}:{valueType.FullName}";
            if (_cache.TryGetValue(cacheKey, out var existing))
                return existing;

            var param = Expression.Parameter(valueType, $"p{Slots.Count}");
            Slots.Add(new MaterializerSlot
            {
                Kind = SlotKind.AliasedValue,
                ValueType = valueType,
                Alias = alias,
            });
            Parameters.Add(param);
            _cache[cacheKey] = param;
            return param;
        }
    }
}

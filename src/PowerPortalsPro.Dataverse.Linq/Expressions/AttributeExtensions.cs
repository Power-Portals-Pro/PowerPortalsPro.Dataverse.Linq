using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Expressions;

/// <summary>
/// Extension methods for resolving Dataverse attribute names and entity
/// metadata from LINQ expression trees and CLR types.
/// </summary>
internal static class AttributeExtensions
{
    /// <summary>
    /// Matches a <see cref="Entity.GetAttributeValue{T}"/> call and resolves its attribute-name
    /// argument. The argument may be a string literal or any expression that can be evaluated
    /// while the query is translated — a captured variable, a field or property, a method call,
    /// a concatenation — so long as it doesn't depend on the entity being queried.
    /// </summary>
    internal static bool IsGetAttributeValueCall(
        this Expression expr,
        [NotNullWhen(true)] out string? attributeName,
        [NotNullWhen(true)] out Expression? entityExpression)
    {
        attributeName = null;
        entityExpression = null;

        if (expr is not MethodCallExpression { Method.Name: nameof(Entity.GetAttributeValue) } call
            || call.Arguments.Count != 1
            || call.Object is null)
        {
            return false;
        }

        if (!call.Arguments[0].TryEvaluateName(out attributeName))
            return false;

        entityExpression = call.Object;
        return true;
    }

    /// <summary>
    /// Resolves an expression to an attribute name and the entity expression it's accessed on.
    /// Handles direct property access, EntityReference.Id, Money.Value, OptionSetValue.Value,
    /// and <see cref="Entity.GetAttributeValue{T}"/> calls whose attribute-name argument can be
    /// resolved at translation time (see <see cref="IsGetAttributeValueCall"/>).
    /// </summary>
    /// <param name="rootEntityLogicalName">
    /// Fallback logical name used to resolve <see cref="Entity.Id"/> when the entity expression's
    /// CLR type is the base <see cref="Entity"/> (e.g. an unbound <c>Queryable("logicalname")</c>
    /// query) and therefore has no <see cref="EntityLogicalNameAttribute"/> to read.
    /// </param>
    internal static (string AttributeName, Expression EntityExpression)? ResolveAttributeAccess(
        this Expression expr, Func<string, string>? primaryKeyResolver = null,
        string? rootEntityLogicalName = null)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        // Entity.GetAttributeValue<T>(name)
        if (expr.IsGetAttributeValueCall(out var attributeName, out var attributeEntity))
            return (attributeName, attributeEntity);

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
        var isUnwrap = memberExpr.Member.Name switch
        {
            "Id" => true,
            "Value" when memberExpr.Member.DeclaringType == typeof(Money)
                      || memberExpr.Member.DeclaringType == typeof(OptionSetValue)
                      || Nullable.GetUnderlyingType(memberExpr.Member.DeclaringType!) is not null => true,
            _ => false
        };

        if (isUnwrap)
        {
            // Parent is a property with [AttributeLogicalName]: entity.NavProp.Id
            if (memberExpr.Expression is MemberExpression { Member: PropertyInfo parentProp, Expression: { } parentContainer })
            {
                var attrName = parentProp.GetCustomAttribute<AttributeLogicalNameAttribute>()?.LogicalName;
                if (attrName is not null)
                    return (attrName, parentContainer);
            }

            // Parent is GetAttributeValue<T>(name): entity.GetAttributeValue<EntityReference>("attr").Id
            if (memberExpr.Expression.IsGetAttributeValueCall(out var unwrapAttrName, out var unwrapEntity))
                return (unwrapAttrName, unwrapEntity);
        }

        // Entity.Id — resolve via primary key lookup when the entity type is known
        if (memberExpr.Member.Name == "Id"
            && typeof(Entity).IsAssignableFrom(memberExpr.Member.DeclaringType)
            && primaryKeyResolver is not null)
        {
            var entityLogicalName = memberExpr.Expression.Type
                .GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
                ?? rootEntityLogicalName;

            if (entityLogicalName is not null)
            {
                var primaryKey = primaryKeyResolver(entityLogicalName);
                return (primaryKey, memberExpr.Expression);
            }
        }

        return null;
    }

    internal static string? GetAttributeName(this Expression expr) =>
        expr.ResolveAttributeAccess()?.AttributeName;

    internal static string? GetAttributeName(this Expression expr, Func<string, string>? primaryKeyResolver,
        string? rootEntityLogicalName = null) =>
        expr.ResolveAttributeAccess(primaryKeyResolver, rootEntityLogicalName)?.AttributeName;

    internal static string GetEntityLogicalName(this Type entityType) =>
        entityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName
        ?? throw new InvalidOperationException(
            $"Type '{entityType.Name}' must be decorated with [EntityLogicalName].");

    /// <summary>
    /// Gets entity info from the source expression's generic type argument
    /// without requiring a runtime <see cref="DataverseQueryable{T}"/> instance.
    /// Used for <c>Any()</c> where the source may be captured in a closure.
    /// </summary>
    internal static (string EntityLogicalName, Type EntityType) GetSourceInfoFromType(this Expression sourceExpr)
    {
        // Try the existing runtime approach first (works for direct DataverseQueryable constants).
        if (sourceExpr is ConstantExpression { Value: { } val })
        {
            var type = val.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(DataverseQueryable<>))
            {
                var entityType = type.GetGenericArguments()[0];

                // For unbound entities (Entity base class), read the logical name from the instance
                if (entityType == typeof(Entity))
                {
                    var logicalName = (string)type.GetProperty(
                        nameof(DataverseQueryable<Entity>.EntityLogicalName),
                        BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(val)!;
                    return (logicalName, entityType);
                }

                return (entityType.GetEntityLogicalName(), entityType);
            }
        }

        // Fall back to the expression's declared type (e.g. IQueryable<T>).
        var exprType = sourceExpr.Type;
        if (exprType.IsGenericType)
        {
            var entityType = exprType.GetGenericArguments()[0];
            if (entityType.GetCustomAttribute<EntityLogicalNameAttribute>() is not null)
                return (entityType.GetEntityLogicalName(), entityType);
        }

        throw new NotSupportedException(
            "Source must be a DataverseQueryable<T> or IQueryable<T> where T has [EntityLogicalName].");
    }
}

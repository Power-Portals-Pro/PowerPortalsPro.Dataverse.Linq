using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
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
    /// Resolves an expression to an attribute name and the entity expression it's accessed on.
    /// Handles direct property access, EntityReference.Id, Money.Value, OptionSetValue.Value,
    /// and <see cref="Entity.GetAttributeValue{T}"/> calls with a string-constant argument.
    /// </summary>
    internal static (string AttributeName, Expression EntityExpression)? ResolveAttributeAccess(
        this Expression expr, Func<string, string>? primaryKeyResolver = null)
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

            // Parent is GetAttributeValue<T>("name"): entity.GetAttributeValue<EntityReference>("attr").Id
            if (memberExpr.Expression is MethodCallExpression { Method.Name: nameof(Entity.GetAttributeValue) } getAttrUnwrap
                && getAttrUnwrap.Arguments.Count == 1
                && getAttrUnwrap.Arguments[0] is ConstantExpression { Value: string unwrapAttrName }
                && getAttrUnwrap.Object is not null)
            {
                return (unwrapAttrName, getAttrUnwrap.Object);
            }
        }

        // Entity.Id — resolve via primary key lookup when the entity type is known
        if (memberExpr.Member.Name == "Id"
            && typeof(Entity).IsAssignableFrom(memberExpr.Member.DeclaringType)
            && primaryKeyResolver is not null)
        {
            var entityLogicalName = memberExpr.Expression.Type
                .GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName;

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

    internal static string? GetAttributeName(this Expression expr, Func<string, string>? primaryKeyResolver) =>
        expr.ResolveAttributeAccess(primaryKeyResolver)?.AttributeName;

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

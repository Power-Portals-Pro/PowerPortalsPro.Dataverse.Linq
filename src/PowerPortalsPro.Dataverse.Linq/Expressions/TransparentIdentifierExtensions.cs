using PowerPortalsPro.Dataverse.Linq.Model;
using System.Linq.Expressions;

namespace PowerPortalsPro.Dataverse.Linq.Expressions;

/// <summary>
/// Extension methods for working with compiler-generated transparent identifiers
/// and link-entity lookup in join expression trees.
/// </summary>
internal static class TransparentIdentifierExtensions
{
    internal static bool IsTransparentIdentifier(this LambdaExpression lambda) =>
        lambda.Body is NewExpression ne
        && ne.Arguments.Count > 0
        && ne.Arguments.All(a => a is ParameterExpression);

    /// <summary>
    /// Recursively searches <paramref name="type"/> for a property whose type is (or is
    /// assignable from) <paramref name="outerEntityType"/>, descending into compiler-generated
    /// transparent-identifier types (<c>&lt;&gt;h__TransparentIdentifier</c>).
    /// Returns the property-name chain or <c>null</c> if not found.
    /// </summary>
    internal static string[]? FindOuterPropertyPath(this Type type, Type outerEntityType, int maxDepth = 5)
    {
        foreach (var prop in type.GetProperties())
        {
            if (outerEntityType.IsAssignableFrom(prop.PropertyType))
                return [prop.Name];

            if (maxDepth > 1 && prop.PropertyType.Name.StartsWith("<>"))
            {
                var nested = prop.PropertyType.FindOuterPropertyPath(outerEntityType, maxDepth - 1);
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
    internal static bool IsOuterEntityAccess(this Expression expr, ParameterExpression param, string[] path)
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
    /// Returns <c>true</c> when <paramref name="expr"/> accesses the outer entity through
    /// the transparent-identifier <paramref name="path"/>, regardless of which parameter
    /// instance is at the root. Used in Where/OrderBy resolution after a left join.
    /// </summary>
    internal static bool IsOuterEntityAccess(this Expression expr, string[] path)
    {
        for (var i = path.Length - 1; i >= 0; i--)
        {
            if (expr is not MemberExpression me || me.Member.Name != path[i])
                return false;
            expr = me.Expression!;
        }

        return expr is ParameterExpression;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="expr"/> accesses the inner (joined) entity
    /// via the transparent-identifier property named <paramref name="innerPropertyName"/>.
    /// </summary>
    internal static bool IsInnerEntityAccess(this Expression expr, string innerPropertyName)
    {
        return expr is MemberExpression { Member.Name: var name, Expression: ParameterExpression }
            && name == innerPropertyName;
    }

    /// <summary>
    /// Recursively searches link entities to find one with the given alias.
    /// </summary>
    internal static FetchLinkEntity? FindLinkByAlias(this List<FetchLinkEntity> links, string alias)
    {
        foreach (var link in links)
        {
            if (link.Alias == alias)
                return link;

            var nested = link.Links.FindLinkByAlias(alias);
            if (nested is not null)
                return nested;
        }

        return null;
    }

    /// <summary>
    /// Returns <c>true</c> when the lambda body is <c>param.property == null</c>
    /// where <paramref name="innerPropertyName"/> is the property holding the inner
    /// entity in the transparent-identifier type.
    /// </summary>
    internal static bool IsNullCheck(this LambdaExpression lambda, string innerPropertyName)
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
}

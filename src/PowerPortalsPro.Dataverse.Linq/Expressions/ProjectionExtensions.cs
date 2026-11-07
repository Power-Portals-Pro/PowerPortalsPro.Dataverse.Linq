using PowerPortalsPro.Dataverse.Linq.Model;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
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
    /// The root entity as a select on it sees it: the lambda's parameter, and nothing else.
    /// </summary>
    internal static MaterializerBuilder.EntityResolver RootEntityResolver(
        ParameterExpression parameter, string logicalName) =>
        expr => expr == parameter ? (null, parameter.Type, logicalName) : null;

    /// <summary>
    /// Extracts the columns a select reads on each entity it reaches, wherever they sit in its
    /// body: the whole body (<c>a =&gt; a.GetAttributeValue&lt;string&gt;("name")</c>,
    /// <c>a =&gt; a["name"]</c>, <c>a =&gt; a.Id</c>), an anonymous type or member-init, or nested
    /// in a method call, operator or conditional (<c>a.Website.ToUpper()</c>,
    /// <c>a.NumberOfEmployees + 1</c>). The same walk serves a select on the root entity and one
    /// after a join; <paramref name="resolveEntity"/> says which entity an expression is.
    /// </summary>
    /// <param name="lambda">The select's lambda.</param>
    /// <param name="resolveEntity">
    /// Identifies an expression that is an entity of the query (the root, or a link entity by its
    /// alias). The materializer resolves entities with the same function, so the columns requested
    /// are the ones it reads.
    /// </param>
    /// <param name="primaryKeyOf">Resolves an entity's primary key, for <see cref="Entity.Id"/>.</param>
    internal static ProjectedColumns ExtractProjectedColumns(
        this LambdaExpression lambda, MaterializerBuilder.EntityResolver resolveEntity,
        Func<string, string> primaryKeyOf)
    {
        var collector = new ProjectedColumnCollector(resolveEntity, primaryKeyOf);
        collector.Visit(lambda.Body);
        return collector.Result;
    }

    /// <summary>
    /// Returns <c>true</c> when a select is one attribute read and nothing else
    /// (<c>a =&gt; a.Name</c>, <c>a =&gt; (decimal?)a.CreditLimit.Value</c>, <c>a =&gt; a["name"]</c>),
    /// so its value is the column's value.
    /// </summary>
    internal static bool IsSingleAttributeRead(
        this LambdaExpression lambda, MaterializerBuilder.EntityResolver resolveEntity,
        Func<string, string> primaryKeyOf) =>
        new ProjectedColumnCollector(resolveEntity, primaryKeyOf).TryResolveAttribute(lambda.Body, out _, out _);

    /// <summary>
    /// Requests the columns a select reads: an entity it uses whole gets every column, one it reads
    /// columns of gets those, and one it doesn't touch gets none (the root's rows still come back,
    /// keyed by their primary key).
    /// </summary>
    internal static void ApplyProjectedColumns(this FetchXmlQuery query, ProjectedColumns projected)
    {
        if (!projected.WholeEntities.Contains(ProjectedColumns.Root))
        {
            if (projected.Columns.TryGetValue(ProjectedColumns.Root, out var rootColumns))
                query.ApplyColumns(rootColumns);
            else
                query.AllAttributes = false;
        }

        foreach (var (alias, columns) in projected.Columns)
        {
            if (alias == ProjectedColumns.Root || projected.WholeEntities.Contains(alias))
                continue;

            if (query.Links.FindLinkByAlias(alias) is { } link)
                foreach (var column in columns)
                    link.Attributes.Add(new FetchAttribute { Name = column });
        }

        foreach (var alias in projected.WholeEntities)
        {
            if (alias != ProjectedColumns.Root && query.Links.FindLinkByAlias(alias) is { } link)
                link.AllAttributes = true;
        }
    }

    /// <summary>
    /// Walks a select's body and records, for each entity it reaches, the columns it reads.
    /// Reaching an entity other than through a column (selecting it, passing it to a method,
    /// reading its <see cref="Entity.Attributes"/> or <see cref="Entity.FormattedValues"/>, naming
    /// a column only the row can supply) means the select needs it whole.
    /// </summary>
    private sealed class ProjectedColumnCollector(
        MaterializerBuilder.EntityResolver resolveEntity, Func<string, string> primaryKeyOf)
        : ExpressionVisitor
    {
        public ProjectedColumns Result { get; } = new();

        public override Expression? Visit(Expression? node)
        {
            if (node is null)
                return node;

            if (TryResolveAttribute(node, out var alias, out var column))
            {
                Result.Add(alias, column);
                return node;
            }

            if (ResolveEntity(node) is { } entity)
            {
                Result.WholeEntities.Add(ProjectedColumns.KeyOf(entity.LinkAlias));
                return node;
            }

            return base.Visit(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Every row returned carries its entity's logical name; reading it needs only the row.
            if (node.Member.Name == nameof(Entity.LogicalName)
                && node.Member.DeclaringType == typeof(Entity)
                && ResolveEntity(node.Expression) is { } entity)
            {
                RequireRow(entity);
                return node;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            // c == null / c != null asks whether a joined row matched, which needs only the row.
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
                && (IsNull(node.Right) ? ResolveEntity(node.Left)
                    : IsNull(node.Left) ? ResolveEntity(node.Right)
                    : null) is { } entity)
            {
                RequireRow(entity);
                return node;
            }

            return base.VisitBinary(node);
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

        /// <summary>
        /// Resolves a read of one column on an entity of the query: a property with
        /// [AttributeLogicalName] (and its <c>.Id</c> / <c>.Value</c>), <c>GetAttributeValue</c>,
        /// the indexer, or <see cref="Entity.Id"/> as the entity's primary key.
        /// </summary>
        internal bool TryResolveAttribute(
            Expression expr, out string? linkAlias, [NotNullWhen(true)] out string? column)
        {
            expr = Unwrap(expr)!;

            if (expr.IsEntityIndexerCall(out column, out var indexed) && ResolveEntity(indexed) is { } indexedEntity)
            {
                linkAlias = indexedEntity.LinkAlias;
                return true;
            }

            if (expr.ResolveAttributeAccess() is { } access && ResolveEntity(access.EntityExpression) is { } accessed)
            {
                (linkAlias, column) = (accessed.LinkAlias, access.AttributeName);
                return true;
            }

            if (expr is MemberExpression { Member: PropertyInfo { Name: nameof(Entity.Id) } idProperty, Expression: { } owner }
                && typeof(Entity).IsAssignableFrom(idProperty.DeclaringType)
                && ResolveEntity(owner) is { } keyed
                && PrimaryKeyOf(keyed) is { } primaryKey)
            {
                (linkAlias, column) = (keyed.LinkAlias, primaryKey);
                return true;
            }

            (linkAlias, column) = (null, null);
            return false;
        }

        // A joined row is there when its primary key is; the root's rows always are.
        private void RequireRow((string? LinkAlias, Type EntityType, string? LogicalName) entity)
        {
            if (entity.LinkAlias is null)
                return;

            if (PrimaryKeyOf(entity) is { } primaryKey)
                Result.Add(entity.LinkAlias, primaryKey);
            else
                Result.WholeEntities.Add(entity.LinkAlias);
        }

        private string? PrimaryKeyOf((string? LinkAlias, Type EntityType, string? LogicalName) entity) =>
            (entity.EntityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName ?? entity.LogicalName)
                is { } logicalName
                ? primaryKeyOf(logicalName)
                : null;

        private (string? LinkAlias, Type EntityType, string? LogicalName)? ResolveEntity(Expression? expr) =>
            Unwrap(expr) is { } unwrapped ? resolveEntity(unwrapped) : null;

        private static bool IsNull(Expression expr) => Unwrap(expr) is ConstantExpression { Value: null };

        private static Expression? Unwrap(Expression? expr)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs } convert)
                expr = convert.Operand;
            return expr;
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
/// The columns a select reads, per entity of the query, keyed by link alias (<see cref="Root"/>
/// for the root entity).
/// </summary>
internal sealed class ProjectedColumns
{
    /// <summary>The root entity's key; a link entity's is its alias.</summary>
    public const string Root = "";

    /// <summary>The columns read on each entity, in the order the select reads them.</summary>
    public Dictionary<string, List<string>> Columns { get; } = [];

    /// <summary>The entities the select needs whole, which get every column.</summary>
    public HashSet<string> WholeEntities { get; } = [];

    public static string KeyOf(string? linkAlias) => linkAlias ?? Root;

    public void Add(string? linkAlias, string column)
    {
        var key = KeyOf(linkAlias);
        if (!Columns.TryGetValue(key, out var columns))
            Columns[key] = columns = [];
        if (!columns.Contains(column))
            columns.Add(column);
    }
}

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

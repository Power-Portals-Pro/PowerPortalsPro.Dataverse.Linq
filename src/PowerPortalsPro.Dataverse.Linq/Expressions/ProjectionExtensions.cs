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
    /// The root entity as a select on it sees it: the lambda's parameter, and nothing else. The
    /// parameter must be the root entity's type, not the result of an earlier projection.
    /// </summary>
    internal static MaterializerBuilder.EntityResolver RootEntityResolver(
        ParameterExpression parameter, string logicalName) =>
        expr => expr == parameter ? new MaterializerBuilder.ResolvedEntity(null, parameter.Type, logicalName) : null;

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
        Func<string, string> primaryKeyOf) =>
        new ProjectedColumnCollector(resolveEntity, primaryKeyOf).Collect(lambda.Body);

    /// <summary>
    /// Resolves a read of one column on an entity <paramref name="resolveEntity"/> finds, under any
    /// casts around it: a property with [AttributeLogicalName] (and its <c>.Id</c> /
    /// <c>.Value</c>), <c>GetAttributeValue</c>, the indexer, or <see cref="Entity.Id"/> as the
    /// entity's primary key. A select's columns and an operator's read through it (see
    /// FetchXmlQueryTranslator.ReadThroughSelect) resolve alike.
    /// </summary>
    internal static bool TryResolveColumn(
        this Expression expr, MaterializerBuilder.EntityResolver resolveEntity, Func<string, string> primaryKeyOf,
        out MaterializerBuilder.ResolvedEntity entity, [NotNullWhen(true)] out string? column)
    {
        expr = expr.UnwrapConvert();

        // Entity.Id is the primary key of the entity it's read on, which for an unbound entity
        // only that entity's resolution names; nothing else needs the key looked up.
        var owner = expr is MemberExpression { Member.Name: nameof(Entity.Id), Expression: { } idOwner }
            ? resolveEntity(idOwner.UnwrapConvert())
            : null;

        if (expr.ResolveAttributeAccess(owner is null ? null : primaryKeyOf, LogicalNameOf(owner)) is { } access
            && resolveEntity(access.EntityExpression.UnwrapConvert()) is { } accessed)
        {
            (entity, column) = (accessed, access.AttributeName);
            return true;
        }

        (entity, column) = (default, null);
        return false;
    }

    /// <summary>
    /// The logical name of an entity the query resolved: its type's, or for an unbound entity the
    /// one its resolution names.
    /// </summary>
    private static string? LogicalNameOf(MaterializerBuilder.ResolvedEntity? entity) =>
        entity is { } resolved
            ? resolved.EntityType.GetCustomAttribute<EntityLogicalNameAttribute>()?.LogicalName ?? resolved.LogicalName
            : null;

    /// <summary>
    /// Requests the columns a select reads: an entity it uses whole gets every column, one it reads
    /// columns of gets those, and one it doesn't touch gets none (the root's rows still come back,
    /// keyed by their primary key). A column already requested, by a projection this one is
    /// composed on, is not requested twice.
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
                AddColumns(link.Attributes, columns);
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
        private readonly ProjectedColumns _result = new();
        private readonly ProjectedColumns _unchanged = new();
        private Expression? _body;

        // Whether the node being visited is reached from the body through nothing but anonymous
        // types, member-init bindings, array initializers and casts that keep the value: it's then
        // in the result as it is. A constructor (new Initial(a.Name)) may change what it's given.
        private bool _inResult = true;

        public ProjectedColumns Collect(Expression body)
        {
            _body = body;
            Visit(body);

            // Every column read is also in the result unchanged, and so is every entity used whole.
            // A select that reads no column (a constant) gets one row per record from Dataverse.
            _result.PreservesColumns = (_result.Columns.Count > 0 || _result.WholeEntities.Count > 0)
                && _result.WholeEntities.All(_unchanged.WholeEntities.Contains)
                && _result.Columns.All(entry => _result.WholeEntities.Contains(entry.Key)
                    || _unchanged.Columns.TryGetValue(entry.Key, out var unchanged) && entry.Value.All(unchanged.Contains));
            return _result;
        }

        public override Expression? Visit(Expression? node)
        {
            if (node is null)
                return node;

            if (node.TryResolveColumn(resolveEntity, primaryKeyOf, out var entity, out var column))
            {
                _result.Add(entity.LinkAlias, column);
                if (_inResult && IsValuePreserving(node))
                    _unchanged.Add(entity.LinkAlias, column);

                if (node == _body && entity.LinkAlias is null && IsValuePreserving(node))
                    _result.SingleRootColumn = column;
                return node;
            }

            if (ResolveEntity(node) is { } whole)
            {
                _result.WholeEntities.Add(ProjectedColumns.KeyOf(whole.LinkAlias));
                if (_inResult && IsValuePreserving(node))
                    _unchanged.WholeEntities.Add(ProjectedColumns.KeyOf(whole.LinkAlias));
                return node;
            }

            var inResult = _inResult;
            _inResult = inResult && (node is NewExpression { Type: var type } && type.IsAnonymousType()
                || node is MemberInitExpression
                || node is NewArrayExpression { NodeType: ExpressionType.NewArrayInit }
                || node.IsCast(out _) && IsValuePreserving(node));
            try
            {
                return base.Visit(node);
            }
            finally
            {
                _inResult = inResult;
            }
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
            if (node.IsCountChildrenCall())
            {
                return node;
            }

            return base.VisitMethodCall(node);
        }

        // A joined row is there when its primary key is; the root's rows always are.
        private void RequireRow(MaterializerBuilder.ResolvedEntity entity)
        {
            if (entity.LinkAlias is null)
                return;

            if (LogicalNameOf(entity) is { } logicalName)
                _result.Add(entity.LinkAlias, primaryKeyOf(logicalName));
            else
                _result.WholeEntities.Add(entity.LinkAlias);
        }

        private MaterializerBuilder.ResolvedEntity? ResolveEntity(Expression? expr) =>
            expr is null ? null : resolveEntity(expr.UnwrapConvert());

        // A cast that lifts to or from Nullable, boxes or unboxes ((int?)e["count"]), or switches an
        // enum and its underlying type leaves the column's value as it is. A numeric one doesn't:
        // narrowing truncates or wraps ((int)a.CreditLimit.Value), and even widening changes how
        // the value aggregates ((long?)a.Count sums without the int column's overflow).
        private static bool IsValuePreserving(Expression expr)
        {
            while (expr.IsCast(out var convert))
            {
                var from = Nullable.GetUnderlyingType(convert.Operand.Type) ?? convert.Operand.Type;
                var to = Nullable.GetUnderlyingType(convert.Type) ?? convert.Type;

                // "as" gives null for a value of another type (a Money column as decimal?).
                if (convert.NodeType == ExpressionType.TypeAs && !convert.Type.IsAssignableFrom(convert.Operand.Type))
                    return false;

                // A conversion method is user code, or decimal's own numeric conversions.
                if (convert.Method is not null)
                    return false;

                if (from != to && from.IsValueType && to.IsValueType && UnderlyingOf(from) != UnderlyingOf(to))
                    return false;

                expr = convert.Operand;
            }

            return true;
        }

        private static Type UnderlyingOf(Type type) => type.IsEnum ? Enum.GetUnderlyingType(type) : type;

        private static bool IsNull(Expression expr) => expr.UnwrapConvert() is ConstantExpression { Value: null };
    }

    /// <summary>
    /// Returns <c>true</c> when the call is <see cref="ServiceClientExtensions.CountChildren"/>.
    /// </summary>
    internal static bool IsCountChildrenCall(this MethodCallExpression call) =>
        call.Method.Name == nameof(ServiceClientExtensions.CountChildren)
        && call.Method.DeclaringType == typeof(ServiceClientExtensions);

    /// <summary>
    /// The number of <see cref="ServiceClientExtensions.CountChildren"/> calls anywhere in the
    /// expression.
    /// </summary>
    internal static int CountChildrenCalls(this Expression expr)
    {
        var counter = new CountChildrenCounter();
        counter.Visit(expr);
        return counter.Count;
    }

    private sealed class CountChildrenCounter : ExpressionVisitor
    {
        public int Count { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.IsCountChildrenCall())
                Count++;

            return base.VisitMethodCall(node);
        }
    }

    internal static IReadOnlyList<RowAggregateInfo>? ExtractRowAggregates(this Expression body)
    {
        List<RowAggregateInfo>? results = null;

        if (body is NewExpression ne && ne.Members is not null)
        {
            for (var i = 0; i < ne.Arguments.Count; i++)
            {
                if (ne.Arguments[i] is MethodCallExpression mc
                    && mc.IsCountChildrenCall())
                {
                    var member = ne.Members[i];
                    var memberName = member.GetInitializedMemberName();
                    results ??= [];
                    results.Add(new RowAggregateInfo(memberName.ToLowerInvariant()));
                }
            }
        }
        else if (body is MemberInitExpression init)
        {
            foreach (var binding in init.Bindings.OfType<MemberAssignment>())
            {
                if (binding.Expression is MethodCallExpression mc
                    && mc.IsCountChildrenCall())
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
        AddColumns(query.Attributes, columns);
    }

    // Adds each column not already requested as a plain column (an aliased attribute is an
    // aggregate, and doesn't count).
    private static void AddColumns(List<FetchAttribute> attributes, IEnumerable<string> columns)
    {
        var requested = new HashSet<string>(attributes.Where(a => a.Alias is null).Select(a => a.Name));
        foreach (var column in columns)
        {
            if (requested.Add(column))
                attributes.Add(new FetchAttribute { Name = column });
        }
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

    /// <summary>
    /// <c>true</c> when every column the select reads is also in its result unchanged, and so is
    /// every entity it uses whole: two rows whose columns differ then give different results, so
    /// Dataverse de-duplicating the columns de-duplicates the results. A value computed from a
    /// column the result doesn't also hold (<c>a.Name.Substring(0, 1)</c> alone) breaks that.
    /// </summary>
    public bool PreservesColumns { get; set; }

    /// <summary>
    /// The root column the select reads, when that read is the whole select and its value is the
    /// column's unchanged (<c>a =&gt; a.Name</c>, <c>a =&gt; (int?)a.NumberOfEmployees</c>), so an
    /// aggregate of the select is an aggregate of the column. Null otherwise.
    /// </summary>
    public string? SingleRootColumn { get; set; }

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
            if (node.Arguments[i] is MethodCallExpression mc
                && mc.IsCountChildrenCall())
            {
                var member = node.Members[i];
                var memberName = member.GetInitializedMemberName();
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
                && assignment.Expression is MethodCallExpression mc
                && mc.IsCountChildrenCall())
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

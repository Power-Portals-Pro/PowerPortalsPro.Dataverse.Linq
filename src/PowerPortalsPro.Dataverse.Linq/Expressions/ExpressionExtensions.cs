using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Expressions;

/// <summary>
/// Extension methods for evaluating, inspecting, and extracting information
/// from LINQ expression trees.
/// </summary>
internal static class ExpressionExtensions
{
    internal static bool IsNullConstant(this Expression expr) =>
        expr is ConstantExpression { Value: null }
        || (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert
            && convert.Operand is ConstantExpression { Value: null });

    internal static object? EvaluateValue(this Expression expr)
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
                ? memberExpr.Expression.EvaluateValue()
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
            var items = newArray.Expressions.Select(EvaluateValueCore).ToArray();
            var array = Array.CreateInstance(newArray.Type.GetElementType()!, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }

        // Convert / cast
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return convert.Operand.EvaluateValue();

        // Method call (e.g. implicit conversions, static methods)
        if (expr is MethodCallExpression methodCall)
        {
            // Implicit/explicit conversion operators with a single argument — just evaluate the argument
            if (methodCall.Method is { IsSpecialName: true, Name: "op_Implicit" or "op_Explicit" }
                && methodCall.Arguments.Count == 1)
            {
                return methodCall.Arguments[0].EvaluateValue();
            }

            var target = methodCall.Object is not null ? methodCall.Object.EvaluateValue() : null;
            var args = methodCall.Arguments.Select(a => a.EvaluateValue()).ToArray();
            return methodCall.Method.Invoke(target, args);
        }

        // Constructor call (e.g. new DateTime(2020, 1, 1))
        if (expr is NewExpression newExpr && newExpr.Constructor is not null)
        {
            var args = newExpr.Arguments.Select(a => a.EvaluateValue()).ToArray();
            return newExpr.Constructor.Invoke(args);
        }

        throw new NotSupportedException(
            $"Unable to evaluate expression of type {expr.GetType().Name} (NodeType={expr.NodeType}): {expr}");
    }

    /// <summary>
    /// Resolves an expression that names something (an attribute logical name, for example)
    /// to its string value. String literals resolve directly; any other expression that does
    /// not depend on a lambda parameter — a captured local, a field or property, a method call,
    /// a concatenation — is evaluated while the query is being translated.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The expression is free of lambda parameters but could not be evaluated, or evaluated to
    /// something other than a non-empty string.
    /// </exception>
    internal static bool TryEvaluateName(this Expression expr, [NotNullWhen(true)] out string? name)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        if (expr is ConstantExpression { Value: string constant })
        {
            name = constant;
            return true;
        }

        // Anything referencing a lambda parameter depends on the row being queried and so
        // cannot be resolved while the query is being built.
        if (expr.ReferencesAnyParameter())
        {
            name = null;
            return false;
        }

        object? value;
        try
        {
            try
            {
                value = expr.EvaluateValue();
            }
            catch (NotSupportedException)
            {
                // Fall back to the compiler for node types EvaluateValue doesn't handle
                // (string concatenation, conditionals, delegate invocation, ...).
                value = Expression.Lambda(expr).Compile().DynamicInvoke();
            }
        }
        catch (Exception ex)
        {
            throw new NotSupportedException(
                $"Unable to evaluate the name argument '{expr}': {ex.Message}", ex);
        }

        name = value as string;
        if (string.IsNullOrEmpty(name))
        {
            throw new NotSupportedException(
                $"The name argument '{expr}' evaluated to "
                + (value is null ? "null" : $"'{value}'")
                + ". It must resolve to a non-empty string.");
        }

        return true;
    }

    /// <summary>
    /// Returns <c>true</c> when the expression references a <see cref="ParameterExpression"/>
    /// that is not declared by a lambda inside the expression itself — i.e. the expression
    /// cannot be evaluated on its own.
    /// </summary>
    internal static bool ReferencesAnyParameter(this Expression expr)
    {
        var finder = new FreeParameterFinder();
        finder.Visit(expr);
        return finder.Found;
    }

    private sealed class FreeParameterFinder : ExpressionVisitor
    {
        private readonly HashSet<ParameterExpression> _declared = [];

        internal bool Found { get; private set; }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            foreach (var param in node.Parameters)
                _declared.Add(param);

            var result = base.VisitLambda(node);

            foreach (var param in node.Parameters)
                _declared.Remove(param);

            return result;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (!_declared.Contains(node))
                Found = true;
            return node;
        }
    }

    /// <summary>
    /// The expression with any casts around it removed.
    /// </summary>
    internal static Expression UnwrapConvert(this Expression expr)
    {
        while (expr.IsCast(out var cast))
            expr = cast.Operand;
        return expr;
    }

    /// <summary>
    /// Returns <c>true</c> when the expression is a cast: <c>(T)x</c>, checked or not, or <c>x as T</c>.
    /// </summary>
    internal static bool IsCast(this Expression expr, [NotNullWhen(true)] out UnaryExpression? cast)
    {
        cast = expr as UnaryExpression;
        return cast is { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs };
    }

    /// <summary>
    /// Returns <c>true</c> when a cast around the expression may change the value it gives: a
    /// numeric conversion that truncates, wraps or rounds (<c>(int)a.Revenue.Value</c>,
    /// <c>(byte)a.NumberOfEmployees</c>, <c>(double)a.Revenue.Value</c>), or a conversion method
    /// of the user's. Lifting to or from Nullable, boxing, unboxing, a reference conversion,
    /// switching an enum and its number, and a numeric conversion that keeps every value exactly
    /// (<c>(long?)a.NumberOfEmployees</c>, an int to double) give the value as it compares.
    /// </summary>
    internal static bool HasLossyCast(this Expression expr)
    {
        while (expr.IsCast(out var cast))
        {
            if (NumericCodeOf(cast.Operand.Type) is { } from && NumericCodeOf(cast.Type) is { } to
                    ? !Holds(to, from)
                    : cast.Method is not null)
            {
                return true;
            }

            expr = cast.Operand;
        }

        return false;
    }

    // The numeric type a value is, under Nullable and an enum; null for any other type.
    private static TypeCode? NumericCodeOf(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);

        var code = Type.GetTypeCode(type);
        return code is >= TypeCode.Char and <= TypeCode.Decimal ? code : null;
    }

    // Whether every value of one numeric type converts to another exactly. An integer type holds
    // the integers in its range, decimal holds every integer, and float and double the integers
    // their significands hold (2^24, 2^53: (float)16777217 is 16777216). A float is a double
    // exactly; no other fraction converts exactly ((double)0.1m isn't 0.1m).
    private static bool Holds(TypeCode to, TypeCode from)
    {
        if (to == from)
            return true;

        if (IntegerRange(from) is not var (fromMin, fromMax))
            return from == TypeCode.Single && to == TypeCode.Double;

        return to switch
        {
            TypeCode.Decimal => true,
            TypeCode.Single => -16_777_216m <= fromMin && fromMax <= 16_777_216m,
            TypeCode.Double => -9_007_199_254_740_992m <= fromMin && fromMax <= 9_007_199_254_740_992m,
            _ => IntegerRange(to) is var (toMin, toMax) && toMin <= fromMin && fromMax <= toMax,
        };
    }

    private static (decimal Min, decimal Max)? IntegerRange(TypeCode code) => code switch
    {
        TypeCode.SByte => (sbyte.MinValue, sbyte.MaxValue),
        TypeCode.Byte => (byte.MinValue, byte.MaxValue),
        TypeCode.Int16 => (short.MinValue, short.MaxValue),
        TypeCode.UInt16 or TypeCode.Char => (ushort.MinValue, ushort.MaxValue),
        TypeCode.Int32 => (int.MinValue, int.MaxValue),
        TypeCode.UInt32 => (uint.MinValue, uint.MaxValue),
        TypeCode.Int64 => (long.MinValue, long.MaxValue),
        TypeCode.UInt64 => (ulong.MinValue, ulong.MaxValue),
        _ => null,
    };

    /// <summary>
    /// Replaces every reference to <paramref name="parameter"/> in the expression with
    /// <paramref name="replacement"/>.
    /// </summary>
    internal static Expression ReplaceParameter(
        this Expression expr, ParameterExpression parameter, Expression replacement) =>
        new ParameterReplacer(parameter, replacement).Visit(expr);

    private sealed class ParameterReplacer(ParameterExpression parameter, Expression replacement)
        : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == parameter ? replacement : node;
    }

    /// <summary>
    /// Returns <c>true</c> when evaluating the expression may evaluate one of
    /// <paramref name="parts"/> (those very nodes, not equal ones) more than once: it occurs twice,
    /// or inside a nested lambda, which may run any number of times. A part nested in another
    /// counts each time the other does. An occurrence that <paramref name="onlyReadsMember"/> says
    /// is the owner of a member read, not used itself, doesn't count.
    /// </summary>
    internal static bool EvaluatesAnyMoreThanOnce(
        this Expression expr, IEnumerable<Expression> parts, Func<Expression, bool> onlyReadsMember)
    {
        var finder = new RepeatFinder(parts, onlyReadsMember);
        finder.Visit(expr);
        return finder.Found;
    }

    private sealed class RepeatFinder(IEnumerable<Expression> parts, Func<Expression, bool> onlyReadsMember)
        : ExpressionVisitor
    {
        private readonly HashSet<Expression> _parts = [.. parts];
        private readonly Dictionary<Expression, int> _counts = [];
        private int _lambdaDepth;

        // The owner of the member read being visited, when reading the member doesn't use it.
        private Expression? _readOwner;

        public bool Found { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (node is not null && ReferenceEquals(node, _readOwner))
            {
                _readOwner = null;
            }
            else if (node is not null && _parts.Contains(node))
            {
                var count = (_counts.TryGetValue(node, out var counted) ? counted : 0) + (_lambdaDepth > 0 ? 2 : 1);
                _counts[node] = count;
                Found |= count > 1;
            }

            return base.Visit(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is { } owner && onlyReadsMember(owner))
                _readOwner = owner;

            return base.VisitMember(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            _lambdaDepth++;
            var result = base.VisitLambda(node);
            _lambdaDepth--;
            return result;
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the expression makes a new object.
    /// </summary>
    internal static bool IsNewObject(this Expression expr) =>
        expr is NewExpression or MemberInitExpression or NewArrayExpression or ListInitExpression;

    /// <summary>
    /// The parts of the expression that do work, which evaluating twice would change: a method
    /// call (<c>Guid.NewGuid()</c>; reading an entity's column is not one), a static property
    /// (<c>DateTime.Now</c>), and a new object, since one made twice is two objects. Reading a
    /// parameter, a constant, a member or a column, or applying an operator, gives the same value
    /// again. Parts inside a lambda of the expression aren't listed: they run when the call the
    /// lambda is passed to does, which is listed.
    /// </summary>
    internal static List<Expression> WorkingParts(this Expression expr)
    {
        var finder = new WorkingPartFinder();
        finder.Visit(expr);
        return finder.Parts;
    }

    private sealed class WorkingPartFinder : ExpressionVisitor
    {
        // The constructor call of a member-init or list-init being visited: part of that object,
        // not another one.
        private Expression? _initialized;

        public List<Expression> Parts { get; } = [];

        protected override Expression VisitLambda<T>(Expression<T> node) => node;

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            _initialized = node.NewExpression;
            return base.VisitMemberInit(node);
        }

        protected override Expression VisitListInit(ListInitExpression node)
        {
            _initialized = node.NewExpression;
            return base.VisitListInit(node);
        }

        public override Expression? Visit(Expression? node)
        {
            if (node is MethodCallExpression call
                    && !call.IsColumnReadCall(out _, out _)
                || node is not null && (node.IsNewObject() || node is InvocationExpression)
                    && !ReferenceEquals(node, _initialized)
                || node is MemberExpression { Expression: null, Member: PropertyInfo })
            {
                Parts.Add(node);
            }

            return base.Visit(node);
        }
    }

    /// <summary>
    /// Rewrites each read through an entity's indexer, <c>entity["name"]</c>, as
    /// <c>entity.GetAttributeValue&lt;object&gt;("name")</c>: null for a column the row doesn't
    /// have, as a select requests only the columns it reads and a null column comes back absent,
    /// rather than <see cref="KeyNotFoundException"/>.
    /// </summary>
    internal static Expression ReadIndexerAsAttributeValue(this Expression expr) =>
        new IndexerReadRewriter().Visit(expr);

    private sealed class IndexerReadRewriter : ExpressionVisitor
    {
        private static readonly MethodInfo GetAttributeValueOfObject =
            typeof(Microsoft.Xrm.Sdk.Entity).GetMethod(nameof(Microsoft.Xrm.Sdk.Entity.GetAttributeValue))!
                .MakeGenericMethod(typeof(object));

        protected override Expression VisitMethodCall(MethodCallExpression node) =>
            node.IsEntityIndexer(out var entity, out var name)
                ? Expression.Call(Visit(entity), GetAttributeValueOfObject, Visit(name))
                : base.VisitMethodCall(node);
    }

    /// <summary>
    /// The name of a member a <see cref="NewExpression"/> initializes: the property's, or, on
    /// runtimes that record its getter instead, the name the getter is for.
    /// </summary>
    internal static string GetInitializedMemberName(this MemberInfo member) =>
        member is MethodInfo { Name: ['g', 'e', 't', '_', ..] } getter ? getter.Name[4..] : member.Name;

    /// <summary>
    /// Replaces a read of an anonymous type's member off the expression that creates it,
    /// <c>new { a.Name, a.Website }.Name</c>, with the value it was created from, <c>a.Name</c>.
    /// An anonymous type's property returns exactly that value; a class's might not, so only
    /// anonymous types (the <see cref="NewExpression"/>s that name their members) are reduced.
    /// </summary>
    internal static Expression ReduceAnonymousMemberReads(this Expression expr) =>
        new AnonymousMemberReducer(columnOf: null).Visit(expr);

    /// <summary>
    /// As <see cref="ReduceAnonymousMemberReads"/>, and also replaces a read of an entity's column
    /// off the member-init that sets it with the value it's set to: an entity's property stores the
    /// value as its column, and the entity reads it back. The read may name the member
    /// (<c>new Account { Name = a.Name }.Name</c>) or the column another member sets: <c>Id</c> after
    /// <c>AccountId = a.AccountId</c>, <c>["name"]</c> or <c>GetAttributeValue</c> after
    /// <c>Name = …</c>. A column the member-init doesn't set is left as it is.
    /// </summary>
    /// <param name="columnOf">The column a read of an entity's member, indexer or GetAttributeValue names.</param>
    internal static Expression ReduceCreatedMemberReads(this Expression expr, Func<Expression, string?> columnOf) =>
        new AnonymousMemberReducer(columnOf).Visit(expr);

    // The compiler's anonymous types: generated, and named for it (<>f__AnonymousType0, VB$AnonymousType_0).
    internal static bool IsAnonymousType(this Type type) =>
        type.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), inherit: false)
        && type.Name.Contains("AnonymousType");

    // Reduces reads off anonymous types, and, given columnOf, off entity member-inits.
    private sealed class AnonymousMemberReducer(Func<Expression, string?>? columnOf) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            var owner = Visit(node.Expression);
            if (owner is NewExpression { Members: { } members } created && created.Type.IsAnonymousType())
            {
                for (var i = 0; i < members.Count; i++)
                {
                    var name = members[i].GetInitializedMemberName();
                    if (name == node.Member.Name)
                        return created.Arguments[i];
                }
            }

            var read = node.Update(owner);
            return EntityInitOf(owner) is { } init && ValueSet(init, read, node.Member.Name) is { } value
                ? value
                : read;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var read = (MethodCallExpression)base.VisitMethodCall(node);
            return EntityInitOf(read.Object) is { } init
                && read.IsColumnReadCall(out _, out _)
                && ValueSet(init, read, memberName: null) is { } value
                    ? value
                    : read;
        }

        private MemberInitExpression? EntityInitOf(Expression? owner) =>
            columnOf is not null
            && owner is MemberInitExpression { Type: var type } init
            && typeof(Microsoft.Xrm.Sdk.Entity).IsAssignableFrom(type)
                ? init
                : null;

        // The value the member-init last sets the read's member, or its column, to, as the read's type.
        private Expression? ValueSet(MemberInitExpression init, Expression read, string? memberName)
        {
            var bindings = init.Bindings.OfType<MemberAssignment>().ToList();
            var set = memberName is null ? null : bindings.LastOrDefault(b => b.Member.Name == memberName);
            if (set is null && columnOf!(read) is { } column)
                set = bindings.LastOrDefault(b => b.Member is not PropertyInfo { CanRead: false }
                    && columnOf(Expression.MakeMemberAccess(init, b.Member)) == column);

            if (set is null)
                return null;

            if (set.Expression.Type == read.Type)
                return set.Expression;

            // The column's value read as another type: Guid? as Id's Guid, a string as the indexer's object.
            try
            {
                return Expression.Convert(set.Expression, read.Type);
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Unwraps Convert nodes and Nullable&lt;T&gt; to get the underlying CLR type of an expression.
    /// </summary>
    internal static Type UnwrapExpressionType(this Expression expr)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;
        var type = expr.Type;
        return Nullable.GetUnderlyingType(type) ?? type;
    }

    internal static LambdaExpression ExtractLambda(this Expression expr) =>
        expr is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expr;

    /// <summary>
    /// Flattens a chain of <see cref="ExpressionType.AndAlso"/> nodes into a flat list.
    /// </summary>
    internal static List<Expression> FlattenAndAlso(this Expression expr)
    {
        var result = new List<Expression>();
        if (expr is BinaryExpression { NodeType: ExpressionType.AndAlso } binary)
        {
            result.AddRange(binary.Left.FlattenAndAlso());
            result.AddRange(binary.Right.FlattenAndAlso());
        }
        else
        {
            result.Add(expr);
        }
        return result;
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="expr"/> contains a reference
    /// to the given <paramref name="param"/>.
    /// </summary>
    internal static bool ReferencesParameter(this Expression expr, ParameterExpression param)
    {
        return expr switch
        {
            _ when expr == param => true,
            UnaryExpression u => u.Operand.ReferencesParameter(param),
            MemberExpression m => m.Expression is not null && m.Expression.ReferencesParameter(param),
            MethodCallExpression mc =>
                (mc.Object is not null && mc.Object.ReferencesParameter(param))
                || mc.Arguments.Any(a => a.ReferencesParameter(param)),
            BinaryExpression b =>
                b.Left.ReferencesParameter(param) || b.Right.ReferencesParameter(param),
            _ => false
        };
    }

    internal static bool IsStringLength(this Expression expr) =>
        expr is MemberExpression { Member: { Name: "Length", DeclaringType: var dt } }
        && dt == typeof(string);
}

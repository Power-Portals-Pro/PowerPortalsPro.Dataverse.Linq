using System.Collections;
using System.Linq.Expressions;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// An <see cref="IQueryProvider"/> used by <c>ToFetchXml(q =&gt; q.Terminal())</c> to capture
/// the LINQ expression produced by a terminal operator (aggregate or element operator)
/// without querying Dataverse. Execution is short-circuited: the expression is recorded
/// and a default result is returned. The captured expression is then translated by the
/// real provider via <see cref="GenerateFetchXml"/>.
/// </summary>
internal sealed class FetchXmlCaptureProvider : IQueryProvider
{
    // The real DataverseQueryProvider<T>; T is not known statically here, so member
    // access goes through dynamic (resolved within this assembly, internal-visible).
    private readonly object _innerProvider;

    internal Expression? CapturedExpression { get; private set; }

    internal FetchXmlCaptureProvider(object innerProvider) => _innerProvider = innerProvider;

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Use the generic CreateQuery<TElement> overload.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
        new FetchXmlCaptureQueryable<TElement>(this, expression);

    public object? Execute(Expression expression)
    {
        CapturedExpression = expression;
        return null;
    }

    public TResult Execute<TResult>(Expression expression)
    {
        CapturedExpression = expression;
        return default!;
    }

    internal void Capture(Expression expression) => CapturedExpression = expression;

    internal string GenerateFetchXml() =>
        (string)((dynamic)_innerProvider).GenerateFetchXml(CapturedExpression!);
}

/// <summary>
/// A minimal <see cref="IQueryable{T}"/> backed by <see cref="FetchXmlCaptureProvider"/>.
/// Operators build their expressions against it as usual; terminal operators route into
/// the capture provider's <c>Execute</c>, and enumeration captures the expression and
/// yields nothing.
/// </summary>
internal sealed class FetchXmlCaptureQueryable<TElement> : IQueryable<TElement>
{
    private readonly FetchXmlCaptureProvider _provider;

    internal FetchXmlCaptureQueryable(FetchXmlCaptureProvider provider, Expression expression)
    {
        _provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(TElement);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    public IEnumerator<TElement> GetEnumerator()
    {
        // Enumeration (e.g. ToList) — capture the expression and yield no results.
        _provider.Capture(Expression);
        return Enumerable.Empty<TElement>().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

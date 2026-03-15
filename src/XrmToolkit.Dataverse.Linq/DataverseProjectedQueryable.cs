using System.Collections;
using System.Linq.Expressions;

namespace XrmToolkit.Dataverse.Linq;

/// <summary>
/// Returned by <see cref="DataverseQueryProvider{T}.CreateQuery{TElement}"/> when
/// <typeparamref name="TElement"/> is not the source entity type (e.g. an anonymous-type
/// projection).  Carries the full expression tree through to async execution.
/// </summary>
internal class DataverseProjectedQueryable<TElement> : IQueryable<TElement>, IOrderedQueryable<TElement>
{
    private readonly IQueryProvider _provider;

    internal DataverseProjectedQueryable(IQueryProvider provider, Expression expression)
    {
        _provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(TElement);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    public IEnumerator<TElement> GetEnumerator() =>
        _provider.Execute<IEnumerable<TElement>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

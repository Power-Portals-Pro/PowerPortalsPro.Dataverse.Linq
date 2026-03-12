using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using System.Collections;
using System.Linq.Expressions;

namespace Dataverse.Linq;

/// <summary>
/// An async-capable queryable for Dataverse entities that builds and executes FetchXml.
/// </summary>
/// <typeparam name="T">An entity type decorated with <see cref="Microsoft.Xrm.Sdk.Client.EntityLogicalNameAttribute"/>.</typeparam>
public class DataverseQueryable<T> : IQueryable<T>, IOrderedQueryable<T> where T : Entity
{
    private readonly DataverseQueryProvider<T> _provider;

    internal DataverseQueryable(IOrganizationServiceAsync service, string entityLogicalName)
    {
        _provider = new DataverseQueryProvider<T>(service, entityLogicalName);
        Expression = Expression.Constant(this);
    }

    // Called by CreateQuery<T> when LINQ operators are applied (e.g. Where, OrderBy).
    internal DataverseQueryable(DataverseQueryProvider<T> provider, Expression expression)
    {
        _provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    public IEnumerator<T> GetEnumerator() =>
        _provider.ExecuteList(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    internal Task<List<T>> ExecuteAsync(CancellationToken cancellationToken = default) =>
        _provider.ExecuteAsync<List<T>>(Expression, cancellationToken);
}

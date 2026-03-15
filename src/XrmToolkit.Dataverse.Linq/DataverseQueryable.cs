using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using System.Collections;
using System.Linq.Expressions;

namespace XrmToolkit.Dataverse.Linq;

/// <summary>
/// An async-capable queryable for Dataverse entities that builds and executes FetchXml.
/// </summary>
/// <typeparam name="T">An entity type decorated with <see cref="Microsoft.Xrm.Sdk.Client.EntityLogicalNameAttribute"/>.</typeparam>
public class DataverseQueryable<T> : IQueryable<T>, IOrderedQueryable<T> where T : Entity
{
    private readonly DataverseQueryProvider<T> _provider;

    internal DataverseQueryable(IOrganizationServiceAsync service, string entityLogicalName, IReadOnlyList<string>? columns = null)
    {
        _provider = new DataverseQueryProvider<T>(service, entityLogicalName, columns);
        Expression = Expression.Constant(this);
    }

    // Called by CreateQuery<T> when LINQ operators are applied (e.g. Where, OrderBy).
    internal DataverseQueryable(DataverseQueryProvider<T> provider, Expression expression)
    {
        _provider = provider;
        Expression = expression;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(T);
    /// <inheritdoc />
    public Expression Expression { get; }
    /// <inheritdoc />
    public IQueryProvider Provider => _provider;

    /// <inheritdoc />
    public IEnumerator<T> GetEnumerator() =>
        _provider.ExecuteList(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

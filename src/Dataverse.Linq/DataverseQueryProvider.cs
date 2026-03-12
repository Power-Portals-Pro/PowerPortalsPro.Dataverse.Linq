using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Linq.Expressions;
using System.Xml.Linq;

namespace Dataverse.Linq;

internal class DataverseQueryProvider<T> : IAsyncQueryProvider where T : Entity
{
    internal IOrganizationServiceAsync Service { get; }
    internal string EntityLogicalName { get; }

    internal DataverseQueryProvider(IOrganizationServiceAsync service, string entityLogicalName)
    {
        Service = service;
        EntityLogicalName = entityLogicalName;
    }

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Use the generic CreateQuery<TElement> overload.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) != typeof(T))
            throw new NotSupportedException($"Query element type mismatch: expected {typeof(T).Name}, got {typeof(TElement).Name}.");

        return (IQueryable<TElement>)(object)new DataverseQueryable<T>(this, expression);
    }

    public object? Execute(Expression expression) => ExecuteList(expression);

    public TResult Execute<TResult>(Expression expression) =>
        ExecuteAsync<TResult>(expression).GetAwaiter().GetResult();

    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        var results = await ExecuteListAsync(expression, cancellationToken);
        if (results is TResult result)
            return result;
        throw new InvalidCastException($"Cannot convert {nameof(List<T>)} to {typeof(TResult).Name}.");
    }

    internal List<T> ExecuteList(Expression expression) =>
        ExecuteListAsync(expression).GetAwaiter().GetResult();

    internal async Task<List<T>> ExecuteListAsync(Expression expression, CancellationToken cancellationToken = default)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return entities.Select(e => e.ToEntity<T>()).ToList();
    }

    private async Task<List<Entity>> RetrieveAllAsync(string baseFetchXml, CancellationToken cancellationToken)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = await Service.RetrieveMultipleAsync(
                new FetchExpression(fetchDocument.ToString()));

            results.AddRange(response.Entities);

            if (!response.MoreRecords)
                break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }

        return results;
    }
}

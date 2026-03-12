using Dataverse.Linq.Expressions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Collections;
using System.Linq.Expressions;
using System.Xml.Linq;

namespace Dataverse.Linq;

internal class DataverseQueryProvider<T> : IAsyncQueryProvider where T : Entity
{
    internal IOrganizationServiceAsync Service { get; }
    internal string EntityLogicalName { get; }
    internal IReadOnlyList<string>? Columns { get; }

    internal DataverseQueryProvider(IOrganizationServiceAsync service, string entityLogicalName, IReadOnlyList<string>? columns = null)
    {
        Service = service;
        EntityLogicalName = entityLogicalName;
        Columns = columns;
    }

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Use the generic CreateQuery<TElement> overload.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) == typeof(T))
            return (IQueryable<TElement>)(object)new DataverseQueryable<T>(this, expression);

        // Projected type (e.g. anonymous type from a Select clause)
        return new DataverseProjectedQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression) => ExecuteList(expression);

    public TResult Execute<TResult>(Expression expression) =>
        ExecuteAsync<TResult>(expression).GetAwaiter().GetResult();

    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        var (selectColumns, projector) = SelectExpressionParser.Parse(expression);
        var columns = selectColumns ?? Columns;

        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        var typedEntities = entities.Select(e => e.ToEntity<T>());

        if (projector is not null)
            return BuildProjectedList<TResult>(typedEntities, projector);

        var results = typedEntities.ToList();
        if (results is TResult directResult)
            return directResult;

        throw new InvalidCastException($"Cannot convert List<{typeof(T).Name}> to {typeof(TResult).Name}.");
    }

    internal List<T> ExecuteList(Expression expression) =>
        ExecuteAsync<List<T>>(expression).GetAwaiter().GetResult();

    private static TResult BuildProjectedList<TResult>(IEnumerable<T> source, Delegate projector)
    {
        var elementType = typeof(TResult).GetGenericArguments()[0];
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;

        foreach (var item in source)
            list.Add(projector.DynamicInvoke(item));

        return (TResult)(object)list;
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

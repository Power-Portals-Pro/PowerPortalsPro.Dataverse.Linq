#if !NETFRAMEWORK
using PowerPortalsPro.Dataverse.Linq.Expressions;
using PowerPortalsPro.Dataverse.Linq.Model;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Linq;

namespace PowerPortalsPro.Dataverse.Linq;

internal class DataverseQueryProviderAsync<T> : DataverseQueryProvider<T>, IAsyncQueryProvider where T : Entity
{
    private readonly IOrganizationServiceAsync _asyncService;

    internal DataverseQueryProviderAsync(IOrganizationServiceAsync service, string entityLogicalName, IReadOnlyList<string>? columns = null)
        : base(service, entityLogicalName, columns)
    {
        _asyncService = service;
    }

    // -------------------------------------------------------------------------
    // IAsyncQueryProvider (EF Core)
    // Note: TResult is the Task itself (e.g. Task<List<TElement>>), not the
    // unwrapped value — this matches EF Core's interface contract.
    // -------------------------------------------------------------------------

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);

        // Aggregate terminal operator: TResult = Task<TElement>
        if (query.TerminalOperator is QueryTerminalOperator.Min or QueryTerminalOperator.Max
            or QueryTerminalOperator.Sum or QueryTerminalOperator.Average
            or QueryTerminalOperator.Count or QueryTerminalOperator.LongCount
            or QueryTerminalOperator.CountColumn)
        {
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var method = GetAsyncMethod(nameof(ExecuteAggregateAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }

        // Scalar terminal operator: TResult = Task<TElement>
        if (query.TerminalOperator is not QueryTerminalOperator.List)
        {
            var elementType = typeof(TResult).GetGenericArguments()[0];
            var method = GetAsyncMethod(nameof(ExecuteScalarAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }

        // List: TResult = Task<List<TElement>>
        {
            var elementType = typeof(TResult).GetGenericArguments()[0].GetGenericArguments()[0];
            var method = GetAsyncMethod(nameof(ExecuteQueryAsync)).MakeGenericMethod(elementType);
            return (TResult)method.Invoke(this, [query, cancellationToken])!;
        }
    }

    // -------------------------------------------------------------------------
    // Async paged iteration
    // -------------------------------------------------------------------------

    internal async Task ForEachPageAsync<TElement>(
        Expression expression,
        Func<List<TElement>, Task> onPage,
        CancellationToken cancellationToken)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);
        var fetchXml = FetchXmlBuilder.Build(query);
        var fetchDocument = XDocument.Parse(fetchXml);
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = await _asyncService.RetrieveMultipleAsync(new FetchExpression(fetchDocument.ToString()));
            var page = ProjectEntities<TElement>(response.Entities.ToList(), query);
            await onPage(page);

            if (!response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }
    }

    // -------------------------------------------------------------------------
    // Async execution
    // -------------------------------------------------------------------------

    private async Task<List<TElement>> ExecuteQueryAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return ProjectEntities<TElement>(entities, query);
    }

    private async Task<TElement> ExecuteScalarAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        var projected = ProjectEntities<TElement>(entities, query);
        return ApplyTerminalOperator(projected, query.TerminalOperator);
    }

    private async Task<TElement> ExecuteAggregateAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return ExtractAggregateResult<TElement>(entities, query.TerminalOperator);
    }

    // -------------------------------------------------------------------------
    // Paged retrieval (asynchronous)
    // -------------------------------------------------------------------------

    private async Task<List<Entity>> RetrieveAllAsync(string baseFetchXml, CancellationToken cancellationToken)
    {
        var results = new List<Entity>();
        var fetchDocument = XDocument.Parse(baseFetchXml);
        var explicitPage = fetchDocument.Root!.Attribute("page") != null;
        string? pagingCookie = null;
        var pageNumber = 1;

        while (true)
        {
            if (pagingCookie != null)
            {
                fetchDocument.Root!.SetAttributeValue("paging-cookie", pagingCookie);
                fetchDocument.Root!.SetAttributeValue("page", pageNumber);
            }

            var response = await _asyncService.RetrieveMultipleAsync(new FetchExpression(fetchDocument.ToString()));
            results.AddRange(response.Entities);

            if (explicitPage || !response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }

        return results;
    }

    private static MethodInfo GetAsyncMethod(string name) =>
        typeof(DataverseQueryProviderAsync<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}
#endif

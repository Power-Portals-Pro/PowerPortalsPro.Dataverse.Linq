#if !NETFRAMEWORK
using PowerPortalsPro.Dataverse.Linq.Expressions;
using PowerPortalsPro.Dataverse.Linq.Model;
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
    // IAsyncQueryProvider
    // Note: TResult is the Task itself (e.g. Task<List<TElement>>), not the
    // unwrapped value.
    // -------------------------------------------------------------------------

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression, Columns, EntityLogicalName, Service);

        string methodName;
        Type elementType;

        if (query.TerminalOperator.IsAggregate())
        {
            methodName = nameof(ExecuteAggregateAsync);
            elementType = typeof(TResult).GetGenericArguments()[0];
        }
        else if (query.TerminalOperator.IsScalar())
        {
            methodName = nameof(ExecuteScalarAsync);
            elementType = typeof(TResult).GetGenericArguments()[0];
        }
        else
        {
            methodName = nameof(ExecuteQueryAsync);
            elementType = typeof(TResult).GetGenericArguments()[0].GetGenericArguments()[0];
        }

        var method = GetAsyncMethod(methodName).MakeGenericMethod(elementType);
        return (TResult)method.Invoke(this, [query, cancellationToken])!;
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

        await PagedFetchAsync(fetchXml,
            expr => _asyncService.RetrieveMultipleAsync(expr),
            async (response, _) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                await onPage(ProjectEntities<TElement>(response.Entities.ToList(), query));
                return response.MoreRecords;
            });
    }

    // -------------------------------------------------------------------------
    // Async execution
    // -------------------------------------------------------------------------

    private async Task<List<TElement>> ExecuteQueryAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var entities = await RetrieveQueryAsync(query, cancellationToken);
        return ProjectEntities<TElement>(entities, query);
    }

    private async Task<TElement> ExecuteScalarAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var entities = await RetrieveQueryAsync(query, cancellationToken);
        return ApplyTerminalOperator(ProjectEntities<TElement>(entities, query), query.TerminalOperator);
    }

    private async Task<TElement> ExecuteAggregateAsync<TElement>(
        FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var entities = await RetrieveQueryAsync(query, cancellationToken);
        return ExtractAggregateResult<TElement>(entities, query.TerminalOperator);
    }

    private async Task<List<Entity>> RetrieveQueryAsync(FetchXmlQuery query, CancellationToken cancellationToken)
    {
        var results = new List<Entity>();
        var recordCountInvoked = false;

        await PagedFetchAsync(FetchXmlBuilder.Build(query),
            expr => _asyncService.RetrieveMultipleAsync(expr),
            async (response, _) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!recordCountInvoked)
                {
                    if (query.OnRecordCountAsync != null)
                        await query.OnRecordCountAsync(new RecordCountArguments(response.TotalRecordCount, response.TotalRecordCountLimitExceeded));
                    else
                        query.OnRecordCount?.Invoke(new RecordCountArguments(response.TotalRecordCount, response.TotalRecordCountLimitExceeded));
                    recordCountInvoked = true;
                }
                results.AddRange(response.Entities);
                return response.MoreRecords;
            });

        return results;
    }

    // -------------------------------------------------------------------------
    // Async paged retrieval
    // -------------------------------------------------------------------------

    private static Task PagedFetchAsync(
        string baseFetchXml,
        Func<FetchExpression, Task<EntityCollection>> retrieve,
        Func<EntityCollection, int, Task<bool>> onPage) =>
        PagedFetchAsyncCore(baseFetchXml, retrieve, onPage);

    private static Task PagedFetchAsync(
        string baseFetchXml,
        Func<FetchExpression, Task<EntityCollection>> retrieve,
        Func<EntityCollection, int, bool> onPage) =>
        PagedFetchAsyncCore(baseFetchXml, retrieve,
            (response, page) => Task.FromResult(onPage(response, page)));

    private static async Task PagedFetchAsyncCore(
        string baseFetchXml,
        Func<FetchExpression, Task<EntityCollection>> retrieve,
        Func<EntityCollection, int, Task<bool>> onPage)
    {
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

            var response = await retrieve(new FetchExpression(fetchDocument.ToString()));
            var shouldContinue = await onPage(response, pageNumber);

            if (explicitPage || !shouldContinue || !response.MoreRecords) break;

            pagingCookie = response.PagingCookie;
            pageNumber++;
        }
    }

    private static MethodInfo GetAsyncMethod(string name) =>
        typeof(DataverseQueryProviderAsync<T>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)!;
}
#endif

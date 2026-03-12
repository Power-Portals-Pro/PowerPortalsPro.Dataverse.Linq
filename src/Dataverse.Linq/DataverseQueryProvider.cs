using Dataverse.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
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

    // -------------------------------------------------------------------------
    // IQueryProvider
    // -------------------------------------------------------------------------

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Use the generic CreateQuery<TElement> overload.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) == typeof(T))
            return (IQueryable<TElement>)(object)new DataverseQueryable<T>(this, expression);

        return new DataverseProjectedQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression) => ExecuteList(expression);

    /// <summary>
    /// Synchronous execution — used by <see cref="DataverseProjectedQueryable{TElement}.GetEnumerator"/>.
    /// <typeparamref name="TResult"/> is typically <see cref="IEnumerable{T}"/> for projected types.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        var (selectColumns, projector) = SelectExpressionParser.Parse(expression);
        var columns = selectColumns ?? Columns;
        var entities = FetchListAsync(columns, CancellationToken.None).GetAwaiter().GetResult();

        if (projector is not null)
            return BuildProjectedList<TResult>(entities, projector);

        if (entities is TResult directResult)
            return directResult;

        throw new InvalidCastException($"Cannot convert List<{typeof(T).Name}> to {typeof(TResult).Name}.");
    }

    // -------------------------------------------------------------------------
    // IAsyncQueryProvider (EF Core)
    // Note: TResult is the Task itself (e.g. Task<List<TElement>>), not the
    // unwrapped value — this matches EF Core's interface contract.
    // -------------------------------------------------------------------------

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        var (selectColumns, projector) = SelectExpressionParser.Parse(expression);
        var columns = selectColumns ?? Columns;

        if (projector is null)
        {
            // TResult = Task<List<T>>
            return (TResult)(object)FetchListAsync(columns, cancellationToken);
        }

        // TResult = Task<List<TElement>> — extract TElement from Task<List<TElement>>
        var elementType = typeof(TResult).GetGenericArguments()[0].GetGenericArguments()[0];
        var method = typeof(DataverseQueryProvider<T>)
            .GetMethod(nameof(FetchProjectedListAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(elementType);

        return (TResult)method.Invoke(this, [columns, projector, cancellationToken])!;
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    internal List<T> ExecuteList(Expression expression) =>
        FetchListAsync(Columns, CancellationToken.None).GetAwaiter().GetResult();

    private async Task<List<T>> FetchListAsync(IReadOnlyList<string>? columns, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return entities.Select(e => e.ToEntity<T>()).ToList();
    }

    private async Task<List<TElement>> FetchProjectedListAsync<TElement>(
        IReadOnlyList<string>? columns, Delegate projector, CancellationToken cancellationToken)
    {
        var fetchXml = FetchXmlBuilder.Build(EntityLogicalName, columns);
        var entities = await RetrieveAllAsync(fetchXml, cancellationToken);
        return entities
            .Select(e => e.ToEntity<T>())
            .Select(e => (TElement)projector.DynamicInvoke(e)!)
            .ToList();
    }

    private static TResult BuildProjectedList<TResult>(List<T> entities, Delegate projector)
    {
        var elementType = typeof(TResult).GetGenericArguments()[0];
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;

        foreach (var entity in entities)
            list.Add(projector.DynamicInvoke(entity));

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

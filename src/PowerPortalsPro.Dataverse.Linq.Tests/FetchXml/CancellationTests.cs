#if !NETFRAMEWORK
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Verifies that a <see cref="CancellationToken"/> passed to an async query operator reaches
/// the service, so an in-flight RetrieveMultiple is cancelled rather than only being observed
/// once the page has already been fetched.
/// </summary>
public class CancellationTests
{
    private static IOrganizationServiceAsync2 CreateCancellableService()
    {
        EntityMetadataCache.Clear();
        return Substitute.For<IOrganizationServiceAsync2>();
    }

    [Fact]
    public async Task ToListAsync_ForwardsCancellationTokenToService()
    {
        var service = CreateCancellableService();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntityCollection()));

        using var cts = new CancellationTokenSource();
        await service.Queryable<CustomAccount>().ToListAsync(cts.Token);

        await service.Received(1).RetrieveMultipleAsync(Arg.Any<QueryBase>(), cts.Token);
    }

    [Fact]
    public async Task ToListAsync_WithAlreadyCancelledToken_ThrowsBeforeSendingRequest()
    {
        var service = CreateCancellableService();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new EntityCollection()));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await service.Queryable<CustomAccount>().ToListAsync(cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        await service.DidNotReceiveWithAnyArgs().RetrieveMultipleAsync(default!, default);
    }

    [Fact(Timeout = 30000)]
    public async Task ToListAsync_CancelledWhileRequestInFlight_ThrowsWithoutWaitingForThePage()
    {
        var service = CreateCancellableService();
        var requestStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                requestStarted.TrySetResult(true);
                // Only ever completes by cancellation — if the token were not forwarded to
                // the service this request would never return and the test would time out.
                await Task.Delay(Timeout.Infinite, call.Arg<CancellationToken>());
                return new EntityCollection();
            });

        using var cts = new CancellationTokenSource();
        var query = service.Queryable<CustomAccount>().ToListAsync(cts.Token);

        await requestStarted.Task;
        cts.Cancel();

        var act = async () => await query;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ForEachPageAsync_CancelledInPageCallback_StopsPagingAfterCurrentPage()
    {
        var service = CreateCancellableService();
        // Every page reports more records, so paging only ends when cancellation is observed.
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new EntityCollection { MoreRecords = true, PagingCookie = "<cookie />" }));

        using var cts = new CancellationTokenSource();
        var pages = 0;

        var act = async () => await service.Queryable<CustomAccount>().ForEachPageAsync(
            _ =>
            {
                pages++;
                cts.Cancel();
                return Task.CompletedTask;
            },
            cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        pages.Should().Be(1);
        await service.Received(1).RetrieveMultipleAsync(Arg.Any<QueryBase>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ToListAsync_WithServiceLackingCancellationSupport_StillExecutes()
    {
        EntityMetadataCache.Clear();
        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>())
            .Returns(Task.FromResult(new EntityCollection()));

        using var cts = new CancellationTokenSource();
        var results = await service.Queryable<CustomAccount>().ToListAsync(cts.Token);

        results.Should().BeEmpty();
        await service.Received(1).RetrieveMultipleAsync(Arg.Any<QueryBase>());
    }
}
#endif

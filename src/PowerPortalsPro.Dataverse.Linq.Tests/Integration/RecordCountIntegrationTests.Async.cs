using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class RecordCountIntegrationTests
{
    [Fact]
    public async Task ReturnRecordCountAsync_ReturnsResultsAndTotalCount()
    {
        RecordCountArguments? countArgs = null;

        var results = await Service.Queryable<CustomAccount>()
            .ReturnRecordCountAsync(async args =>
            {
                countArgs = args;
                await Task.CompletedTask;
            })
            .ToListAsync();

        results.Should().NotBeEmpty();
        countArgs.Should().NotBeNull();
        countArgs!.TotalRecordCount.Should().Be(results.Count);
    }

    [Fact]
    public async Task ReturnRecordCountAsync_WithFilter_ReturnsFilteredCountAndResults()
    {
        RecordCountArguments? countArgs = null;

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ReturnRecordCountAsync(async args =>
            {
                countArgs = args;
                await Task.CompletedTask;
            })
            .ToListAsync();

        results.Should().NotBeEmpty();
        countArgs.Should().NotBeNull();
        countArgs!.TotalRecordCount.Should().Be(results.Count);
    }

    [Fact]
    public async Task ReturnRecordCountAsync_WithSelect_ProjectsCorrectlyAndReturnsCount()
    {
        RecordCountArguments? countArgs = null;

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Select(a => new { a.Name })
            .ReturnRecordCountAsync(async args =>
            {
                countArgs = args;
                await Task.CompletedTask;
            })
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNull());
        countArgs.Should().NotBeNull();
        countArgs!.TotalRecordCount.Should().Be(results.Count);
    }

    [Fact]
    public async Task ReturnRecordCount_WithToListAsync_InvokesSyncCallback()
    {
        RecordCountArguments? countArgs = null;

        var results = await Service.Queryable<CustomAccount>()
            .ReturnRecordCount(args => countArgs = args)
            .ToListAsync();

        results.Should().NotBeEmpty();
        countArgs.Should().NotBeNull();
        countArgs!.TotalRecordCount.Should().Be(results.Count);
    }

    [Fact]
    public async Task ReturnRecordCountAsync_WithPageSize_ReturnsAllResultsAndTotalCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        RecordCountArguments? countArgs = null;

        var results = await Service.Queryable<CustomAccount>()
            .WithPageSize(10)
            .ReturnRecordCountAsync(async args =>
            {
                countArgs = args;
                await Task.CompletedTask;
            })
            .ToListAsync();

        results.Should().HaveCount(all.Count);
        countArgs.Should().NotBeNull();
        countArgs!.TotalRecordCount.Should().Be(all.Count);
    }
}

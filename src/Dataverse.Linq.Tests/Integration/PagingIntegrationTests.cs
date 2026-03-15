using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace Dataverse.Linq.Tests.Integration;

public class PagingIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // WithPageSize
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithPageSize_ReturnsAllRecords()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .WithPageSize(10)
            .ToListAsync();

        results.Should().HaveCount(all.Count);
    }

    [Fact]
    public async Task ToListAsync_WithPageSizeAndWhere_ReturnsAllFilteredRecords()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .WithPageSize(5)
            .ToListAsync();

        results.Should().HaveCount(all.Count);
        results.Should().OnlyContain(r => r.Name != null);
    }

    // -------------------------------------------------------------------------
    // ForEachPageAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ForEachPageAsync_ProcessesAllPages()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var collected = new List<CustomAccount>();
        var pageCount = 0;

        await Service.Queryable<CustomAccount>()
            .WithPageSize(10)
            .ForEachPageAsync(async page =>
            {
                pageCount++;
                collected.AddRange(page);
                await Task.CompletedTask;
            });

        collected.Should().HaveCount(all.Count);
        pageCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task ForEachPageAsync_WithWhereAndSelect_ProjectsCorrectly()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Select(a => new { a.Name })
            .ToListAsync();

        var collected = new List<string>();

        await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Select(a => new { a.Name })
            .WithPageSize(10)
            .ForEachPageAsync(async page =>
            {
                collected.AddRange(page.Select(p => p.Name));
                await Task.CompletedTask;
            });

        collected.Should().HaveCount(all.Count);
        collected.Should().OnlyContain(n => n != null);
    }

    [Fact]
    public async Task ForEachPageAsync_WithoutPageSize_ReturnsSinglePage()
    {
        var pageCount = 0;

        await Service.Queryable<CustomAccount>()
            .ForEachPageAsync(async page =>
            {
                pageCount++;
                page.Should().NotBeEmpty();
                await Task.CompletedTask;
            });

        // Without WithPageSize, Dataverse uses its default page size (5000)
        // so all 150 records come back in a single page
        pageCount.Should().Be(1);
    }

    // -------------------------------------------------------------------------
    // Take
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Take_LimitsResultCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var taken = await Service.Queryable<CustomAccount>().Take(5).ToListAsync();

        all.Count.Should().BeGreaterThan(5);
        taken.Should().HaveCount(5);
    }

    [Fact]
    public async Task Take_WithWhereAndOrderBy_ReturnsLimitedOrderedResults()
    {
        var taken = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Custom"))
            .OrderBy(a => a.Name)
            .Take(3)
            .ToListAsync();

        taken.Should().HaveCount(3);
        taken.Select(a => a.Name).Should().BeInAscendingOrder();
    }

    // -------------------------------------------------------------------------
    // WithPage
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithPageAndPageSize_ReturnsCorrectPage()
    {
        const int pageSize = 10;

        var all = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .ToListAsync();

        var page1 = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .WithPageSize(pageSize)
            .WithPage(1)
            .ToListAsync();

        var page2 = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .WithPageSize(pageSize)
            .WithPage(2)
            .ToListAsync();

        page1.Should().HaveCount(pageSize);
        page2.Should().HaveCount(pageSize);

        // Pages should contain different records
        page1.Select(a => a.Id).Should().NotIntersectWith(page2.Select(a => a.Id));

        // Page 1 should match the first N records from the full list
        page1.Select(a => a.Name).Should().Equal(all.Take(pageSize).Select(a => a.Name));

        // Page 2 should match the next N records
        page2.Select(a => a.Name).Should().Equal(all.Skip(pageSize).Take(pageSize).Select(a => a.Name));
    }

    // -------------------------------------------------------------------------
    // WithAggregateLimit
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithAggregateLimit_GroupByQuery_ReturnsResults()
    {
        var results = await Service.Queryable<CustomAccount>()
            .GroupBy(a => a.AccountRating)
            .Select(g => new { Rating = g.Key, Count = g.Count() })
            .WithAggregateLimit(10000)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Count.Should().BeGreaterThan(0));
        results.Sum(r => r.Count).Should().Be(150);
    }
}

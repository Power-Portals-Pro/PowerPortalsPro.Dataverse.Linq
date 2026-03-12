using Dataverse.Linq.Tests.ProxyClasses;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace Dataverse.Linq.Tests;

public class QueryableIntegrationTests : IntegrationTestBase
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // Basic retrieval
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_ReturnsAccounts()
    {
        var results = await Service.Queryable<Account>().ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(a => a.Id.Should().NotBe(Guid.Empty));
    }

    [Fact]
    public async Task ToListAsync_ReturnsAccountsWithAttributes()
    {
        var results = await Service.Queryable<Account>().ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(a => a.Attributes.Should().NotBeEmpty());
    }

    // -------------------------------------------------------------------------
    // Column selection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithExplicitColumns_OnlyReturnsRequestedColumns()
    {
        var results = await Service.Queryable<Account>("name").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(a =>
            a.Attributes.Keys.Should().OnlyContain(k => k == "name" || k == Account.PrimaryIdAttribute));
    }

    [Fact]
    public async Task ToListAsync_WithExplicitColumns_ReturnsFewerAttributesThanAllColumns()
    {
        var allColumns = await Service.Queryable<Account>().ToListAsync();
        var singleColumn = await Service.Queryable<Account>("name").ToListAsync();

        allColumns.Should().NotBeEmpty();
        singleColumn.Should().NotBeEmpty();
        allColumns[0].Attributes.Count.Should().BeGreaterThan(singleColumn[0].Attributes.Count);
    }

    // -------------------------------------------------------------------------
    // Select projection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithSelectProjection_ReturnsProjectedValues()
    {
        var results = await (from a in Service.Queryable<Account>()
                             select new { a.Name, a.Website }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(a =>
        {
            a.Should().NotBeNull();
            a.GetType().GetProperties().Should().Contain(p => p.Name == nameof(a.Name));
            a.GetType().GetProperties().Should().Contain(p => p.Name == nameof(a.Website));
        });
    }

    [Fact]
    public async Task ToListAsync_WithSelectProjection_MatchesExplicitColumnQuery()
    {
        var projected = await (from a in Service.Queryable<Account>()
                               select new { a.Name, a.Website }).ToListAsync();

        var explicit_ = await Service.Queryable<Account>("name", "websiteurl").ToListAsync();

        projected.Should().HaveSameCount(explicit_);
        projected.Select(p => p.Name).Should().BeEquivalentTo(explicit_.Select(e => e.Name));
    }

    [Fact]
    public async Task ToListAsync_WithSelectProjectionIncludingEntityReference_ReturnsProjectedValues()
    {
        var results = await (from a in Service.Queryable<Account>()
                             select new { a.Name, a.Website, a.PrimaryContact }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(a =>
        {
            a.Should().NotBeNull();
            a.GetType().GetProperties().Should().Contain(p => p.Name == nameof(a.Name));
            a.GetType().GetProperties().Should().Contain(p => p.Name == nameof(a.Website));
            a.GetType().GetProperties().Should().Contain(p => p.Name == nameof(a.PrimaryContact));
        });
    }
}

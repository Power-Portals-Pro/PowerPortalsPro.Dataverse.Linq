using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace Dataverse.Linq.Tests.Integration;

public class UnboundEntityIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // Unbound entity queries
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_ReturnsRecords()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Id.Should().NotBe(Guid.Empty));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_ReturnsRecordsWithAttributes()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Attributes.Should().NotBeEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_Returns150Records()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WithExplicitColumns_OnlyReturnsRequestedColumns()
    {
        var results = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Attributes.Keys.Should().OnlyContain(k => k == "new_name" || k == "new_customaccountid"));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WithExplicitColumns_ReturnsFewerAttributesThanAllColumns()
    {
        var allColumns = await Service.Queryable("new_customaccount").ToListAsync();
        var singleColumn = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        allColumns.Should().NotBeEmpty();
        singleColumn.Should().NotBeEmpty();
        allColumns[0].Attributes.Count.Should().BeGreaterThan(singleColumn[0].Attributes.Count);
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_MatchesTypedQueryResults()
    {
        var typed = await Service.Queryable<CustomAccount>("new_name").ToListAsync();
        var unbound = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        unbound.Should().HaveSameCount(typed);
        unbound.Select(e => e.GetAttributeValue<string>("new_name")).Should()
            .BeEquivalentTo(typed.Select(e => e.Name));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_CanAccessAttributesViaGetAttributeValue()
    {
        var results = await Service.Queryable("new_customaccount", "new_name", "new_website").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.GetAttributeValue<string>("new_name").Should().NotBeNullOrEmpty();
        });
    }

    // -------------------------------------------------------------------------
    // Where — unbound entity with GetAttributeValue
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereNotIsNullOrEmpty_ReturnsNonNullRecords()
    {
        var results = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r =>
            r.GetAttributeValue<string>("new_name").Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereIsNullOrEmpty_ReturnsNullRecords()
    {
        var all = await Service.Queryable("new_customaccount").ToListAsync();
        var nullDescriptions = all.Count(a => a.GetAttributeValue<string>("new_description") == null);

        var results = await Service.Queryable("new_customaccount")
            .Where(x => string.IsNullOrEmpty(x.GetAttributeValue<string>("new_description")))
            .ToListAsync();

        results.Should().HaveCount(nullDescriptions);
    }

    // -------------------------------------------------------------------------
    // Select — unbound entity with GetAttributeValue
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_SelectWithGetAttributeValue_ReturnsProjectedValues()
    {
        var results = await Service.Queryable("new_customaccount")
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name") })
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereAndSelect_ReturnsFilteredProjection()
    {
        var results = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_description")))
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name"), Description = x.GetAttributeValue<string>("new_description") })
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Name.Should().NotBeNullOrEmpty();
            r.Description.Should().NotBeNullOrEmpty();
        });
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_SelectMatchesTypedQuery()
    {
        var typed = await (from a in Service.Queryable<CustomAccount>()
                           select new { a.Name, a.Website }).ToListAsync();

        var unbound = await Service.Queryable("new_customaccount")
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name"), Website = x.GetAttributeValue<string>("new_website") })
            .ToListAsync();

        unbound.Should().HaveSameCount(typed);
        unbound.Select(u => u.Name).Should().BeEquivalentTo(typed.Select(t => t.Name));
    }
}

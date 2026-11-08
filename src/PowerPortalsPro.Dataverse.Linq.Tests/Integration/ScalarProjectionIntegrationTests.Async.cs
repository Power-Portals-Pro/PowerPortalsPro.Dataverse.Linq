using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class ScalarProjectionIntegrationTests
{
    // -------------------------------------------------------------------------
    // Distinct over a single value
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        var results = await Service.Queryable("new_customcontact")
            .Select(c => c.GetAttributeValue<string>("new_lastname"))
            .Distinct()
            .ToListAsync();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public async Task ToListAsync_UnboundSelectIndexer_Distinct_ReturnsEachValueOnce()
    {
        var results = await Service.Queryable("new_customcontact")
            .Select(c => (string)c["new_lastname"])
            .Distinct()
            .ToListAsync();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public async Task ToListAsync_TypedSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        var results = await Service.Queryable<CustomContact>()
            .Select(c => c.GetAttributeValue<string>("new_lastname"))
            .Distinct()
            .ToListAsync();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public async Task ToListAsync_UnboundSelectLookup_Distinct_ReturnsEachParentOnce()
    {
        var contacts = await Service.Queryable("new_customcontact", "new_parentaccount").ToListAsync();
        var expected = contacts
            .Select(c => c.GetAttributeValue<EntityReference>("new_parentaccount")?.Id)
            .Distinct()
            .ToList();

        var results = await Service.Queryable("new_customcontact")
            .Select(c => (Guid?)c.GetAttributeValue<EntityReference>("new_parentaccount").Id)
            .Distinct()
            .ToListAsync();

        results.Should().HaveCount(expected.Count).And.OnlyHaveUniqueItems();
        results.Should().BeEquivalentTo(expected);
    }

    // -------------------------------------------------------------------------
    // Entity.Id as the whole projection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundSelectId_ReturnsEveryRecordId()
    {
        var expected = (await Service.Queryable("new_customaccount", "new_name").ToListAsync()).Select(a => a.Id);

        var results = await Service.Queryable("new_customaccount")
            .Select(a => a.Id)
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().NotContain(Guid.Empty);
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ToListAsync_UnboundSelectId_Distinct_ReturnsEveryRecordId()
    {
        var results = await Service.Queryable("new_customaccount")
            .Select(a => a.Id)
            .Distinct()
            .ToListAsync();

        results.Should().HaveCount(150).And.OnlyHaveUniqueItems();
        results.Should().NotContain(Guid.Empty);
    }

    [Fact]
    public async Task ToListAsync_TypedSelectId_ReturnsEveryRecordId()
    {
        var expected = (await Service.Queryable<CustomAccount>().ToListAsync()).Select(a => a.Id);

        var results = await Service.Queryable<CustomAccount>()
            .Select(a => a.Id)
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_UnboundSelectNullableId_ReturnsMatchingId()
    {
        var existing = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        var result = await Service.Queryable("new_customaccount")
            .Where(a => a.GetAttributeValue<string>("new_name") == "Custom Account 001")
            .Select(a => (Guid?)a.Id)
            .FirstOrDefaultAsync();

        result.Should().Be(existing.Id);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_UnboundSelectNullableId_NoMatch_ReturnsNull()
    {
        var result = await Service.Queryable("new_customaccount")
            .Where(a => a.GetAttributeValue<string>("new_name") == "NonExistent Account ZZZ")
            .Select(a => (Guid?)a.Id)
            .FirstOrDefaultAsync();

        result.Should().BeNull();
    }

    // -------------------------------------------------------------------------
    // A select directly on another
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_SelectThenSelect_ReturnsTheSecondProjection()
    {
        var expected = (await Service.Queryable<CustomAccount>().Select(a => a.Name).ToListAsync())
            .Select(n => n.ToUpper());

        var results = await Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Select(x => x.Name.ToUpper())
            .ToListAsync();

        results.Should().BeEquivalentTo(expected);
    }

    // -------------------------------------------------------------------------
    // Values computed from attributes
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_TypedSelectComputedMembers_ReturnsValuesFromEveryAttributeRead()
    {
        var expected = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees != null)
            .Select(a => new { a.Name, a.Website, a.NumberOfEmployees })
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees != null)
            .Select(a => new
            {
                a.Name,
                Website = a.Website.ToUpper(),
                Employees = a.NumberOfEmployees + 1,
            })
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().BeEquivalentTo(expected.Select(e => new
        {
            e.Name,
            Website = e.Website.ToUpper(),
            Employees = e.NumberOfEmployees + 1,
        }));
    }
}

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Selects of a single value (<c>GetAttributeValue</c>, the indexer, <c>Id</c>) and of values
/// computed from attributes. The seeded data has 500 contacts whose last names are
/// <c>Last1</c> to <c>Last5</c>, and 150 accounts that each have a website.
/// </summary>
public partial class ScalarProjectionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private static readonly string[] LastNames = ["Last1", "Last2", "Last3", "Last4", "Last5"];

    [Fact]
    public void UnboundSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        var results = Service.Queryable("new_customcontact")
            .Select(c => c.GetAttributeValue<string>("new_lastname"))
            .Distinct()
            .ToList();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public void UnboundSelectId_ReturnsEveryRecordId()
    {
        var expected = Service.Queryable("new_customaccount", "new_name").ToList().Select(a => a.Id);

        var results = Service.Queryable("new_customaccount")
            .Select(a => a.Id)
            .ToList();

        results.Should().HaveCount(150);
        results.Should().NotContain(Guid.Empty);
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void TypedSelectComputedMembers_ReturnsValuesFromEveryAttributeRead()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website, Upper = a.Website.ToUpper() })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r =>
        {
            r.Website.Should().NotBeNullOrEmpty();
            r.Upper.Should().Be(r.Website.ToUpper());
        });
    }

    [Fact]
    public void UnboundSelectGetAttributeValue_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList().Max(a => a.NumberOfEmployees);

        var result = Service.Queryable("new_customaccount")
            .Select(a => a.GetAttributeValue<int?>("new_numberofemployees"))
            .Max();

        result.Should().Be(expected);
    }
}

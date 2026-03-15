using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using XrmToolkit.Dataverse.Linq.Extensions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class SpecialFilterIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // DateTime fiscal extensions
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_InFiscalYear_ReturnsMatchingRecords()
    {
        // Use a year we know has Won opportunities from seed data
        var knownYear = Service.Queryable<CustomOpportunity>()
            .Where(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won)
            .ToList()
            .Where(o => o.ActualCloseDate.HasValue)
            .Select(o => o.ActualCloseDate!.Value.Year)
            .First();

        var results = Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualCloseDate.InFiscalYear(knownYear))
            .ToList();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public void Where_InFiscalPeriodAndYear_ReturnsMatchingRecords()
    {
        // Use a year/period we know has data
        var knownDate = Service.Queryable<CustomOpportunity>()
            .Where(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won)
            .ToList()
            .Where(o => o.ActualCloseDate.HasValue)
            .Select(o => o.ActualCloseDate!.Value)
            .First();

        var quarter = (knownDate.Month - 1) / 3 + 1;

        var results = Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualCloseDate.InFiscalPeriodAndYear(quarter, knownDate.Year))
            .ToList();

        results.Should().NotBeEmpty();
    }
}

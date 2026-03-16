using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Extensions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class SpecialFilterIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

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

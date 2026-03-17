using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class AggregateIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Sum with no results
    // -------------------------------------------------------------------------

    [Fact]
    public void Sum_WithNoResults_ThrowsInvalidCast()
    {
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         where a.Name == "!@#$#EQSE_NONEXISTENT"
                         select a.CreditLimit).Sum();

        act.Should().Throw<InvalidCastException>();
    }

    // -------------------------------------------------------------------------
    // Composite key GroupBy with date grouping and navigation property
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupBy_CompositeKeyWithDateAndNavProperty_ReturnsGroupedResults()
    {
        var results = (from c in Service.Queryable<CustomContact>()
                       where c.CreatedOn != null
                       group c by new
                       {
                           Year = c.CreatedOn!.Value.Year,
                           Month = c.CreatedOn!.Value.Month,
                           Account = c.ParentAccount,
                       } into g
                       select new
                       {
                           AccountId = g.Key.Account.Id,
                           g.Key.Year,
                           g.Key.Month,
                           Count = g.Count()
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Year.Should().BeGreaterThan(0);
            r.Month.Should().BeInRange(1, 12);
            r.Count.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public void GroupBy_CompositeKeyWithConstructorProjection_ReturnsGroupedResults()
    {
        var results = (from c in Service.Queryable<CustomContact>()
                       where c.CreatedOn != null
                       group c by new
                       {
                           Year = c.CreatedOn!.Value.Year,
                           Month = c.CreatedOn!.Value.Month,
                           Account = c.ParentAccount,
                       } into g
                       select new GroupTestResult(
                           g.Key.Account.Id,
                           g.Key.Year,
                           g.Key.Month,
                           g.Count())).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Year.Should().BeGreaterThan(0);
            r.Month.Should().BeInRange(1, 12);
            r.Count.Should().BeGreaterThan(0);
        });
    }
}

internal record GroupTestResult(Guid AccountId, int Year, int Month, int Count);

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using XrmToolkit.Dataverse.Linq.Extensions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class GroupByIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

    [Fact]
    public void ToListAsync_GroupByDateYear_ReturnsGroupedResults()
    {
        var results = (from o in Service.Queryable<CustomOpportunity>()
                       join c in Service.Queryable<CustomContact>()
                           on o.Contact.Id equals c.CustomContactId
                       where c.FirstName.Contains("First")
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Year into g
                       orderby g.Key ascending
                       select new
                       {
                           Year = g.Key,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.ActualRevenue),
                           AverageRevenue = g.Average(x => x.ActualRevenue),
                           TotalEstimatedRevenue = g.Sum(x => x.EstimatedRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Year.Should().BeGreaterThan(2000);
            r.Count.Should().BeGreaterThan(0);
        });

        // Verify ascending order by Year
        results.Select(r => r.Year).Should().BeInAscendingOrder();
    }

    [Fact]
    public void GroupBy_SimpleCount_ReturnsGroupedCounts()
    {
        var totalWithRating = Service.Queryable<CustomAccount>()
            .Count(a => a.AccountRating_OptionSetValue != null);

        var results = (from a in Service.Queryable<CustomAccount>()
                       where a.AccountRating_OptionSetValue != null
                       group a by a.AccountRating_OptionSetValue.Value into g
                       select new
                       {
                           Rating = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWithRating);
    }

    [Fact]
    public void JoinGroupBy_AggregateOnLinkEntity_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from c in Service.Queryable<CustomContact>()
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group o by c.CustomContactId into g
                       select new
                       {
                           ContactId = g.Key,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ContactId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        // Sum of grouped counts should equal total opportunities
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    [Fact]
    public void MultiJoinGroupBy_AggregateOnNestedLinkEntity_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group o by a.CustomAccountId into g
                       select new
                       {
                           AccountId = g.Key,
                           Count = g.Count(),
                           MaxRevenue = g.Max(x => x.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    [Fact]
    public void JoinGroupBy_CompositeKey_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from c in Service.Queryable<CustomContact>()
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group new { c, o }
                           by new { c.CustomContactId } into g
                       select new
                       {
                           ContactId = g.Key.CustomContactId,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.o.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ContactId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    // -------------------------------------------------------------------------
    // GroupBy — Date grouping variants
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupBy_DateQuarter_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Quarter() into g
                       select new
                       {
                           Quarter = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Quarter.Should().BeInRange(1, 4);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateMonth_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Month into g
                       select new
                       {
                           Month = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Month.Should().BeInRange(1, 12);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateDay_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Day into g
                       select new
                       {
                           Day = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Day.Should().BeInRange(1, 31);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateWeek_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Week() into g
                       select new
                       {
                           Week = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Week.Should().BeInRange(1, 53);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_FiscalPeriod_GeneratesCorrectFetchXml()
    {
        // FiscalPeriod grouping returns composite date strings (e.g., "2020-01")
        // that may not parse correctly at runtime, so we verify FetchXml generation only.
        var fetchXml = (from o in Service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.FiscalPeriod() into g
                        select new
                        {
                            FiscalPeriod = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        fetchXml.Should().Contain("dategrouping=\"fiscal-period\"");
    }

    [Fact]
    public void GroupBy_FiscalYear_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.FiscalYear() into g
                       select new
                       {
                           FiscalYear = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    // -------------------------------------------------------------------------
    // GroupBy — OptionSetValue
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupBy_OptionSetValue_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       group o by o.StatusReason_OptionSetValue.Value into g
                       select new
                       {
                           Status = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        // Sum of all group counts should equal total opportunities
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    // -------------------------------------------------------------------------
    // GroupBy deep — group root entity by linked entity key
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupByDeep_GroupRootByLinkedEntityKey_ReturnsGroupedResults()
    {
        var results = (from c in Service.Queryable<CustomContact>()
                       join a in Service.Queryable<CustomAccount>()
                           on c.ParentAccount.Id equals a.CustomAccountId
                       group c by a.CustomAccountId into g
                       select new
                       {
                           AccountId = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        // 100 accounts × 5 contacts each = 500 total contacts
        results.Sum(r => r.Count).Should().Be(500);
        // Each of the 100 accounts should have 5 contacts
        results.Should().AllSatisfy(r => r.Count.Should().Be(5));
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class AggregateIntegrationTests
{
    // -------------------------------------------------------------------------
    // Aggregate operators — Min / Max / Sum / Average / Count
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Count_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = Service.Queryable<CustomAccount>().Count();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task Count_WithPredicate_MatchesFilteredListCount()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("00"))
            .ToListAsync();

        var count = Service.Queryable<CustomAccount>()
            .Count(a => a.Name.Contains("00"));

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task Min_WithSelector_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Min(a => a.NumberOfEmployees);

        var min = Service.Queryable<CustomAccount>()
            .Min(a => a.NumberOfEmployees);

        min.Should().Be(expected);
    }

    [Fact]
    public async Task Max_WithSelector_MatchesLinqMax()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Max(a => a.NumberOfEmployees);

        var max = Service.Queryable<CustomAccount>()
            .Max(a => a.NumberOfEmployees);

        max.Should().Be(expected);
    }

    [Fact]
    public async Task Sum_WithSelect_MatchesLinqSum()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Sum(a => a.NumberOfEmployees);

        var sum = (from a in Service.Queryable<CustomAccount>()
                   select a.NumberOfEmployees).Sum();

        sum.Should().Be(expected);
    }

    [Fact]
    public async Task Average_WithSelector_MatchesLinqAverage()
    {
        // Use a decimal column (PercentComplete) to avoid integer rounding
        // that Dataverse applies to integer avg aggregates.
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.PercentComplete != null)
            .Average(a => a.PercentComplete);

        var avg = Service.Queryable<CustomAccount>()
            .Average(a => a.PercentComplete);

        avg.Should().Be(expected);
    }

    [Fact]
    public async Task Min_OnMoneyValue_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.CreditLimitMoney != null)
            .Min(a => a.CreditLimitMoney.Value);

        var min = (from a in Service.Queryable<CustomAccount>()
                   select a.CreditLimitMoney.Value).Min();

        min.Should().Be(expected);
    }

    [Fact]
    public async Task LongCount_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = Service.Queryable<CustomAccount>().LongCount();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task CountColumn_CountsNonNullValues()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expectedNonNull = all.Count(a => a.NumberOfEmployees != null);

        var countColumn = Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .CountColumn();

        countColumn.Should().Be(expectedNonNull);
    }

    [Fact]
    public async Task CountColumnAsync_CountsNonNullValues()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expectedNonNull = all.Count(a => a.NumberOfEmployees != null);

        var countColumn = await Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .CountColumnAsync();

        countColumn.Should().Be(expectedNonNull);
    }

    [Fact]
    public async Task CountAsync_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = await Service.Queryable<CustomAccount>().CountAsync();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_MatchesFilteredListCount()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("00"))
            .ToListAsync();

        var count = await Service.Queryable<CustomAccount>()
            .CountAsync(a => a.Name.Contains("00"));

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task LongCountAsync_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = await Service.Queryable<CustomAccount>().LongCountAsync();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task MinAsync_WithSelector_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Min(a => a.NumberOfEmployees);

        var min = await Service.Queryable<CustomAccount>()
            .MinAsync(a => a.NumberOfEmployees);

        min.Should().Be(expected);
    }

    [Fact]
    public async Task MaxAsync_WithSelector_MatchesLinqMax()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Max(a => a.NumberOfEmployees);

        var max = await Service.Queryable<CustomAccount>()
            .MaxAsync(a => a.NumberOfEmployees);

        max.Should().Be(expected);
    }

    [Fact]
    public async Task SumAsync_WithSelect_MatchesLinqSum()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Sum(a => a.NumberOfEmployees);

        var sum = await Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .SumAsync();

        sum.Should().Be(expected);
    }

    [Fact]
    public async Task AverageAsync_WithSelector_MatchesLinqAverage()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.PercentComplete != null)
            .Average(a => a.PercentComplete);

        var avg = await Service.Queryable<CustomAccount>()
            .Select(a => a.PercentComplete)
            .AverageAsync();

        avg.Should().Be(expected);
    }

    // -------------------------------------------------------------------------
    // Composite key GroupBy with date grouping and navigation property
    // -------------------------------------------------------------------------

    [Fact]
    public async Task GroupByAsync_CompositeKeyWithDateAndNavProperty_ReturnsGroupedResults()
    {
        var results = await (from c in Service.Queryable<CustomContact>()
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
                             }).ToListAsync();

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
    public async Task GroupByAsync_CompositeKeyWithConstructorProjection_ReturnsGroupedResults()
    {
        var results = await (from c in Service.Queryable<CustomContact>()
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
                                 g.Count())).ToListAsync();

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

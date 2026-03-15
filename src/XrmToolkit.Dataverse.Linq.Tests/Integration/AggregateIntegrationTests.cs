using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public class AggregateIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

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
}

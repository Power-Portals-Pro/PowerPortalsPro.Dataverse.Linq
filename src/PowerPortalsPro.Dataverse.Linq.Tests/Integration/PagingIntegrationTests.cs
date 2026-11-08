using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class PagingIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Take — the first rows of the query as it stands
    // -------------------------------------------------------------------------

    [Fact]
    public void TakeAfterTake_TakesTheSmallerCount()
    {
        var results = Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .Take(5)
            .Take(20)
            .ToList();

        results.Select(a => a.Name).Should().Equal(
            "Custom Account 001", "Custom Account 002", "Custom Account 003", "Custom Account 004", "Custom Account 005");
    }

    [Fact]
    public void SingleAfterTakeOne_ReturnsTheRowTaken()
    {
        var result = Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .Take(1)
            .Single();

        result.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public void WhereAfterTake_Throws()
    {
        // LINQ finds no Empty Account among the first five; Dataverse would filter, then take five.
        var act = () => Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .Take(5)
            .Where(a => a.Name.StartsWith("Empty Account"))
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    // Dataverse would apply each to every row, then take the first five.
    public static TheoryData<string, Func<IQueryable<CustomAccount>, IQueryable<CustomContact>, object?>> OperatorsAfterTake => new()
    {
        { "OrderBy", (taken, _) => taken.OrderBy(a => a.Website).ToList() },
        { "GroupBy", (taken, _) => taken.GroupBy(a => a.AccountRating).Select(g => new { g.Key, Count = g.Count() }).ToList() },
        { "Join", (taken, contacts) => taken.Join(contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a.Name, c.LastName }).ToList() },
        { "FirstOrDefault with a predicate", (taken, _) => taken.FirstOrDefault(a => a.Name.StartsWith("Empty Account")) },
    };

    [Theory]
    [MemberData(nameof(OperatorsAfterTake))]
    public void OperatorAfterTake_Throws(
        string operation, Func<IQueryable<CustomAccount>, IQueryable<CustomContact>, object?> query)
    {
        var taken = Service.Queryable<CustomAccount>().OrderBy(a => a.Name).Take(5);

        var act = () => query(taken, Service.Queryable<CustomContact>());

        act.Should().Throw<NotSupportedException>(operation).WithMessage("*after Take()*");
    }

    [Fact]
    public void CountAfterTake_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>().Take(5).Count();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectAfterTake_ReturnsTheValuesOfTheRowsTaken()
    {
        var results = Service.Queryable<CustomAccount>()
            .OrderByDescending(a => a.Name)
            .Take(3)
            .Select(a => a.Name)
            .ToList();

        results.Should().Equal("Empty Account 050", "Empty Account 049", "Empty Account 048");
    }
}

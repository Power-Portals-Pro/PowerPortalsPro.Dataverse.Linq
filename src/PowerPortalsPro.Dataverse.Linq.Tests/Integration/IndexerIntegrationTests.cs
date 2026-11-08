using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// The indexer, <c>entity["name"]</c>, in filters, orders, join keys and aggregates. The seeded
/// data has 100 "Custom Account" rows with 5 contacts each and 50 "Empty Account" rows with none.
/// </summary>
public partial class IndexerIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private IQueryable<Entity> Accounts => Service.Queryable("new_customaccount");

    private IQueryable<Entity> Contacts => Service.Queryable("new_customcontact");

    [Fact]
    public void WhereIndexerEquals_ReturnsTheMatch()
    {
        var results = Accounts.Where(a => (string)a["new_name"] == "Custom Account 001").ToList();

        results.Should().ContainSingle()
            .Which.GetAttributeValue<string>("new_name").Should().Be("Custom Account 001");
    }

    [Fact]
    public void WhereIndexerAsString_ReturnsTheMatch()
    {
        var results = Accounts.Where(a => (a["new_name"] as string) == "Empty Account 001").ToList();

        results.Should().ContainSingle()
            .Which.GetAttributeValue<string>("new_name").Should().Be("Empty Account 001");
    }

    [Fact]
    public void WhereNarrowedColumnValue_Throws()
    {
        var act = () => Accounts
            .Where(a => ((int?)a.GetAttributeValue<decimal?>("new_percentcomplete")).Value == 3)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void WhereIndexerIsNull_MatchesGetAttributeValue()
    {
        var expected = Accounts.Where(a => a.GetAttributeValue<string>("new_description") == null).Count();

        var count = Accounts.Where(a => a["new_description"] == null).Count();

        count.Should().Be(expected).And.BeGreaterThan(0);
    }

    [Fact]
    public void WhereIndexerStartsWith_ReturnsTheMatches()
    {
        var results = Accounts.Where(a => ((string)a["new_name"]).StartsWith("Empty Account")).ToList();

        results.Should().HaveCount(50);
    }

    [Fact]
    public void WhereIndexerGreaterThan_MatchesGetAttributeValue()
    {
        var expected = Accounts.Where(a => a.GetAttributeValue<int?>("new_numberofemployees") > 500).Count();

        var count = Accounts.Where(a => (int?)a["new_numberofemployees"] > 500).Count();

        count.Should().Be(expected).And.BeGreaterThan(0);
    }

    [Fact]
    public void WhereIndexerLookupId_ReturnsTheChildren()
    {
        var parent = Accounts.First(a => (string)a["new_name"] == "Custom Account 001");

        var results = Contacts.Where(c => ((EntityReference)c["new_parentaccount"]).Id == parent.Id).ToList();

        results.Should().HaveCount(5);
    }

    [Fact]
    public void WhereIndexerOptionSetValue_ReturnsTheActiveRows()
    {
        var count = Accounts.Where(a => ((OptionSetValue)a["statecode"]).Value == 0).Count();

        count.Should().Be(150);
    }

    [Fact]
    public void WhereListContainsIndexer_ReturnsTheListed()
    {
        var names = new[] { "Custom Account 001", "Empty Account 001" };

        var results = Accounts.Where(a => names.Contains((string)a["new_name"])).ToList();

        results.Select(a => a.GetAttributeValue<string>("new_name")).Should().BeEquivalentTo(names);
    }

    [Fact]
    public void OrderByIndexer_OrdersLikeGetAttributeValue()
    {
        var expected = Accounts
            .OrderByDescending(a => a.GetAttributeValue<string>("new_name"))
            .Select(a => a.GetAttributeValue<string>("new_name"))
            .ToList();

        var results = Accounts
            .OrderByDescending(a => a["new_name"])
            .Select(a => (string)a["new_name"])
            .ToList();

        results.Should().Equal(expected);
    }

    [Fact]
    public void JoinOnIndexerKey_ReturnsEveryPair()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.Id equals ((EntityReference)c["new_parentaccount"]).Id
                       select new { Name = (string)a["new_name"], LastName = (string)c["new_lastname"] }).ToList();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r => r.Name.Should().StartWith("Custom Account"));
    }

    [Fact]
    public void MaxOfIndexer_MatchesGetAttributeValue()
    {
        var expected = Accounts.Max(a => a.GetAttributeValue<int?>("new_numberofemployees"));

        var result = Accounts.Max(a => (int?)a["new_numberofemployees"]);

        result.Should().Be(expected).And.NotBeNull();
    }

    [Fact]
    public void CountWithIndexerPredicate_CountsTheMatches()
    {
        var count = Accounts.Count(a => ((string)a["new_name"]).StartsWith("Custom Account"));

        count.Should().Be(100);
    }
}

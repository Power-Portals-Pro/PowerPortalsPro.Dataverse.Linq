using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// After a join's select that keeps one of its entities, an operator reads that entity whatever
/// it names its parameter. The seeded data has 100 "Custom Account NNN" rows with 5 contacts each,
/// last names Last1 to Last5, and 100 opportunities on the first 100 contacts.
/// </summary>
public partial class KeptJoinEntityIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private IQueryable<CustomContact> Contacts =>
        from a in Service.Queryable<CustomAccount>()
        join c in Service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id
        select c;

    private IQueryable<CustomAccount> Accounts =>
        from a in Service.Queryable<CustomAccount>()
        join c in Service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id
        select a;

    private IQueryable<CustomContact> LeftJoinedContacts =>
        from a in Service.Queryable<CustomAccount>()
        join c in Service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id into contacts
        from c in contacts.DefaultIfEmpty()
        select c;

    [Fact]
    public void WhereOnKeptInnerEntityByAnotherName_FiltersTheContacts()
    {
        var results = Contacts.Where(x => x.LastName == "Last1").ToList();

        results.Should().HaveCount(100);
        results.Should().AllSatisfy(c => c.LastName.Should().Be("Last1"));
    }

    [Fact]
    public void WhereOnKeptRootEntityNamedForTheJoinedOne_FiltersTheAccounts()
    {
        var results = Accounts.Where(c => c.Name == "Custom Account 001").ToList();

        results.Should().HaveCount(5, "the account is joined to each of its five contacts");
        results.Should().AllSatisfy(a => a.Name.Should().Be("Custom Account 001"));
    }

    [Fact]
    public void OrderByKeptInnerEntityByAnotherName_OrdersTheContacts()
    {
        var results = Contacts.OrderByDescending(x => x.LastName).Take(5).ToList();

        results.Should().AllSatisfy(c => c.LastName.Should().Be("Last5"));
    }

    [Fact]
    public void WhereOnKeptLeftJoinedEntityByAnotherName_FiltersTheContacts()
    {
        var results = LeftJoinedContacts.Where(x => x.LastName == "Last2").ToList();

        results.Should().HaveCount(100);
        results.Should().AllSatisfy(c => c.LastName.Should().Be("Last2"));
    }

    [Fact]
    public void SelectAfterTakeOnKeptInnerEntity_ReadsTheContacts()
    {
        var results = Contacts.OrderBy(x => x.LastName).Take(3).Select(x => x.LastName).ToList();

        results.Should().Equal("Last1", "Last1", "Last1");
    }

    [Fact]
    public void GroupByKeptInnerEntityByAnotherName_GroupsTheContacts()
    {
        var results = Contacts
            .GroupBy(x => x.LastName)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToList();

        results.Should().BeEquivalentTo(
            Enumerable.Range(1, 5).Select(i => new { Key = $"Last{i}", Count = 100 }));
    }

    [Fact]
    public void MaxOfKeptRootEntityNamedForTheJoinedOne_AggregatesTheAccounts()
    {
        var expected = Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom Account"))
            .Max(a => a.NumberOfEmployees);

        var result = Accounts.Max(c => c.NumberOfEmployees);

        result.Should().Be(expected).And.NotBeNull();
    }

    [Fact]
    public void JoinOnKeptInnerEntityByAnotherName_MatchesTheJoinNamedForIt()
    {
        var expected = (from c in Contacts
                        join o in Service.Queryable<CustomOpportunity>() on c.CustomContactId equals o.Contact.Id
                        select new { c.LastName, o.Name }).ToList();

        var results = (from x in Contacts
                       join o in Service.Queryable<CustomOpportunity>() on x.CustomContactId equals o.Contact.Id
                       select new { x.LastName, o.Name }).ToList();

        results.Should().HaveCount(100);
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void JoinOnKeptInnerEntityReusingItsAlias_ReadsEachEntity()
    {
        // contacts joined its own c; here c is the opportunity.
        var results = (from x in Contacts
                       join c in Service.Queryable<CustomOpportunity>() on x.CustomContactId equals c.Contact.Id
                       where c.Name == "Opportunity 001"
                       select new { x.LastName, Opportunity = c.Name }).ToList();

        results.Should().ContainSingle()
            .Which.Opportunity.Should().Be("Opportunity 001");
        results[0].LastName.Should().StartWith("Last");
    }

    [Fact]
    public void CountChildrenOnKeptInnerJoinedRoot_CountsTheChildAccounts()
    {
        // Accounts 001 to 010 each parent four others; 001 is joined to each of its five contacts.
        var results = Accounts
            .Where(x => x.Name == "Custom Account 001")
            .Select(x => new { x.Name, Children = x.CountChildren() })
            .ToList();

        results.Should().HaveCount(5);
        results.Should().AllSatisfy(r => r.Children.Should().Be(4));
    }

    [Fact]
    public void SelectAfterDistinctOnKeptLeftJoinedRoot_ReturnsEachAccountOnce()
    {
        var accounts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select a;

        var results = accounts.Distinct().Select(x => x.Name).ToList();

        results.Should().HaveCount(150).And.OnlyHaveUniqueItems();
    }
}

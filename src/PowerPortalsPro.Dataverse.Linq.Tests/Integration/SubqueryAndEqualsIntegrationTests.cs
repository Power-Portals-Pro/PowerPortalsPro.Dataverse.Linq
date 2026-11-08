using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// <c>Equals</c> in a filter, subqueries on a joined entity or under <c>||</c>, and aggregates
/// over ordered rows. The seeded data has 100 "Custom Account NNN" rows with 5 contacts each, 50
/// "Empty Account NNN" rows with none, and 100 opportunities, one on each of the first 100
/// contacts.
/// </summary>
public partial class SubqueryAndEqualsIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private IQueryable<CustomAccount> Accounts => Service.Queryable<CustomAccount>();

    private IQueryable<CustomContact> Contacts => Service.Queryable<CustomContact>();

    private IQueryable<CustomOpportunity> Opportunities => Service.Queryable<CustomOpportunity>();

    // -------------------------------------------------------------------------
    // Equals
    // -------------------------------------------------------------------------

    [Fact]
    public void WhereStringEquals_ReturnsTheMatch()
    {
        var results = Accounts.Where(a => a.Name.Equals("Custom Account 001")).ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public void WhereNotEquals_ReturnsTheOthers()
    {
        var count = Accounts.Count(a => !a.Name.Equals("Custom Account 001"));

        count.Should().Be(149);
    }

    [Fact]
    public void WhereIndexerEquals_ReturnsTheMatch()
    {
        var results = Service.Queryable("new_customaccount")
            .Where(a => a["new_name"].Equals("Empty Account 001"))
            .ToList();

        results.Should().ContainSingle()
            .Which.GetAttributeValue<string>("new_name").Should().Be("Empty Account 001");
    }

    [Fact]
    public void WhereValueTypeEquals_ReturnsTheMatch()
    {
        var results = Accounts.Where(a => a.NumberOfEmployees.Equals(20)).ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Custom Account 002");
    }

    // -------------------------------------------------------------------------
    // Subqueries
    // -------------------------------------------------------------------------

    [Fact]
    public void AnyOnInnerJoinedEntity_ReturnsTheContactsWithAnOpportunity()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                       where Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                       select new { a.Name, c.CustomContactId }).ToList();

        var expected = Opportunities.Select(o => o.Contact.Id).ToList();

        results.Should().HaveCount(100);
        results.Select(r => r.CustomContactId).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void ExistsOnInnerJoinedEntity_ReturnsTheContactsWithAnOpportunity()
    {
        var count = (from a in Accounts
                     join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                     where Opportunities.Exists(o => o.Contact.Id == c.CustomContactId)
                     select c.LastName).ToList().Count;

        count.Should().Be(100);
    }

    [Fact]
    public void ExistsUnderOr_ReturnsEitherMatch()
    {
        // 100 accounts have contacts; Empty Account 001 has none but matches by name.
        var count = Accounts
            .Count(a => a.Name == "Empty Account 001" || Contacts.Exists(c => c.ParentAccount.Id == a.CustomAccountId));

        count.Should().Be(101);
    }

    [Fact]
    public void SubqueryNamedLikeAJoin_ReadsTheJoinedContacts()
    {
        var count = Accounts
            .Join(Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
            .Where(x => Opportunities.Any(c => c.Contact.Id == x.c.CustomContactId))
            .Select(x => x.c.LastName)
            .ToList()
            .Count;

        count.Should().Be(100);
    }

    [Fact]
    public void WhereEqualsWithCaseSensitiveComparison_Throws()
    {
        var act = () => Accounts.Where(a => a.Name.Equals("custom account 001", StringComparison.Ordinal)).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void WhereAfterWhereWithOr_AndsThePredicates()
    {
        // Custom Account 002 has 20 employees, 001 has 10.
        var results = Accounts
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Custom Account 002")
            .Where(a => a.NumberOfEmployees > 15)
            .ToList();

        results.Should().ContainSingle().Which.Name.Should().Be("Custom Account 002");
    }

    [Fact]
    public void ExistsAfterWhereWithOr_AndsThePredicates()
    {
        var count = Accounts
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Empty Account 001")
            .Count(a => Contacts.Exists(c => c.ParentAccount.Id == a.CustomAccountId));

        count.Should().Be(1, "only Custom Account 001 has contacts");
    }

    [Fact]
    public void ExistsInALeftJoin_ReadsTheLeftJoinedContacts()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       where Contacts.Exists(p => p.ParentAccount.Id == a.CustomAccountId)
                       select new { a.Name, c.LastName }).ToList();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r => r.LastName.Should().StartWith("Last"));
    }

    [Fact]
    public void WhereEqualsOfAnotherNumberType_Throws()
    {
        var act = () => Accounts.Where(a => a.NumberOfEmployees.Equals(20L)).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void AnyOnLeftJoinedEntity_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                         from c in contacts.DefaultIfEmpty()
                         where Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                         select a.Name).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Aggregates over ordered rows
    // -------------------------------------------------------------------------

    [Fact]
    public void CountAfterOrderBy_CountsEveryRow()
    {
        var count = Accounts.OrderBy(a => a.Name).Count();

        count.Should().Be(150);
    }

    [Fact]
    public void SumOverUnorderedWithFirstRowJoin_SumsEachAccountOnce()
    {
        var expected = Accounts.Where(a => a.Name.StartsWith("Custom Account")).Sum(a => a.NumberOfEmployees);

        var result = (from a in Accounts
                      join c in Contacts.WithFirstRow() on a.CustomAccountId equals c.ParentAccount.Id
                      select a.NumberOfEmployees).Sum();

        result.Should().Be(expected).And.BeGreaterThan(0);
    }

    [Fact]
    public void CountOverOrderedWithFirstRowJoin_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts.OrderByDescending(p => p.LastName).WithFirstRow()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select new { a, c }).Count();

        act.Should().Throw<NotSupportedException>();
    }
}

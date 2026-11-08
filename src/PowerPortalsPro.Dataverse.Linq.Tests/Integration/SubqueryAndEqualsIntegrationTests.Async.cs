using FluentAssertions;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class SubqueryAndEqualsIntegrationTests
{
    [Fact]
    public async Task ToListAsync_WhereStaticStringEquals_ReturnsTheMatch()
    {
        var results = await Accounts.Where(a => string.Equals(a.Name, "Empty Account 002")).ToListAsync();

        results.Should().ContainSingle().Which.Name.Should().Be("Empty Account 002");
    }

    [Fact]
    public async Task CountAsync_AnyOnInnerJoinedEntity_CountsTheContactsWithAnOpportunity()
    {
        var count = await (from a in Accounts
                           join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                           where Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                           select c).CountAsync();

        count.Should().Be(100);
    }

    [Fact]
    public async Task CountAsync_ExistsInsideAny_CountsTheAccountsWithAContactWithAnOpportunity()
    {
        // Counted from the rows themselves, not by the query under test.
        var contactsWithOpportunities = new HashSet<Guid>(Opportunities.Select(o => o.Contact.Id).ToList());
        var expected = Contacts.ToList()
            .Where(c => contactsWithOpportunities.Contains(c.CustomContactId))
            .Select(c => c.ParentAccount.Id)
            .Distinct()
            .Count();

        var count = await Accounts
            .Where(a => Contacts.Any(c => c.ParentAccount.Id == a.CustomAccountId
                && Opportunities.Exists(o => o.Contact.Id == c.CustomContactId)))
            .CountAsync();

        count.Should().Be(expected).And.BeInRange(1, 99);
    }

    [Fact]
    public async Task CountAsync_InUnderOr_CountsEitherMatch()
    {
        var count = await Accounts
            .Where(a => a.Name == "Empty Account 001" || Contacts.In(c => c.ParentAccount.Id == a.CustomAccountId))
            .CountAsync();

        count.Should().Be(101);
    }

    [Fact]
    public async Task CountAsync_AfterOrderBy_CountsEveryRow()
    {
        var count = await Accounts.OrderByDescending(a => a.Name).CountAsync();

        count.Should().Be(150);
    }
}

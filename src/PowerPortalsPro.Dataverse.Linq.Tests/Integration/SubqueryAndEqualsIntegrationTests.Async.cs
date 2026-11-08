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
    public async Task CountAsync_AfterOrderBy_CountsEveryRow()
    {
        var count = await Accounts.OrderByDescending(a => a.Name).CountAsync();

        count.Should().Be(150);
    }
}

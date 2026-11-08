using FluentAssertions;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class KeptJoinEntityIntegrationTests
{
    [Fact]
    public async Task ToListAsync_WhereOnKeptInnerEntityByAnotherName_FiltersTheContacts()
    {
        var results = await Contacts.Where(x => x.LastName == "Last3").ToListAsync();

        results.Should().HaveCount(100);
        results.Should().AllSatisfy(c => c.LastName.Should().Be("Last3"));
    }

    [Fact]
    public async Task ToListAsync_CountChildrenOnKeptInnerJoinedRoot_CountsTheChildAccounts()
    {
        var results = await Accounts
            .Where(x => x.Name == "Custom Account 002")
            .Select(x => new { x.Name, Children = x.CountChildren() })
            .ToListAsync();

        results.Should().HaveCount(5);
        results.Should().AllSatisfy(r => r.Children.Should().Be(4));
    }

    [Fact]
    public async Task CountAsync_PredicateOnKeptLeftJoinedEntityByAnotherName_CountsTheContacts()
    {
        var count = await LeftJoinedContacts.CountAsync(x => x.LastName == "Last4");

        count.Should().Be(100);
    }
}

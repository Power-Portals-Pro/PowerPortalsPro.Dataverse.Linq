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
    public async Task CountAsync_PredicateOnKeptLeftJoinedEntityByAnotherName_CountsTheContacts()
    {
        var count = await LeftJoinedContacts.CountAsync(x => x.LastName == "Last4");

        count.Should().Be(100);
    }
}

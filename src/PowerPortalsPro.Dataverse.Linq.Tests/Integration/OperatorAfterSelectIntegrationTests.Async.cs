using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class OperatorAfterSelectIntegrationTests
{
    [Fact]
    public async Task ToListAsync_WhereOnAnonymousMember_ReturnsTheMatch()
    {
        var results = await Accounts
            .Select(a => new { a.Name, a.Website })
            .Where(x => x.Name == "Custom Account 001")
            .ToListAsync();

        results.Should().ContainSingle()
            .Which.Website.Should().Be("https://account001.example.com");
    }

    [Fact]
    public async Task FirstOrDefaultAsync_PredicateOnAnonymousMember_ReturnsTheMatch()
    {
        var result = await Accounts
            .Select(a => new { a.Name, a.Website })
            .FirstOrDefaultAsync(x => x.Name == "Empty Account 001");

        result.Should().NotBeNull();
        result!.Website.Should().Be("https://empty001.example.com");
    }

    [Fact]
    public async Task CountAsync_WhereOnSelectedValue_CountsTheMatches()
    {
        var count = await Accounts
            .Select(a => a.Name)
            .Where(name => name.StartsWith("Empty Account"))
            .CountAsync();

        count.Should().Be(50);
    }
}

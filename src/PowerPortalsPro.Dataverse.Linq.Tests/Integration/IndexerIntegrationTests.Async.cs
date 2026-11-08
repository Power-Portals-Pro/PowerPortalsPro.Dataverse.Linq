using FluentAssertions;
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class IndexerIntegrationTests
{
    [Fact]
    public async Task ToListAsync_WhereIndexerEquals_ReturnsTheMatch()
    {
        var results = await Accounts.Where(a => (string)a["new_name"] == "Custom Account 001").ToListAsync();

        results.Should().ContainSingle()
            .Which.GetAttributeValue<string>("new_name").Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task FirstOrDefaultAsync_IndexerPredicate_ReturnsTheMatch()
    {
        var result = await Accounts.FirstOrDefaultAsync(a => (string)a["new_name"] == "Empty Account 001");

        result.Should().NotBeNull();
        result!.GetAttributeValue<string>("new_name").Should().Be("Empty Account 001");
    }

    [Fact]
    public async Task ToListAsync_LeftJoinWhereOnInnerIndexer_ReturnsTheMatches()
    {
        var results = await (from a in Accounts
                             join c in Contacts on a.Id equals ((EntityReference)c["new_parentaccount"]).Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             where (string)c["new_lastname"] == "Last1"
                             select new { Name = (string)a["new_name"] }).ToListAsync();

        results.Should().HaveCount(100);
    }

    [Fact]
    public async Task CountAsync_WhereIndexerStartsWith_CountsTheMatches()
    {
        var count = await Accounts.Where(a => ((string)a["new_name"]).StartsWith("Empty Account")).CountAsync();

        count.Should().Be(50);
    }
}

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class CompositionIntegrationTests
{
    [Fact]
    public async Task ProjectInnerThenWhereAsync_ReturnsMatchingContacts()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var filtered = await contacts.Where(c => c.FirstName != null).ToListAsync();

        filtered.Should().NotBeEmpty();
        filtered.Should().AllSatisfy(c =>
        {
            c.Id.Should().NotBe(Guid.Empty);
            c.FirstName.Should().NotBeNull();
        });
    }

    [Fact]
    public async Task TwoStepJoinCompositionAsync_MatchesMonolithicQuery()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var composed = await (from c in contacts
                              join o in Service.Queryable<CustomOpportunity>()
                                  on c.CustomContactId equals o.Contact.Id
                              select new { c.FirstName, o.Name }).ToListAsync();

        var monolithic = await (from a in Service.Queryable<CustomAccount>()
                                join c in Service.Queryable<CustomContact>()
                                    on a.CustomAccountId equals c.ParentAccount.Id
                                join o in Service.Queryable<CustomOpportunity>()
                                    on c.CustomContactId equals o.Contact.Id
                                where a.Name != null
                                select new { c.FirstName, o.Name }).ToListAsync();

        composed.Should().BeEquivalentTo(monolithic);
    }
}

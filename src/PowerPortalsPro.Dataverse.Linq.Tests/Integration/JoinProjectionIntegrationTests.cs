using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Selects after a join that compute values from columns, use an entity whole beside one of its
/// columns, or read a single value. The seeded data has 100 accounts with 5 contacts each (last
/// names <c>Last1</c> to <c>Last5</c>) and 50 accounts with none; every account has a website.
/// </summary>
public partial class JoinProjectionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private static readonly string[] LastNames = ["Last1", "Last2", "Last3", "Last4", "Last5"];

    [Fact]
    public void JoinSelectLinkColumn_Distinct_ReturnsEachValueOnce()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select c.LastName).Distinct().ToList();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public void JoinSelectComputedRootMember_ReturnsComputedValues()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new { a.Website, WebsiteLength = a.Website.Length }).ToList();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r => r.WebsiteLength.Should().Be(r.Website.Length));
    }

    [Fact]
    public void LeftJoinSelectWholeRootBesideColumn_ReturnsCompleteEntity()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { a.Name, Account = a }).ToList();

        results.Should().HaveCount(550);
        results.Should().AllSatisfy(r =>
        {
            r.Account.Name.Should().Be(r.Name);
            r.Account.Website.Should().NotBeNullOrEmpty();
        });
    }
}

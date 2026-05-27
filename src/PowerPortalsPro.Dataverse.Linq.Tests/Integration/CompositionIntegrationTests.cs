using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Integration tests for query composition — extending an already-built
/// <see cref="IQueryable{T}"/> with further operators. Each composed query is
/// validated against the equivalent monolithic query (or known seed data).
/// </summary>
public partial class CompositionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Filter composition
    // -------------------------------------------------------------------------

    [Fact]
    public void IncrementalWhere_MatchesCombinedWhere()
    {
        var composed = Service.Queryable<CustomContact>().Where(c => c.FirstName != null);
        composed = composed.Where(c => c.LastName != null);
        var composedIds = composed.ToList().Select(c => c.Id).ToList();

        var combined = Service.Queryable<CustomContact>()
            .Where(c => c.FirstName != null && c.LastName != null)
            .ToList()
            .Select(c => c.Id)
            .ToList();

        composedIds.Should().BeEquivalentTo(combined);
    }

    // -------------------------------------------------------------------------
    // Join composition — project inner entity, then extend
    // -------------------------------------------------------------------------

    [Fact]
    public void ProjectInnerThenWhere_ReturnsMatchingContacts()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var filtered = contacts.Where(c => c.FirstName != null).ToList();

        filtered.Should().NotBeEmpty();
        filtered.Should().AllSatisfy(c =>
        {
            c.Id.Should().NotBe(Guid.Empty);
            c.FirstName.Should().NotBeNull();
            c.ParentAccount.Should().NotBeNull();
        });
    }

    [Fact]
    public void ProjectInnerThenReprojectColumns_ReturnsNarrowedData()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var projected = contacts.Select(c => new { c.FirstName, c.LastName }).ToList();

        projected.Should().NotBeEmpty();
        // 100 accounts × 5 contacts each = 500 joined rows.
        projected.Should().HaveCount(500);
    }

    [Fact]
    public void ProjectInnerThenOrderByThenTake_ReturnsOrderedPage()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var page = contacts.OrderBy(c => c.LastName).Take(10).ToList();

        page.Should().HaveCount(10);
        page.Select(c => c.LastName).Should().BeInAscendingOrder();
    }

    // -------------------------------------------------------------------------
    // Multi-step join composition
    // -------------------------------------------------------------------------

    [Fact]
    public void TwoStepJoinComposition_MatchesMonolithicQuery()
    {
        var contacts = from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var composed = (from c in contacts
                        join o in Service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        select new { c.FirstName, o.Name })
                       .ToList();

        var monolithic = (from a in Service.Queryable<CustomAccount>()
                          join c in Service.Queryable<CustomContact>()
                              on a.CustomAccountId equals c.ParentAccount.Id
                          join o in Service.Queryable<CustomOpportunity>()
                              on c.CustomContactId equals o.Contact.Id
                          where a.Name != null
                          select new { c.FirstName, o.Name })
                         .ToList();

        composed.Should().BeEquivalentTo(monolithic);
    }

    // -------------------------------------------------------------------------
    // Composing a join onto a pre-filtered outer query
    // -------------------------------------------------------------------------

    [Fact]
    public void FilteredOuterThenJoin_MatchesMonolithicQuery()
    {
        var accounts = Service.Queryable<CustomAccount>().Where(a => a.Name != null);

        var composed = (from a in accounts
                        join c in Service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, c.FirstName })
                       .ToList();

        var monolithic = (from a in Service.Queryable<CustomAccount>()
                          join c in Service.Queryable<CustomContact>()
                              on a.CustomAccountId equals c.ParentAccount.Id
                          where a.Name != null
                          select new { a.Name, c.FirstName })
                         .ToList();

        composed.Should().NotBeEmpty();
        composed.Should().BeEquivalentTo(monolithic);
    }
}

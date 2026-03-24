using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class JoinIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Null record on join
    // -------------------------------------------------------------------------

    [Fact]
    public void Join_NullPropertyOnJoinedRecord_DoesNotThrow()
    {
        var result = (from c in Service.Queryable<CustomContact>()
                      join a in Service.Queryable<CustomAccount>()
                          on c.ParentAccount.Id equals a.CustomAccountId
                      select new
                      {
                          c.Name,
                          a.Website,
                      }).FirstOrDefault();

        result.Should().NotBeNull();
        result!.Name.Should().NotBeNullOrEmpty();
        // Website may be null for some accounts, but the query should not throw
    }

    // -------------------------------------------------------------------------
    // Left join with where on outer entity
    // -------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_WhereOnOuterEntity_ReturnsFilteredResults()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       where a.Status == CustomAccount.CustomAccount_Status.Active
                       select new { a.Name }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public void LeftJoin_WhereOnOuterEntityAndInnerNull_ReturnsFilteredResults()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       where a.Name.Contains("Account") && c == null
                       select new { a.Name }).ToList();

        // Only accounts without contacts that contain "Account" in the name
        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().Contain("Account"));
    }

    [Fact]
    public void LeftJoin_SelectWholeEntities_ReturnsOuterAndInnerEntities()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       where a.Status == CustomAccount.CustomAccount_Status.Active
                       select new { Account = a, Contact = c }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Account.Should().NotBeNull();
            r.Account.Name.Should().NotBeNullOrEmpty();
            r.Account.CustomAccountId.Should().NotBe(Guid.Empty);
            r.Account.CreatedOn.Should().NotBeNull();
        });
        // Some results should have a contact, some should not (left join)
        results.Should().Contain(r => r.Contact != null);
        results.Where(r => r.Contact != null).Should().AllSatisfy(r =>
        {
            r.Contact!.FirstName.Should().NotBeNullOrEmpty();
            r.Contact.LastName.Should().NotBeNullOrEmpty();
            r.Contact.CustomContactId.Should().NotBe(Guid.Empty);
        });
    }

    [Fact]
    public void LeftJoin_SelectProjectedProperties_ReturnsNestedAnonymousTypes()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       where a.Status == CustomAccount.CustomAccount_Status.Active
                       select new { Account = new { a.CustomAccountId, a.Name }, Contact = new { c.CustomContactId, c.Name } }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Account.CustomAccountId.Should().NotBe(Guid.Empty);
            r.Account.Name.Should().NotBeNullOrEmpty();
        });
        // Matched rows should have populated contact properties
        var matched = results.Where(r => r.Contact.CustomContactId != Guid.Empty).ToList();
        matched.Should().NotBeEmpty();
        matched.Should().AllSatisfy(r =>
        {
            r.Contact.CustomContactId.Should().NotBe(Guid.Empty);
            r.Contact.Name.Should().NotBeNullOrEmpty();
        });
    }

    [Fact]
    public void LeftJoin_SelectWholeEntities_InnerIsNullForUnmatchedRows()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { Account = a, Contact = c }).ToList();

        // 100 accounts × 5 contacts + 50 accounts with no contacts = 550
        results.Should().HaveCount(550);
        results.Where(r => r.Contact == null).Should().HaveCount(50);
        results.Where(r => r.Contact != null).Should().HaveCount(500);
        results.Where(r => r.Contact != null).Should().AllSatisfy(r =>
        {
            r.Contact!.FirstName.Should().NotBeNullOrEmpty();
            r.Contact.LastName.Should().NotBeNullOrEmpty();
            r.Contact.CustomContactId.Should().NotBe(Guid.Empty);
        });
    }

    // -------------------------------------------------------------------------
    // Chained left joins
    // -------------------------------------------------------------------------

    [Fact]
    public void ChainedLeftJoin_TwoLeftJoins_ReturnsColumnsFromAllEntities()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id into opportunities
                       from o in opportunities.DefaultIfEmpty()
                       select new { a.Name, ContactFirstName = c.FirstName, OpportunityName = o.Name }).ToList();

        results.Should().NotBeEmpty();

        // Root entity columns should always be populated
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());

        // Contact columns should be populated for matched rows (accounts with contacts)
        var withContacts = results.Where(r => r.ContactFirstName != null).ToList();
        withContacts.Should().NotBeEmpty("some accounts have contacts");
        withContacts.Should().AllSatisfy(r => r.ContactFirstName.Should().NotBeNullOrEmpty());

        // Opportunity columns should be populated for matched rows (contacts with opportunities)
        var withOpportunities = results.Where(r => r.OpportunityName != null).ToList();
        withOpportunities.Should().NotBeEmpty("some contacts have opportunities");
        withOpportunities.Should().AllSatisfy(r => r.OpportunityName.Should().NotBeNullOrEmpty());

        // Some rows should have no contact (left join)
        results.Should().Contain(r => r.ContactFirstName == null);
    }

    [Fact]
    public void ChainedLeftJoin_MultipleLeftJoinsOnRoot_ReturnsColumnsFromAllEntities()
    {
        // Two left joins both keyed on the root entity produce sibling link entities
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.PrimaryContact.Id equals c.CustomContactId into contacts
                       from c in contacts.DefaultIfEmpty()
                       join o in Service.Queryable<CustomOpportunity>()
                           on a.CustomAccountId equals o.Contact.Id into opportunities
                       from o in opportunities.DefaultIfEmpty()
                       select new { a.Name, c.FirstName, OpportunityName = o.Name }).ToList();

        results.Should().NotBeEmpty();

        // Root entity columns should always be populated
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());

        // Contact columns should be populated for accounts that have a primary contact
        var withContacts = results.Where(r => r.FirstName != null).ToList();
        withContacts.Should().NotBeEmpty("some accounts have a primary contact");
        withContacts.Should().AllSatisfy(r => r.FirstName.Should().NotBeNullOrEmpty());
    }
}

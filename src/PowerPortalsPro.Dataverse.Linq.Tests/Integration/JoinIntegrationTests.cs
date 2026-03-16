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
}

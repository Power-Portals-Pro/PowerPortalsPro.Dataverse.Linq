using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class JoinProjectionIntegrationTests
{
    // -------------------------------------------------------------------------
    // Inner join
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_JoinSelectLinkColumn_Distinct_ReturnsEachValueOnce()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select c.LastName).Distinct().ToListAsync();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public async Task ToListAsync_JoinSelectComputedMembers_ReturnsComputedValues()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new
                             {
                                 a.Website,
                                 WebsiteLength = a.Website.Length,
                                 c.FirstName,
                                 c.LastName,
                                 FullName = c.FirstName + " " + c.LastName,
                             }).ToListAsync();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r =>
        {
            r.WebsiteLength.Should().Be(r.Website.Length);
            r.FullName.Should().Be($"{r.FirstName} {r.LastName}");
        });
    }

    [Fact]
    public async Task ToListAsync_JoinSelectWholeRootBesideColumn_ReturnsCompleteEntity()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, Account = a, ContactName = c.Name }).ToListAsync();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r =>
        {
            r.Account.Name.Should().Be(r.Name);
            r.Account.Website.Should().NotBeNullOrEmpty();
            r.ContactName.Should().NotBeNullOrEmpty();
        });
    }

    // -------------------------------------------------------------------------
    // Left join, select folded into the join
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_LeftJoinSelectComputedMembers_ReturnsComputedValues()
    {
        var websites = (await Service.Queryable<CustomAccount>().Select(a => new { a.Name, a.Website }).ToListAsync())
            .ToDictionary(a => a.Name, a => a.Website);

        // The website is read only inside the computation, so nothing else asks for its column.
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name, WebsiteLength = a.Website.Length }).ToListAsync();

        results.Should().HaveCount(550);
        results.Should().AllSatisfy(r => r.WebsiteLength.Should().Be(websites[r.Name].Length));
    }

    [Fact]
    public async Task ToListAsync_LeftJoinSelectWholeEntitiesBesideColumns_ReturnsCompleteEntities()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name, Account = a, ContactName = c.Name, Contact = c }).ToListAsync();

        results.Should().HaveCount(550);
        results.Should().AllSatisfy(r => r.Account.Website.Should().NotBeNullOrEmpty());
        results.Where(r => r.Contact != null).Should().HaveCount(500).And.AllSatisfy(r =>
        {
            r.Contact.Name.Should().Be(r.ContactName);
            r.Contact.LastName.Should().NotBeNullOrEmpty();
            r.Contact.Id.Should().NotBe(Guid.Empty);
        });
    }

    [Fact]
    public async Task ToListAsync_LeftJoinSelectInnerNullTest_ReportsWhetherEachRowMatched()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name, HasContact = c != null }).ToListAsync();

        results.Should().HaveCount(550);
        results.Count(r => r.HasContact).Should().Be(500);
        results.Where(r => !r.HasContact).Should().AllSatisfy(r => r.Name.Should().StartWith("Empty Account"));
    }

    [Fact]
    public async Task ToListAsync_UnboundLeftJoinSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        var results = await (from a in Service.Queryable("new_customaccount")
                             join c in Service.Queryable("new_customcontact")
                                 on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select c.GetAttributeValue<string>("new_lastname")).Distinct().ToListAsync();

        // The 50 accounts without contacts contribute one null.
        results.Should().BeEquivalentTo([.. LastNames, null]);
    }

    // -------------------------------------------------------------------------
    // Left join, where between the join and the select
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_LeftJoinWhereSelectComputedMembers_ReturnsComputedValues()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             where a.NumberOfEmployees != null
                             select new
                             {
                                 a.NumberOfEmployees,
                                 Employees = a.NumberOfEmployees + 1,
                                 ContactName = c == null ? null : c.Name.ToUpper(),
                             }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Employees.Should().Be(r.NumberOfEmployees + 1);
            r.ContactName.Should().NotBeNullOrEmpty().And.Be(r.ContactName!.ToUpper());
        });
    }

    [Fact]
    public async Task ToListAsync_UnboundLeftJoinWhereSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        // The 50 accounts without contacts are all active.
        var results = await (from a in Service.Queryable("new_customaccount")
                             join c in Service.Queryable("new_customcontact")
                                 on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             where c == null
                             select a.GetAttributeValue<OptionSetValue>("statecode").Value).Distinct().ToListAsync();

        results.Should().Equal(0);
    }
}

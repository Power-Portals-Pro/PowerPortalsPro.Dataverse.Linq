using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class JoinIntegrationTests
{
    // -------------------------------------------------------------------------
    // Joins
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithJoin_ReturnsProjectedValuesFromBothEntities()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.Name));
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.FirstName));
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.LastName));
        });
    }

    [Fact]
    public async Task ToListAsync_WithJoin_Returns500Records()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().HaveCount(500);
    }

    [Fact]
    public async Task ToListAsync_WithJoin_AccountNameIsPopulatedOnAllRows()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_WithJoin_ContactNamesArePopulatedOnAllRows()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.FirstName.Should().NotBeNullOrEmpty();
            r.LastName.Should().NotBeNullOrEmpty();
        });
    }

    [Fact]
    public async Task ToListAsync_WithJoin_EachAccountAppearsExactly5Times()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.GroupBy(r => r.Name)
               .Should().AllSatisfy(g => g.Should().HaveCount(5));
    }

    // -------------------------------------------------------------------------
    // Left joins
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithLeftJoin_WhereInnerIsNull_ReturnsAccountsWithNoContacts()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             where c == null
                             select new { a.Name }).ToListAsync();

        // 50 accounts were seeded without any contacts
        results.Should().HaveCount(50);
    }

    [Fact]
    public async Task ToListAsync_WithLeftJoin_ReturnsAllAccountsIncludingThoseWithNoContacts()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name }).ToListAsync();

        // 100 accounts × 5 contacts = 500 matched rows, plus 50 accounts with no contacts
        results.Should().HaveCount(550);
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_WithJoin_NamesMatchSeedDataPattern()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.Name.Should().StartWith("Custom Account");
            r.FirstName.Should().StartWith("First");
            r.LastName.Should().StartWith("Last");
        });
    }

    [Fact]
    public async Task ToListAsync_WithLeftJoin_NamesMatchSeedDataPattern()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name }).ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Name.Should().Match(n => n.StartsWith("Custom Account") || n.StartsWith("Empty Account")));
    }

    // -------------------------------------------------------------------------
    // Complex queries — join + where + orderby + select
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_JoinWithWhereAndOrderByAndSelect_ReturnsFilteredOrderedResults()
    {
        var results = await (from c in Service.Queryable<CustomContact>()
                             join a in Service.Queryable<CustomAccount>()
                                 on c.CustomContactId equals a.PrimaryContact.Id
                             where a.Name != null
                             orderby c.LastName
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactLastName = c.LastName
                             }).ToListAsync();

        // 100 accounts have a PrimaryContact, all have names
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNull();
            r.ContactLastName.Should().NotBeNullOrEmpty();
        });
        results.Select(r => r.ContactLastName).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task ToListAsync_JoinWithContainsFilter_ReturnsMatchingRecords()
    {
        var results = await (from c in Service.Queryable<CustomContact>()
                             join a in Service.Queryable<CustomAccount>()
                                 on c.CustomContactId equals a.PrimaryContact.Id
                             where a.Name.Contains("Account")
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactLastName = c.LastName
                             }).ToListAsync();

        // All 100 accounts with PrimaryContact have "Account" in their name
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
            r.AccountName.Should().Contain("Account"));
    }

    [Fact]
    public async Task ToListAsync_JoinWithOrFilter_ReturnsRecordsMatchingEitherCondition()
    {
        var results = await (from c in Service.Queryable<CustomContact>()
                             join a in Service.Queryable<CustomAccount>()
                                 on c.CustomContactId equals a.PrimaryContact.Id
                             where a.Name.Contains("001") || c.LastName.Contains("Last1")
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactLastName = c.LastName
                             }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            (r.AccountName.Contains("001") || r.ContactLastName.Contains("Last1")).Should().BeTrue());
    }

    // -------------------------------------------------------------------------
    // Join with complex where and string interpolation projection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_JoinWithComplexWhereAndInterpolation_ReturnsResults()
    {
        var accounts = await (from a in Service.Queryable<CustomAccount>()
                              join c in Service.Queryable<CustomContact>()
                                on a.CustomAccountId equals c.ParentAccount.Id
                              where a.PercentComplete != null
                                && (c.LastName.StartsWith("Last1")
                                    || c.LastName.StartsWith("Last2"))
                                && (a.NumberOfEmployees > 30
                                    || a.PercentComplete < 30)
                              select new
                              {
                                  AccountName = a.Name,
                                  ContactFullName = $"{c.FirstName} {c.LastName}",
                              }).ToListAsync();

        accounts.Should().NotBeEmpty();
        accounts.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNullOrEmpty();
            r.ContactFullName.Should().NotBeNullOrEmpty();
        });
    }

    [Fact]
    public async Task ToListAsync_JoinWithComplexWhereOrderByAndInterpolation_ReturnsOrderedResults()
    {
        var accounts = await (from a in Service.Queryable<CustomAccount>()
                              join c in Service.Queryable<CustomContact>()
                                on a.CustomAccountId equals c.ParentAccount.Id
                              where a.PercentComplete != null
                                && (c.LastName.StartsWith("Last1")
                                    || c.LastName.StartsWith("Last2"))
                                && (a.NumberOfEmployees > 30
                                    || a.PercentComplete < 30)
                              orderby
                                a.Name
                                , c.LastName descending
                              select new
                              {
                                  AccountName = a.Name,
                                  ContactFullName = $"{c.FirstName} {c.LastName}",
                              }).ToListAsync();

        accounts.Should().NotBeEmpty();
        accounts.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNullOrEmpty();
            r.ContactFullName.Should().NotBeNullOrEmpty();
        });

        // Verify primary ordering by AccountName ascending
        accounts.Select(r => r.AccountName).Should().BeInAscendingOrder();
    }

    // -------------------------------------------------------------------------
    // Select into entity type from join
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_SelectIntoEntityType_ReturnsTypedResults()
    {
        var contacts = await (from a in Service.Queryable<CustomAccount>()
                              join c in Service.Queryable<CustomContact>()
                                  on a.PrimaryContact.Id equals c.CustomContactId
                              select new CustomContact
                              {
                                  CustomContactId = c.CustomContactId,
                                  FirstName = c.FirstName,
                                  LastName = c.LastName,
                              }).ToListAsync();

        // 100 accounts have a PrimaryContact set
        contacts.Should().HaveCount(100);
        contacts.Should().AllSatisfy(c =>
        {
            c.CustomContactId.Should().NotBe(Guid.Empty);
            c.FirstName.Should().NotBeNullOrEmpty();
            c.LastName.Should().NotBeNullOrEmpty();
        });
    }

    // -------------------------------------------------------------------------
    // OrderBy on joined columns
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_OrderByJoinedColumn_ReturnsOrderedResults()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             orderby c.LastName, a.Name
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactLastName = c.LastName,
                             }).ToListAsync();

        // 100 accounts have a PrimaryContact set
        results.Should().HaveCount(100);
        results.Select(r => r.ContactLastName).Should().BeInAscendingOrder();
    }

    // -------------------------------------------------------------------------
    // SubQuery with joins
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubQuery_JoinOnPreFilteredQueryable_ReturnsResults()
    {
        var accountsQuery = Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null);

        var contactsQuery = Service.Queryable<CustomContact>()
            .Where(c => c.FirstName != null);

        var results = await (from a in accountsQuery
                             join c in contactsQuery
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             select new
                             {
                                 a.Name,
                                 c.FirstName,
                             }).ToListAsync();

        // 100 accounts have a PrimaryContact set, and all accounts/contacts have names
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
        {
            r.Name.Should().NotBeNull();
            r.FirstName.Should().NotBeNull();
        });
    }

    [Fact]
    public async Task SubQuery_MultipleJoinsOnPreFiltered_ReturnsResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var accountsQuery = Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null);

        var results = await (from a in accountsQuery
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             join o in Service.Queryable<CustomOpportunity>()
                                 on c.CustomContactId equals o.Contact.Id
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactFirstName = c.FirstName,
                                 OpportunityName = o.Name,
                             }).ToListAsync();

        // All opportunities have a contact which has a parent account, so count should match total
        results.Should().HaveCount(totalOpportunities);
    }

    // -------------------------------------------------------------------------
    // Left join with late-bound
    // -------------------------------------------------------------------------

    [Fact]
    public async Task LeftJoin_SelectAccountFromLeftJoin_ReturnsAllAccounts()
    {
        // Left join selecting only from the outer entity works
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.PrimaryContact.Id equals c.CustomContactId into contacts
                             from c in contacts.DefaultIfEmpty()
                             select a).ToListAsync();

        results.Should().HaveCount(150);
    }

    // -------------------------------------------------------------------------
    // Column comparison across different tables (3-entity join)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ThreeWayJoin_ReturnsResults()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             join pa in Service.Queryable<CustomAccount>()
                                 on c.ParentAccount.Id equals pa.CustomAccountId
                             orderby a.Name
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactName = c.Name,
                                 ParentAccountName = pa.Name,
                             }).ToListAsync();

        // 100 accounts with PrimaryContact, each contact has a ParentAccount
        results.Should().HaveCount(100);
        results.Select(r => r.AccountName).Should().BeInAscendingOrder();
        results.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNullOrEmpty();
            r.ContactName.Should().NotBeNullOrEmpty();
            r.ParentAccountName.Should().NotBeNullOrEmpty();
        });
    }

    // -------------------------------------------------------------------------
    // Explicit columns on join
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_WithExplicitColumns_ReturnsOnlyRequestedColumns()
    {
        var results = await (from a in Service.Queryable<CustomAccount>("new_name", "createdon")
                             join c in Service.Queryable<CustomContact>("new_firstname", "new_lastname")
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             where a.Name != null
                             select new
                             {
                                 AccountName = a.Name,
                                 CreatedOn = a.CreatedOn,
                                 FirstName = c.FirstName,
                                 LastName = c.LastName,
                             }).ToListAsync();

        // 100 accounts with PrimaryContact
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNull();
            r.CreatedOn.Should().NotBeNull();
            r.FirstName.Should().NotBeNull();
            r.LastName.Should().NotBeNull();
        });
    }

    // -------------------------------------------------------------------------
    // Inner join — multiple where clauses across entities
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_MultipleWhereClausesAcrossEntities_ReturnsFilteredResults()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             where a.Name != null
                             where c.FirstName != null
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactFirstName = c.FirstName,
                             }).ToListAsync();

        // 100 accounts have PrimaryContact, all have names and first names
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
        {
            r.AccountName.Should().NotBeNull();
            r.ContactFirstName.Should().NotBeNull();
        });
    }

    // -------------------------------------------------------------------------
    // Inner join — Or filter across entities
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_OrFilterAcrossEntities_ReturnsMatchingRecords()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.PrimaryContact.Id equals c.CustomContactId
                             where a.Name.Contains("001") || c.FirstName.Contains("First")
                             select new
                             {
                                 AccountName = a.Name,
                                 ContactFirstName = c.FirstName,
                             }).ToListAsync();

        // All contacts have "First" in their FirstName, so the or-condition matches all 100 rows
        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r =>
            (r.AccountName.Contains("001") || r.ContactFirstName.Contains("First")).Should().BeTrue());
    }
}

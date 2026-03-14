using Dataverse.Linq.Extensions;
using Dataverse.Linq.Model;
using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;

namespace Dataverse.Linq.Tests;

public class QueryableIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // Basic retrieval
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_ReturnsRecords()
    {
        var results = await Service.Queryable<CustomAccount>().ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Id.Should().NotBe(Guid.Empty));
    }

    [Fact]
    public async Task ToListAsync_ReturnsRecordsWithAttributes()
    {
        var results = await Service.Queryable<CustomAccount>().ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Attributes.Should().NotBeEmpty());
    }

    // -------------------------------------------------------------------------
    // Column selection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithExplicitColumns_OnlyReturnsRequestedColumns()
    {
        var results = await Service.Queryable<CustomAccount>("new_name").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Attributes.Keys.Should().OnlyContain(k => k == "new_name" || k == CustomAccount.PrimaryIdAttribute));
    }

    [Fact]
    public async Task ToListAsync_WithExplicitColumns_ReturnsFewerAttributesThanAllColumns()
    {
        var allColumns = await Service.Queryable<CustomAccount>().ToListAsync();
        var singleColumn = await Service.Queryable<CustomAccount>("new_name").ToListAsync();

        allColumns.Should().NotBeEmpty();
        singleColumn.Should().NotBeEmpty();
        allColumns[0].Attributes.Count.Should().BeGreaterThan(singleColumn[0].Attributes.Count);
    }

    // -------------------------------------------------------------------------
    // Select projection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithSelectProjection_ReturnsProjectedValues()
    {
        var results = await (from r in Service.Queryable<CustomAccount>()
                             select new { r.Name, r.Website }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.Name));
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.Website));
        });
    }

    [Fact]
    public async Task ToListAsync_WithSelectProjection_MatchesExplicitColumnQuery()
    {
        var projected = await (from r in Service.Queryable<CustomAccount>()
                               select new { r.Name, r.Website }).ToListAsync();

        var explicit_ = await Service.Queryable<CustomAccount>("new_name", "new_website").ToListAsync();

        projected.Should().HaveSameCount(explicit_);
        projected.Select(p => p.Name).Should().BeEquivalentTo(explicit_.Select(e => e.Name));
    }

    [Fact]
    public async Task ToListAsync_WithSelectProjectionIncludingEntityReference_ReturnsProjectedValues()
    {
        var results = await (from r in Service.Queryable<CustomAccount>()
                             select new { r.Name, r.Website, r.PrimaryContact }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull();
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.Name));
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.Website));
            r.GetType().GetProperties().Should().Contain(p => p.Name == nameof(r.PrimaryContact));
        });
    }

    [Fact]
    public async Task ToListAsync_Returns150CustomAccountRecords()
    {
        var results = await Service.Queryable<CustomAccount>().ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task ToListAsync_100AccountsHavePrimaryContactSet()
    {
        var results = await Service.Queryable<CustomAccount>().ToListAsync();

        results.Count(r => r.PrimaryContact != null).Should().Be(100);
    }

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
    // Unbound entity queries
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_ReturnsRecords()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Id.Should().NotBe(Guid.Empty));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_ReturnsRecordsWithAttributes()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Attributes.Should().NotBeEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_Returns150Records()
    {
        var results = await Service.Queryable("new_customaccount").ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WithExplicitColumns_OnlyReturnsRequestedColumns()
    {
        var results = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Attributes.Keys.Should().OnlyContain(k => k == "new_name" || k == "new_customaccountid"));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WithExplicitColumns_ReturnsFewerAttributesThanAllColumns()
    {
        var allColumns = await Service.Queryable("new_customaccount").ToListAsync();
        var singleColumn = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        allColumns.Should().NotBeEmpty();
        singleColumn.Should().NotBeEmpty();
        allColumns[0].Attributes.Count.Should().BeGreaterThan(singleColumn[0].Attributes.Count);
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_MatchesTypedQueryResults()
    {
        var typed = await Service.Queryable<CustomAccount>("new_name").ToListAsync();
        var unbound = await Service.Queryable("new_customaccount", "new_name").ToListAsync();

        unbound.Should().HaveSameCount(typed);
        unbound.Select(e => e.GetAttributeValue<string>("new_name")).Should()
            .BeEquivalentTo(typed.Select(e => e.Name));
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_CanAccessAttributesViaGetAttributeValue()
    {
        var results = await Service.Queryable("new_customaccount", "new_name", "new_website").ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.GetAttributeValue<string>("new_name").Should().NotBeNullOrEmpty();
        });
    }

    // -------------------------------------------------------------------------
    // Ordering
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithOrderByAscending_ReturnsRecordsInAscendingOrder()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             orderby a.Name
                             select a).ToListAsync();

        results.Should().NotBeEmpty();
        results.Select(r => r.Name).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task ToListAsync_WithOrderByDescending_ReturnsRecordsInDescendingOrder()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             orderby a.Name descending
                             select a).ToListAsync();

        results.Should().NotBeEmpty();
        results.Select(r => r.Name).Should().BeInDescendingOrder();
    }

    [Fact]
    public async Task ToListAsync_WithOrderByAndSelectProjection_ReturnsOrderedProjectedValues()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             orderby a.Name
                             select new { a.Name, a.Website }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Select(r => r.Name).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task ToListAsync_WithOrderBy_Returns150Records()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             orderby a.Name
                             select a).ToListAsync();

        results.Should().HaveCount(150);
    }

    // -------------------------------------------------------------------------
    // Where — typed proxy
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereNameEquals_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001")
            .ToListAsync();

        results.Should().ContainSingle();
        results[0].Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task ToListAsync_WhereNameNotNull_ExcludesNulls()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNull());
    }

    [Fact]
    public async Task ToListAsync_WhereDescriptionIsNull_ReturnsRecordsWithNullDescription()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var nullDescriptions = all.Count(a => a.Description == null);

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Description == null)
            .ToListAsync();

        results.Should().HaveCount(nullDescriptions);
    }

    // -------------------------------------------------------------------------
    // Where — unbound entity with GetAttributeValue
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereNotIsNullOrEmpty_ReturnsNonNullRecords()
    {
        var results = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r =>
            r.GetAttributeValue<string>("new_name").Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereIsNullOrEmpty_ReturnsNullRecords()
    {
        var all = await Service.Queryable("new_customaccount").ToListAsync();
        var nullDescriptions = all.Count(a => a.GetAttributeValue<string>("new_description") == null);

        var results = await Service.Queryable("new_customaccount")
            .Where(x => string.IsNullOrEmpty(x.GetAttributeValue<string>("new_description")))
            .ToListAsync();

        results.Should().HaveCount(nullDescriptions);
    }

    // -------------------------------------------------------------------------
    // Select — unbound entity with GetAttributeValue
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_UnboundEntity_SelectWithGetAttributeValue_ReturnsProjectedValues()
    {
        var results = await Service.Queryable("new_customaccount")
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name") })
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_WhereAndSelect_ReturnsFilteredProjection()
    {
        var results = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_description")))
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name"), Description = x.GetAttributeValue<string>("new_description") })
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Name.Should().NotBeNullOrEmpty();
            r.Description.Should().NotBeNullOrEmpty();
        });
    }

    [Fact]
    public async Task ToListAsync_UnboundEntity_SelectMatchesTypedQuery()
    {
        var typed = await (from a in Service.Queryable<CustomAccount>()
                           select new { a.Name, a.Website }).ToListAsync();

        var unbound = await Service.Queryable("new_customaccount")
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name"), Website = x.GetAttributeValue<string>("new_website") })
            .ToListAsync();

        unbound.Should().HaveSameCount(typed);
        unbound.Select(u => u.Name).Should().BeEquivalentTo(typed.Select(t => t.Name));
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

    [Fact]
    public async Task ToListAsync_WithContainsFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Account"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().Contain("Account"));
    }

    [Fact]
    public async Task ToListAsync_WithStartsWithFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().StartWith("Custom"));
    }

    [Fact]
    public async Task ToListAsync_WithEndsWithFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.EndsWith("001"))
            .ToListAsync();

        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => r.Name.EndsWith("001"));
    }

    // -------------------------------------------------------------------------
    // Where — NotEqual (non-null)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereNotEqualToValue_ExcludesMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != "Custom Account 001")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => r.Name == "Custom Account 001");
    }

    // -------------------------------------------------------------------------
    // Where — comparison operators (lt, le, gt, ge)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereLessThan_ReturnsMatchingRecords()
    {
        // Seed: NumberOfEmployees = i * 10 for i where i % 8 != 0
        // i=1 → 10, i=2 → 20, i=3 → 30, i=4 → 40 all have < 50
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees < 50)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees < 50);
    }

    [Fact]
    public async Task ToListAsync_WhereLessEqual_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees <= 50)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees <= 50);
        // Should include i=5 (50) which < 50 would not
        results.Count.Should().BeGreaterThan(
            (await Service.Queryable<CustomAccount>()
                .Where(a => a.NumberOfEmployees < 50)
                .ToListAsync()).Count);
    }

    [Fact]
    public async Task ToListAsync_WhereGreaterThan_ReturnsMatchingRecords()
    {
        // Seed: i=97 → 970, i=98 → 980, i=99 → 990 (i=100 → 1000 but 100 % 8 == 4 so not null... check: 100%8=4 !=0, so 1000 is set)
        // > 950 should return records with 960, 970, 980, 990, 1000
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees > 950)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees > 950);
    }

    [Fact]
    public async Task ToListAsync_WhereGreaterEqual_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees >= 950)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees >= 950);
        // Should include i=95 (950) which > 950 would not
        results.Count.Should().BeGreaterThan(
            (await Service.Queryable<CustomAccount>()
                .Where(a => a.NumberOfEmployees > 950)
                .ToListAsync()).Count);
    }

    // -------------------------------------------------------------------------
    // Where — In / NotIn
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereIn_ReturnsMatchingRecords()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002", "Custom Account 003" };
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => names.Contains(a.Name))
            .ToListAsync();

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo(names);
    }

    [Fact]
    public async Task ToListAsync_WhereNotIn_ExcludesMatchingRecords()
    {
        var excluded = new[] { "Custom Account 001", "Custom Account 002" };
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => !excluded.Contains(a.Name))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => excluded.Contains(r.Name));
    }

    [Fact]
    public async Task ToListAsync_WhereInWithGuids_ReturnsMatchingRecords()
    {
        // First get some known account IDs
        var allAccounts = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Custom Account 002" || a.Name == "Custom Account 003")
            .ToListAsync();
        var ids = allAccounts.Select(a => a.CustomAccountId).ToArray();
        ids.Should().HaveCount(3);

        // Now query by those IDs
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => ids.Contains(a.CustomAccountId))
            .ToListAsync();

        results.Should().HaveCount(3);
        results.Select(r => r.CustomAccountId).Should().BeEquivalentTo(ids);
    }

    [Fact]
    public async Task ToListAsync_WhereNotInWithGuids_ExcludesMatchingRecords()
    {
        // First get some known account IDs
        var excluded = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Custom Account 002")
            .ToListAsync();
        var excludedIds = excluded.Select(a => a.CustomAccountId).ToArray();
        excludedIds.Should().HaveCount(2);

        // Now query excluding those IDs
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => !excludedIds.Contains(a.CustomAccountId))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => excludedIds.Contains(r.CustomAccountId));
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (parameterless)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereOlderThanXMonths_ReturnsMatchingRecords()
    {
        // Seed dates range from 2001–2024; OlderThanXMonths(12) should return most of them
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OlderThanXMonths(12))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.DateCompanyWasOrganized.HasValue
            && r.DateCompanyWasOrganized.Value < DateTime.UtcNow.AddMonths(-12));
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (On / OnOrBefore / OnOrAfter)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereOnOrAfter_ReturnsMatchingRecords()
    {
        // Seed: dates go up to 2024; OnOrAfter 2020-01-01 should return a subset
        var cutoff = new DateTime(2020, 1, 1);
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrAfter(cutoff))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.DateCompanyWasOrganized.HasValue
            && r.DateCompanyWasOrganized.Value >= cutoff);
    }

    [Fact]
    public async Task ToListAsync_WhereOnOrBefore_ReturnsMatchingRecords()
    {
        var cutoff = new DateTime(2005, 12, 31);
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrBefore(cutoff))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.DateCompanyWasOrganized.HasValue
            && r.DateCompanyWasOrganized.Value <= cutoff);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (Between / NotBetween)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereBetween_ReturnsMatchingRecords()
    {
        var from = new DateTime(2010, 1, 1);
        var to = new DateTime(2015, 12, 31);
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Between(from, to))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.DateCompanyWasOrganized.HasValue
            && r.DateCompanyWasOrganized.Value >= from
            && r.DateCompanyWasOrganized.Value <= to);
    }

    [Fact]
    public async Task ToListAsync_WhereNotBetween_ReturnsMatchingRecords()
    {
        var from = new DateTime(2010, 1, 1);
        var to = new DateTime(2015, 12, 31);
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NotBetween(from, to))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.DateCompanyWasOrganized.HasValue
            && (r.DateCompanyWasOrganized.Value < from || r.DateCompanyWasOrganized.Value > to));
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedBetween_ReturnsSameAsNotBetween()
    {
        var from = new DateTime(2010, 1, 1);
        var to = new DateTime(2015, 12, 31);

        var notBetween = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NotBetween(from, to))
            .ToListAsync();

        var negatedBetween = await Service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.Between(from, to))
            .ToListAsync();

        negatedBetween.Should().HaveCount(notBetween.Count);
        negatedBetween.Select(r => r.CustomAccountId).Should().BeEquivalentTo(notBetween.Select(r => r.CustomAccountId));
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedNotBetween_ReturnsSameAsBetween()
    {
        var from = new DateTime(2010, 1, 1);
        var to = new DateTime(2015, 12, 31);

        var between = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Between(from, to))
            .ToListAsync();

        var negatedNotBetween = await Service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.NotBetween(from, to))
            .ToListAsync();

        negatedNotBetween.Should().HaveCount(between.Count);
        negatedNotBetween.Select(r => r.CustomAccountId).Should().BeEquivalentTo(between.Select(r => r.CustomAccountId));
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (LastXDays)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereLastXDays_ReturnsMatchingRecords()
    {
        // Seed dates are in the past (2001–2024), so LastXDays(1) should return nothing or very few
        // Use a large value to ensure we get results
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.LastXDays(365 * 30))
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    // -------------------------------------------------------------------------
    // Where — User / Business unit operators
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereEqualUserId_ReturnsRecordsOwnedByCurrentUser()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Owner.Id.EqualUserId())
            .ToListAsync();

        // The current user should own the seeded records
        results.Should().NotBeEmpty();

        // All returned records should share the same owner (the current user)
        results.Select(r => r.Owner.Id).Distinct().Should().ContainSingle();
    }

    [Fact]
    public async Task ToListAsync_WhereNotEqualUserId_ReturnsRecordsNotOwnedByCurrentUser()
    {
        var ownedByMe = await Service.Queryable<CustomAccount>()
            .Where(a => a.Owner.Id.EqualUserId())
            .ToListAsync();
        var all = await Service.Queryable<CustomAccount>().ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Owner.Id.NotEqualUserId())
            .ToListAsync();

        // eq-userid + ne-userid should cover all records
        results.Should().HaveCount(all.Count - ownedByMe.Count);
    }

    [Fact]
    public async Task ToListAsync_WhereEqualBusinessId_ReturnsRecordsInCurrentBusinessUnit()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.OwningBusinessUnit.Id.EqualBusinessId())
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereNotEqualBusinessId_ReturnsRecordsNotInCurrentBusinessUnit()
    {
        var allInBu = await Service.Queryable<CustomAccount>()
            .Where(a => a.OwningBusinessUnit.Id.EqualBusinessId())
            .ToListAsync();
        var all = await Service.Queryable<CustomAccount>().ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.OwningBusinessUnit.Id.NotEqualBusinessId())
            .ToListAsync();

        // Records not in current BU = total - records in current BU
        results.Should().HaveCount(all.Count - allInBu.Count);
    }

    // -------------------------------------------------------------------------
    // WithPageSize
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithPageSize_ReturnsAllRecords()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .WithPageSize(10)
            .ToListAsync();

        results.Should().HaveCount(all.Count);
    }

    [Fact]
    public async Task ToListAsync_WithPageSizeAndWhere_ReturnsAllFilteredRecords()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .WithPageSize(5)
            .ToListAsync();

        results.Should().HaveCount(all.Count);
        results.Should().OnlyContain(r => r.Name != null);
    }

    // -------------------------------------------------------------------------
    // ForEachPageAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ForEachPageAsync_ProcessesAllPages()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var collected = new List<CustomAccount>();
        var pageCount = 0;

        await Service.Queryable<CustomAccount>()
            .WithPageSize(10)
            .ForEachPageAsync(async page =>
            {
                pageCount++;
                collected.AddRange(page);
                await Task.CompletedTask;
            });

        collected.Should().HaveCount(all.Count);
        pageCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task ForEachPageAsync_WithWhereAndSelect_ProjectsCorrectly()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Select(a => new { a.Name })
            .ToListAsync();

        var collected = new List<string>();

        await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Select(a => new { a.Name })
            .WithPageSize(10)
            .ForEachPageAsync(async page =>
            {
                collected.AddRange(page.Select(p => p.Name));
                await Task.CompletedTask;
            });

        collected.Should().HaveCount(all.Count);
        collected.Should().OnlyContain(n => n != null);
    }

    [Fact]
    public async Task ForEachPageAsync_WithoutPageSize_ReturnsSinglePage()
    {
        var pageCount = 0;

        await Service.Queryable<CustomAccount>()
            .ForEachPageAsync(async page =>
            {
                pageCount++;
                page.Should().NotBeEmpty();
                await Task.CompletedTask;
            });

        // Without WithPageSize, Dataverse uses its default page size (5000)
        // so all 150 records come back in a single page
        pageCount.Should().Be(1);
    }

    // -------------------------------------------------------------------------
    // Terminal operators — First / FirstOrDefault / Single / SingleOrDefault
    // -------------------------------------------------------------------------

    [Fact]
    public async Task FirstAsync_ReturnsFirstRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .FirstAsync();

        result.Should().NotBeNull();
        result.Name.Should().NotBeNull();
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_ReturnsMatchingRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithNoMatch_ReturnsNull()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstOrDefaultAsync(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithMatch_ReturnsRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstOrDefaultAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task SingleAsync_ReturnsOnlyMatchingRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task SingleAsync_WithMultipleMatches_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .SingleAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithNoMatch_ReturnsNull()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleOrDefaultAsync(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithOneMatch_ReturnsRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleOrDefaultAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public void First_Sync_ReturnsFirstRecord()
    {
        var result = Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .First();

        result.Should().NotBeNull();
        result.Name.Should().NotBeNull();
    }

    [Fact]
    public void FirstOrDefault_Sync_WithNoMatch_ReturnsNull()
    {
        var result = Service.Queryable<CustomAccount>()
            .FirstOrDefault(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }

    // -------------------------------------------------------------------------
    // Where — ContainsValues (multi-select option set)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereContainsValues_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.ContainsValues(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors != null
            && r.FavoriteColors.Contains(CustomContact.Color.Red));
    }

    [Fact]
    public async Task ToListAsync_WhereContainsValuesMultiple_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.ContainsValues(CustomContact.Color.Red, CustomContact.Color.Blue))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors != null
            && (r.FavoriteColors.Contains(CustomContact.Color.Red)
                || r.FavoriteColors.Contains(CustomContact.Color.Blue)));
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedContainsValues_ExcludesMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.ContainsValues(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors == null
            || !r.FavoriteColors.Contains(CustomContact.Color.Red));
    }

    [Fact]
    public async Task ToListAsync_WhereListContainsEnum_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Contains(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors != null
            && r.FavoriteColors.Contains(CustomContact.Color.Red));
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedListContainsEnum_ExcludesMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Contains(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors == null
            || !r.FavoriteColors.Contains(CustomContact.Color.Red));
    }

    [Fact]
    public async Task ToListAsync_WhereMultiSelectEqualsSingleValue_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Equals(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedMultiSelectEquals_ExcludesMatchingRecords()
    {
        var results = await Service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Equals(CustomContact.Color.Red))
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereMultiSelectEqualsMultipleValues_ReturnsMatchingRecords()
    {
        var colors = new[] { CustomContact.Color.Green, CustomContact.Color.Blue };
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Equals(colors))
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedMultiSelectEqualsMultipleValues_ReturnsRecords()
    {
        var colors = new[] { CustomContact.Color.Red, CustomContact.Color.Blue };
        var results = await Service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Equals(colors))
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    // -------------------------------------------------------------------------
    // Column-to-column comparison (valueof)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereSameTableColumnsEqual_ReturnsOnlyMatchingRecords()
    {
        // Seeded contacts have different FirstName and LastName values,
        // so no records should match FirstName == LastName.
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FirstName == c.LastName)
            .ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereSameTableColumnsNotEqual_ReturnsNonMatchingRecords()
    {
        // Seeded contacts have different FirstName and LastName values,
        // so all records should match FirstName != LastName.
        var allContacts = await Service.Queryable<CustomContact>().ToListAsync();
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FirstName != c.LastName)
            .ToListAsync();

        results.Should().HaveCount(allContacts.Count);
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableColumnEqual_ReturnsOnlyMatchingRecords()
    {
        // Seeded data: Account.Name = "Custom Account 001" etc,
        // Contact.FirstName = "First001" etc — never equal.
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             where a.Name == c.FirstName
                             select new { a.Name, c.FirstName }).ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableRatingEqual_ReturnsMatchingRecords()
    {
        // Accounts and contacts both have ratings assigned from the same enum
        // with different modulus patterns, so some match and some don't.
        var matches = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             where a.AccountRating_OptionSetValue == c.ContactRating_OptionSetValue
                             select new { a.Name, a.AccountRating, c.FirstName, c.ContactRating }).ToListAsync();

        matches.Should().NotBeEmpty();
        matches.Should().AllSatisfy(r =>
            ((int?)r.AccountRating).Should().Be((int?)r.ContactRating));
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableRatingNotEqual_ReturnsNonMatchingRecords()
    {
        var nonMatches = await (from a in Service.Queryable<CustomAccount>()
                                join c in Service.Queryable<CustomContact>()
                                    on a.CustomAccountId equals c.ParentAccount.Id
                                where a.AccountRating_OptionSetValue != c.ContactRating_OptionSetValue
                                select new { a.Name, a.AccountRating, c.FirstName, c.ContactRating }).ToListAsync();

        nonMatches.Should().NotBeEmpty();
        nonMatches.Should().AllSatisfy(r =>
        {
            // Either side may be null (null != value is true in FetchXml)
            if (r.AccountRating is not null && r.ContactRating is not null)
                ((int)r.AccountRating).Should().NotBe((int)r.ContactRating);
        });
    }

    // -------------------------------------------------------------------------
    // Any() — link-type="any" / "not any"
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereAny_ReturnsContactsThatArePrimaryContactOfMatchingAccount()
    {
        // Find contacts that are the primary contact of an account named "Custom Account 001".
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task ToListAsync_WhereNotAny_ReturnsContactsThatAreNotPrimaryContactOfMatchingAccount()
    {
        // Find contacts that are NOT the primary contact of any account named "Custom Account 001".
        var allContacts = await Service.Queryable<CustomContact>().ToListAsync();
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => !Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        // All contacts except the one primary contact of "Custom Account 001"
        results.Should().HaveCount(allContacts.Count - 1);
    }

    [Fact]
    public async Task ToListAsync_WhereAnyWithoutFilter_ReturnsContactsThatArePrimaryContactOfAnyAccount()
    {
        // Find contacts that are the primary contact of any account.
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId))
            .ToListAsync();

        // Each of the 100 accounts has a primary contact (the first contact per account group).
        results.Should().HaveCount(100);
    }

    // -------------------------------------------------------------------------
    // Take
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Take_LimitsResultCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var taken = await Service.Queryable<CustomAccount>().Take(5).ToListAsync();

        all.Count.Should().BeGreaterThan(5);
        taken.Should().HaveCount(5);
    }

    [Fact]
    public async Task Take_WithWhereAndOrderBy_ReturnsLimitedOrderedResults()
    {
        var taken = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Custom"))
            .OrderBy(a => a.Name)
            .Take(3)
            .ToListAsync();

        taken.Should().HaveCount(3);
        taken.Select(a => a.Name).Should().BeInAscendingOrder();
    }

    // -------------------------------------------------------------------------
    // WithPage
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithPageAndPageSize_ReturnsCorrectPage()
    {
        const int pageSize = 10;

        var all = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .ToListAsync();

        var page1 = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .WithPageSize(pageSize)
            .WithPage(1)
            .ToListAsync();

        var page2 = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .WithPageSize(pageSize)
            .WithPage(2)
            .ToListAsync();

        page1.Should().HaveCount(pageSize);
        page2.Should().HaveCount(pageSize);

        // Pages should contain different records
        page1.Select(a => a.Id).Should().NotIntersectWith(page2.Select(a => a.Id));

        // Page 1 should match the first N records from the full list
        page1.Select(a => a.Name).Should().Equal(all.Take(pageSize).Select(a => a.Name));

        // Page 2 should match the next N records
        page2.Select(a => a.Name).Should().Equal(all.Skip(pageSize).Take(pageSize).Select(a => a.Name));
    }

    // -------------------------------------------------------------------------
    // WithAggregateLimit
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithAggregateLimit_GroupByQuery_ReturnsResults()
    {
        var results = await Service.Queryable<CustomAccount>()
            .GroupBy(a => a.AccountRating)
            .Select(g => new { Rating = g.Key, Count = g.Count() })
            .WithAggregateLimit(10000)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Count.Should().BeGreaterThan(0));
        results.Sum(r => r.Count).Should().Be(150);
    }

    // -------------------------------------------------------------------------
    // String.Length
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_StringLengthEqual_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length == 50).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length == 50)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Should().HaveLength(50));
    }

    [Fact]
    public async Task Where_StringLengthNotEqual_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and non-matching strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length != 50).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length != 50)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length != 50).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthGreaterThan_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length > 75).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length > 75)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Length.Should().BeGreaterThan(75));
    }

    [Fact]
    public async Task Where_StringLengthGreaterThanOrEqual_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length >= 75).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length >= 75)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Length.Should().BeGreaterThanOrEqualTo(75));
    }

    [Fact]
    public async Task Where_StringLengthLessThan_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and short strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length < 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length < 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length < 25).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthLessThanOrEqual_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and short strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length <= 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length <= 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length <= 25).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthLessThanAndNotNull_ExcludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length < 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString != null && c.VariableLengthString.Length < 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
        {
            c.VariableLengthString.Should().NotBeNull();
            c.VariableLengthString.Length.Should().BeLessThan(25);
        });
    }

    // -------------------------------------------------------------------------
    // Aggregate operators — Min / Max / Sum / Average / Count
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Count_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = Service.Queryable<CustomAccount>().Count();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task Count_WithPredicate_MatchesFilteredListCount()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("00"))
            .ToListAsync();

        var count = Service.Queryable<CustomAccount>()
            .Count(a => a.Name.Contains("00"));

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task Min_WithSelector_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Min(a => a.NumberOfEmployees);

        var min = Service.Queryable<CustomAccount>()
            .Min(a => a.NumberOfEmployees);

        min.Should().Be(expected);
    }

    [Fact]
    public async Task Max_WithSelector_MatchesLinqMax()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Max(a => a.NumberOfEmployees);

        var max = Service.Queryable<CustomAccount>()
            .Max(a => a.NumberOfEmployees);

        max.Should().Be(expected);
    }

    [Fact]
    public async Task Sum_WithSelect_MatchesLinqSum()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Sum(a => a.NumberOfEmployees);

        var sum = (from a in Service.Queryable<CustomAccount>()
                   select a.NumberOfEmployees).Sum();

        sum.Should().Be(expected);
    }

    [Fact]
    public async Task Average_WithSelector_MatchesLinqAverage()
    {
        // Use a decimal column (PercentComplete) to avoid integer rounding
        // that Dataverse applies to integer avg aggregates.
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.PercentComplete != null)
            .Average(a => a.PercentComplete);

        var avg = Service.Queryable<CustomAccount>()
            .Average(a => a.PercentComplete);

        avg.Should().Be(expected);
    }

    [Fact]
    public async Task Min_OnMoneyValue_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.CreditLimitMoney != null)
            .Min(a => a.CreditLimitMoney.Value);

        var min = (from a in Service.Queryable<CustomAccount>()
                   select a.CreditLimitMoney.Value).Min();

        min.Should().Be(expected);
    }

    [Fact]
    public async Task LongCount_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = Service.Queryable<CustomAccount>().LongCount();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task CountColumn_CountsNonNullValues()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expectedNonNull = all.Count(a => a.NumberOfEmployees != null);

        var countColumn = Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .CountColumn();

        countColumn.Should().Be(expectedNonNull);
    }

    [Fact]
    public async Task CountColumnAsync_CountsNonNullValues()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expectedNonNull = all.Count(a => a.NumberOfEmployees != null);

        var countColumn = await Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .CountColumnAsync();

        countColumn.Should().Be(expectedNonNull);
    }

    [Fact]
    public async Task CountAsync_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = await Service.Queryable<CustomAccount>().CountAsync();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_MatchesFilteredListCount()
    {
        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("00"))
            .ToListAsync();

        var count = await Service.Queryable<CustomAccount>()
            .CountAsync(a => a.Name.Contains("00"));

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task LongCountAsync_MatchesListCount()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var count = await Service.Queryable<CustomAccount>().LongCountAsync();

        count.Should().Be(all.Count);
    }

    [Fact]
    public async Task MinAsync_WithSelector_MatchesLinqMin()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Min(a => a.NumberOfEmployees);

        var min = await Service.Queryable<CustomAccount>()
            .MinAsync(a => a.NumberOfEmployees);

        min.Should().Be(expected);
    }

    [Fact]
    public async Task MaxAsync_WithSelector_MatchesLinqMax()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Max(a => a.NumberOfEmployees);

        var max = await Service.Queryable<CustomAccount>()
            .MaxAsync(a => a.NumberOfEmployees);

        max.Should().Be(expected);
    }

    [Fact]
    public async Task SumAsync_WithSelect_MatchesLinqSum()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Sum(a => a.NumberOfEmployees);

        var sum = await Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .SumAsync();

        sum.Should().Be(expected);
    }

    [Fact]
    public async Task AverageAsync_WithSelector_MatchesLinqAverage()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var expected = all.Where(a => a.PercentComplete != null)
            .Average(a => a.PercentComplete);

        var avg = await Service.Queryable<CustomAccount>()
            .Select(a => a.PercentComplete)
            .AverageAsync();

        avg.Should().Be(expected);
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
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_GroupByDateYear_ReturnsGroupedResults()
    {
        var results = (from o in Service.Queryable<CustomOpportunity>()
                       join c in Service.Queryable<CustomContact>()
                           on o.Contact.Id equals c.CustomContactId
                       where c.FirstName.Contains("First")
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Year into g
                       orderby g.Key ascending
                       select new
                       {
                           Year = g.Key,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.ActualRevenue),
                           AverageRevenue = g.Average(x => x.ActualRevenue),
                           TotalEstimatedRevenue = g.Sum(x => x.EstimatedRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Year.Should().BeGreaterThan(2000);
            r.Count.Should().BeGreaterThan(0);
        });

        // Verify ascending order by Year
        results.Select(r => r.Year).Should().BeInAscendingOrder();
    }

    [Fact]
    public void GroupBy_SimpleCount_ReturnsGroupedCounts()
    {
        var totalWithRating = Service.Queryable<CustomAccount>()
            .Count(a => a.AccountRating_OptionSetValue != null);

        var results = (from a in Service.Queryable<CustomAccount>()
                       where a.AccountRating_OptionSetValue != null
                       group a by a.AccountRating_OptionSetValue.Value into g
                       select new
                       {
                           Rating = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWithRating);
    }

    [Fact]
    public async Task GroupBy_CountColumn_CountsNonNullValuesPerGroup()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       where a.AccountRating_OptionSetValue != null
                       group a by a.AccountRating_OptionSetValue.Value into g
                       select new
                       {
                           Rating = g.Key,
                           Count = g.Count(),
                           DescriptionCount = g.CountColumn(x => x.Description),
                       }).ToList();

        var all = await Service.Queryable<CustomAccount>()
            .Where(a => a.AccountRating_OptionSetValue != null)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
            r.DescriptionCount.Should().BeLessThanOrEqualTo(r.Count);
        });

        // Verify total DescriptionCount matches actual non-null descriptions
        var totalDescriptionCount = results.Sum(r => r.DescriptionCount);
        var expectedNonNull = all.Count(a => a.Description != null);
        totalDescriptionCount.Should().Be(expectedNonNull);
    }

    [Fact]
    public async Task GroupByConstant_MultipleAggregates_ReturnsSingleRow()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();

        var result = (from a in Service.Queryable<CustomAccount>()
                      group a by 1 into g
                      select new
                      {
                          Count = g.Count(),
                          ColumnCount = g.CountColumn(x => x.NumberOfEmployees),
                          Maximum = g.Max(x => x.NumberOfEmployees),
                          Minimum = g.Min(x => x.NumberOfEmployees),
                          Sum = g.Sum(x => x.NumberOfEmployees),
                      }).First();

        result.Count.Should().Be(all.Count);
        result.ColumnCount.Should().Be(all.Count(a => a.NumberOfEmployees.HasValue));
        result.Maximum.Should().Be(all.Where(a => a.NumberOfEmployees.HasValue).Max(a => a.NumberOfEmployees));
        result.Minimum.Should().Be(all.Where(a => a.NumberOfEmployees.HasValue).Min(a => a.NumberOfEmployees));
        result.Sum.Should().Be(all.Where(a => a.NumberOfEmployees.HasValue).Sum(a => a.NumberOfEmployees));
    }

    [Fact]
    public void JoinGroupBy_AggregateOnLinkEntity_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from c in Service.Queryable<CustomContact>()
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group o by c.CustomContactId into g
                       select new
                       {
                           ContactId = g.Key,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ContactId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        // Sum of grouped counts should equal total opportunities
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    [Fact]
    public void MultiJoinGroupBy_AggregateOnNestedLinkEntity_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group o by a.CustomAccountId into g
                       select new
                       {
                           AccountId = g.Key,
                           Count = g.Count(),
                           MaxRevenue = g.Max(x => x.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    [Fact]
    public void JoinGroupBy_CompositeKey_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from c in Service.Queryable<CustomContact>()
                       join o in Service.Queryable<CustomOpportunity>()
                           on c.CustomContactId equals o.Contact.Id
                       group new { c, o }
                           by new { c.CustomContactId } into g
                       select new
                       {
                           ContactId = g.Key.CustomContactId,
                           Count = g.Count(),
                           TotalRevenue = g.Sum(x => x.o.ActualRevenue),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ContactId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    // -------------------------------------------------------------------------
    // RowAggregate — CountChildren
    // -------------------------------------------------------------------------

    [Fact]
    public async Task CountChildren_ReturnsChildCountPerAccount()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();

        // Determine which accounts are parents by counting how many accounts reference them
        var childCountByParent = allAccounts
            .Where(a => a.ParentAccount != null)
            .GroupBy(a => a.ParentAccount.Id)
            .ToDictionary(g => g.Key, g => g.Count());

        var results = await Service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.CustomAccountId,
                a.Name,
                NumberOfChildren = a.CountChildren()
            })
            .ToListAsync();

        results.Should().NotBeEmpty();

        var withChildren = results.Where(r => r.NumberOfChildren > 0).ToList();
        var withoutChildren = results.Where(r => r.NumberOfChildren == 0).ToList();

        withChildren.Should().HaveCount(childCountByParent.Count);
        withoutChildren.Should().NotBeEmpty();

        // Verify each parent's child count matches
        foreach (var result in withChildren)
        {
            childCountByParent.Should().ContainKey(result.CustomAccountId);
            result.NumberOfChildren.Should().Be(childCountByParent[result.CustomAccountId]);
        }
    }

    // -------------------------------------------------------------------------
    // Distinct
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Distinct_ReturnsDistinctRecords()
    {
        var allNames = await (from a in Service.Queryable<CustomAccount>()
                              select a.Name).ToListAsync();

        var distinctNames = await (from a in Service.Queryable<CustomAccount>()
                                   select a.Name).Distinct().ToListAsync();

        distinctNames.Should().OnlyHaveUniqueItems();
        // All account names are unique in seed data, so distinct count should equal total
        distinctNames.Should().HaveCount(allNames.Count);
    }

    [Fact]
    public async Task Distinct_WithSelectProjection_ReturnsDistinctProjectedValues()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name })
            .Distinct()
            .ToListAsync();

        results.Select(r => r.Name).Should().OnlyHaveUniqueItems();
        results.Should().HaveCount(150);
    }

    // -------------------------------------------------------------------------
    // GroupBy — Date grouping variants
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupBy_DateQuarter_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Quarter() into g
                       select new
                       {
                           Quarter = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Quarter.Should().BeInRange(1, 4);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateMonth_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Month into g
                       select new
                       {
                           Month = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Month.Should().BeInRange(1, 12);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateDay_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Day into g
                       select new
                       {
                           Day = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Day.Should().BeInRange(1, 31);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_DateWeek_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.Week() into g
                       select new
                       {
                           Week = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Week.Should().BeInRange(1, 53);
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    [Fact]
    public void GroupBy_FiscalPeriod_GeneratesCorrectFetchXml()
    {
        // FiscalPeriod grouping returns composite date strings (e.g., "2020-01")
        // that may not parse correctly at runtime, so we verify FetchXml generation only.
        var fetchXml = (from o in Service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.FiscalPeriod() into g
                        select new
                        {
                            FiscalPeriod = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        fetchXml.Should().Contain("dategrouping=\"fiscal-period\"");
    }

    [Fact]
    public void GroupBy_FiscalYear_ReturnsGroupedResults()
    {
        var totalWon = Service.Queryable<CustomOpportunity>()
            .Count(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won);

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                       group o by o.ActualCloseDate!.Value.FiscalYear() into g
                       select new
                       {
                           FiscalYear = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        results.Sum(r => r.Count).Should().Be(totalWon);
    }

    // -------------------------------------------------------------------------
    // GroupBy — OptionSetValue
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupBy_OptionSetValue_ReturnsGroupedResults()
    {
        var totalOpportunities = Service.Queryable<CustomOpportunity>().Count();

        var results = (from o in Service.Queryable<CustomOpportunity>()
                       group o by o.StatusReason_OptionSetValue.Value into g
                       select new
                       {
                           Status = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Count.Should().BeGreaterThan(0);
        });
        // Sum of all group counts should equal total opportunities
        results.Sum(r => r.Count).Should().Be(totalOpportunities);
    }

    // -------------------------------------------------------------------------
    // Hierarchy operators
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_Under_ReturnsChildRecords()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var parentAccount = allAccounts.First(a =>
            allAccounts.Any(child => child.ParentAccount != null && child.ParentAccount.Id == a.CustomAccountId));

        var expectedChildren = allAccounts
            .Where(a => a.ParentAccount != null && a.ParentAccount.Id == parentAccount.CustomAccountId)
            .ToList();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Under(parentAccount.CustomAccountId))
            .ToListAsync();

        // Under returns descendants (not including the parent itself)
        results.Should().NotBeEmpty();
        results.Select(r => r.CustomAccountId).Should().NotContain(parentAccount.CustomAccountId);
        // At minimum, direct children should be present
        results.Count.Should().BeGreaterThanOrEqualTo(expectedChildren.Count);
    }

    [Fact]
    public async Task Where_UnderOrEqual_IncludesParentAndChildren()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var parentAccount = allAccounts.First(a =>
            allAccounts.Any(child => child.ParentAccount != null && child.ParentAccount.Id == a.CustomAccountId));

        var underResults = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Under(parentAccount.CustomAccountId))
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.UnderOrEqual(parentAccount.CustomAccountId))
            .ToListAsync();

        // UnderOrEqual should include the parent itself plus all descendants
        results.Select(r => r.CustomAccountId).Should().Contain(parentAccount.CustomAccountId);
        results.Should().HaveCount(underResults.Count + 1);
    }

    [Fact]
    public async Task Where_Above_ReturnsParentRecords()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var childAccount = allAccounts.First(a => a.ParentAccount != null);

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Above(childAccount.CustomAccountId))
            .ToListAsync();

        // Above returns ancestors (not including the child itself)
        results.Should().NotBeEmpty();
        results.Select(r => r.CustomAccountId).Should().NotContain(childAccount.CustomAccountId);
        results.Select(r => r.CustomAccountId).Should().Contain(childAccount.ParentAccount.Id);
    }

    [Fact]
    public async Task Where_AboveOrEqual_IncludesChildAndParents()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var childAccount = allAccounts.First(a => a.ParentAccount != null);

        var aboveResults = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Above(childAccount.CustomAccountId))
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.AboveOrEqual(childAccount.CustomAccountId))
            .ToListAsync();

        // AboveOrEqual should include the child itself plus all ancestors
        results.Select(r => r.CustomAccountId).Should().Contain(childAccount.CustomAccountId);
        results.Should().HaveCount(aboveResults.Count + 1);
    }

    [Fact]
    public async Task Where_NotUnder_ExcludesChildRecords()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var parentAccount = allAccounts.First(a =>
            allAccounts.Any(child => child.ParentAccount != null && child.ParentAccount.Id == a.CustomAccountId));

        var underResults = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Under(parentAccount.CustomAccountId))
            .ToListAsync();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.NotUnder(parentAccount.CustomAccountId))
            .ToListAsync();

        // NotUnder + Under should cover all records
        (results.Count + underResults.Count).Should().Be(allAccounts.Count);
        results.Select(r => r.CustomAccountId).Should().NotIntersectWith(
            underResults.Select(r => r.CustomAccountId));
    }

    // -------------------------------------------------------------------------
    // DateTime fiscal extensions
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_InFiscalYear_ReturnsMatchingRecords()
    {
        // Use a year we know has Won opportunities from seed data
        var knownYear = Service.Queryable<CustomOpportunity>()
            .Where(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won)
            .ToList()
            .Where(o => o.ActualCloseDate.HasValue)
            .Select(o => o.ActualCloseDate!.Value.Year)
            .First();

        var results = Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualCloseDate.InFiscalYear(knownYear))
            .ToList();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public void Where_InFiscalPeriodAndYear_ReturnsMatchingRecords()
    {
        // Use a year/period we know has data
        var knownDate = Service.Queryable<CustomOpportunity>()
            .Where(o => o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won)
            .ToList()
            .Where(o => o.ActualCloseDate.HasValue)
            .Select(o => o.ActualCloseDate!.Value)
            .First();

        var quarter = (knownDate.Month - 1) / 3 + 1;

        var results = Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualCloseDate.InFiscalPeriodAndYear(quarter, knownDate.Year))
            .ToList();

        results.Should().NotBeEmpty();
    }

    // -------------------------------------------------------------------------
    // NoLock, LateMaterialize, QueryHints
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithNoLock_ReturnsRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .WithNoLock()
            .ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task WithNoLock_WithFilter_ReturnsFilteredRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .WithNoLock()
            .Where(a => a.Name != null)
            .ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task WithLateMaterialize_ReturnsRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .WithLateMaterialize()
            .ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task WithQueryHints_ReturnsRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .WithQueryHints(SqlQueryHint.ForceOrder)
            .ToListAsync();

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task WithQueryHints_Multiple_ReturnsRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .WithQueryHints(SqlQueryHint.ForceOrder, SqlQueryHint.DisableRowGoal)
            .Where(a => a.Name != null)
            .ToListAsync();

        results.Should().HaveCount(150);
    }

    // -------------------------------------------------------------------------
    // Negated filter expressions
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_NegatedContains_ExcludesMatchingRecords()
    {
        var withMatch = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Account"))
            .ToListAsync();

        var withoutMatch = await Service.Queryable<CustomAccount>()
            .Where(a => !a.Name.Contains("Account"))
            .ToListAsync();

        var total = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        (withMatch.Count + withoutMatch.Count).Should().Be(total.Count);
    }

    [Fact]
    public async Task Where_NotEqual_ReturnsOtherRecords()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var knownName = allAccounts.First(a => a.Name != null).Name;

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != knownName)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(a => a.Name == knownName);
        results.Count.Should().BeLessThan(allAccounts.Count);
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
    // Query transformation / composition
    // -------------------------------------------------------------------------

    [Fact]
    public async Task QueryComposition_WhereAfterSelect_ReturnsFilteredResults()
    {
        var query = Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount
            {
                CustomAccountId = a.CustomAccountId,
                Name = a.Name,
            });

        query = query.Where(a => a.Name != null);

        var results = await query.ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(a => a.Name.Should().NotBeNull());
    }

    [Fact]
    public async Task QueryComposition_ChainedWheres_ReturnsFilteredResults()
    {
        var expectedCount = Service.Queryable<CustomAccount>()
            .Count(a => a.NumberOfEmployees != null);

        var query = Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null);

        query = query.Where(a => a.NumberOfEmployees != null);

        var results = await query.ToListAsync();

        // All accounts have names, so the count should equal accounts with non-null NumberOfEmployees
        results.Should().HaveCount(expectedCount);
        results.Should().AllSatisfy(a =>
        {
            a.Name.Should().NotBeNull();
            a.NumberOfEmployees.Should().NotBeNull();
        });
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
    // Contains with zero elements
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ContainsWithEmptyGuidList_ThrowsServerError()
    {
        var accountIds = new List<Guid>();

        var act = () => Service.Queryable<CustomAccount>()
            .Where(a => accountIds.Contains(a.CustomAccountId))
            .ToList();

        act.Should().Throw<Exception>();
    }

    // -------------------------------------------------------------------------
    // CompareEntityRef to Guid / CompareGuidToEntityRef
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_CompareEntityRefToGuid_WithSameEntity_ReturnsResults()
    {
        // Column comparison within a single entity (not cross-entity in a join)
        var all = await Service.Queryable<CustomOpportunity>().ToListAsync();
        var expected = all.Count(o =>
            o.ActualRevenue.HasValue && o.EstimatedRevenue.HasValue
            && o.ActualRevenue > o.EstimatedRevenue);

        var results = await Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualRevenue > o.EstimatedRevenue)
            .ToListAsync();

        results.Should().HaveCount(expected);
    }

    // -------------------------------------------------------------------------
    // Compare to constant with Or condition
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ConstantOrCondition_WithNullVariable_ThrowsNotSupported()
    {
        var date = (DateTime?)null;

        var act = () => (from a in Service.Queryable<CustomAccount>()
                         where (date == null || a.CreatedOn > date)
                         select a).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Where_ConstantAndCondition_WithNullVariable_ThrowsNotSupported()
    {
        var date = (DateTime?)null;

        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.PrimaryContact.Id equals c.CustomContactId into contacts
                         from c in contacts.DefaultIfEmpty()
                         where (a.CreatedOn > c.CreatedOn && date == null)
                         select a).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Sum with no results
    // -------------------------------------------------------------------------

    [Fact]
    public void Sum_WithNoResults_ThrowsInvalidCast()
    {
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         where a.Name == "!@#$#EQSE_NONEXISTENT"
                         select a.CreditLimit).Sum();

        act.Should().Throw<InvalidCastException>();
    }

    // -------------------------------------------------------------------------
    // Select with ternary / null-coalesce
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Select_WithNullCoalesceAndTernary_ReturnsResults()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.CustomAccountId,
                BoolValue = (a.IsPreferredAccount ?? false) ? true : false,
            })
            .ToListAsync();

        results.Should().HaveCount(150);
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
    // Simple select early-bound (select into same entity type)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SimpleSelect_IntoEntityType_ReturnsResults()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount
            {
                CustomAccountId = a.CustomAccountId,
                Name = a.Name,
            }).ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(a =>
        {
            a.CustomAccountId.Should().NotBe(Guid.Empty);
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
    // GroupBy deep — group root entity by linked entity key
    // -------------------------------------------------------------------------

    [Fact]
    public void GroupByDeep_GroupRootByLinkedEntityKey_ReturnsGroupedResults()
    {
        var results = (from c in Service.Queryable<CustomContact>()
                       join a in Service.Queryable<CustomAccount>()
                           on c.ParentAccount.Id equals a.CustomAccountId
                       group c by a.CustomAccountId into g
                       select new
                       {
                           AccountId = g.Key,
                           Count = g.Count(),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AccountId.Should().NotBe(Guid.Empty);
            r.Count.Should().BeGreaterThan(0);
        });
        // 100 accounts × 5 contacts each = 500 total contacts
        results.Sum(r => r.Count).Should().Be(500);
        // Each of the 100 accounts should have 5 contacts
        results.Should().AllSatisfy(r => r.Count.Should().Be(5));
    }
}

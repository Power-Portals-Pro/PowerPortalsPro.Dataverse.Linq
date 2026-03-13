using Dataverse.Linq.Extensions;
using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;

namespace Dataverse.Linq.Tests;

public class QueryableIntegrationTests : IntegrationTestBase
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

        results.Should().NotBeEmpty();
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

        results.Should().NotBeEmpty();
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
    // Where — ContainValues / DoesNotContainValues (multi-select option set)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereContainValues_ReturnsMatchingRecords()
    {
        var red = (int)CustomContact.Color.Red;
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.ContainValues(red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors_OptionSetValues != null
            && r.FavoriteColors_OptionSetValues.Any(o => o.Value == red));
    }

    [Fact]
    public async Task ToListAsync_WhereContainValuesMultiple_ReturnsMatchingRecords()
    {
        var red = (int)CustomContact.Color.Red;
        var blue = (int)CustomContact.Color.Blue;
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.ContainValues(red, blue))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors_OptionSetValues != null
            && r.FavoriteColors_OptionSetValues.Any(o => o.Value == red || o.Value == blue));
    }

    [Fact]
    public async Task ToListAsync_WhereDoesNotContainValues_ExcludesMatchingRecords()
    {
        var red = (int)CustomContact.Color.Red;
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.DoesNotContainValues(red))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.FavoriteColors_OptionSetValues == null
            || r.FavoriteColors_OptionSetValues.All(o => o.Value != red));
    }

    [Fact]
    public async Task ToListAsync_WhereNegatedContainValues_ReturnsSameAsDoesNotContainValues()
    {
        var red = (int)CustomContact.Color.Red;

        var doesNotContain = await Service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.DoesNotContainValues(red))
            .ToListAsync();

        var negatedContain = await Service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors_OptionSetValues.ContainValues(red))
            .ToListAsync();

        negatedContain.Should().HaveCount(doesNotContain.Count);
        negatedContain.Select(r => r.CustomContactId).Should().BeEquivalentTo(doesNotContain.Select(r => r.CustomContactId));
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

}

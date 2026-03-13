using Dataverse.Linq.Tests.ProxyClasses;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

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
                                 on a.AccountId equals c.ParentAccountId
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
                                 on a.AccountId equals c.ParentAccountId
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().HaveCount(500);
    }

    [Fact]
    public async Task ToListAsync_WithJoin_AccountNameIsPopulatedOnAllRows()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.AccountId equals c.ParentAccountId
                             select new { a.Name, c.FirstName, c.LastName }).ToListAsync();

        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ToListAsync_WithJoin_ContactNamesArePopulatedOnAllRows()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.AccountId equals c.ParentAccountId
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
                                 on a.AccountId equals c.ParentAccountId
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
                                 on a.AccountId equals c.ParentAccount.Id into contacts
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
                                 on a.AccountId equals c.ParentAccount.Id into contacts
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
                                 on a.AccountId equals c.ParentAccountId
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
                                 on a.AccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new { a.Name }).ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Name.Should().Match(n => n.StartsWith("Custom Account") || n.StartsWith("Empty Account")));
    }
}

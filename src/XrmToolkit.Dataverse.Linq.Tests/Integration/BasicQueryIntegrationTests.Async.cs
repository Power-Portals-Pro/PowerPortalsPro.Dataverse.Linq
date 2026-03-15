using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class BasicQueryIntegrationTests
{
    // --- Basic retrieval ---

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

    // --- Data verification ---

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

    // --- Column selection ---

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

    // --- Select projection ---

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

    // --- Ordering ---

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

    // --- Distinct ---

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

    // --- Simple select into entity type ---

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

    // --- Select with ternary/null-coalesce ---

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

    // --- Query composition ---

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
}

using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class GroupByIntegrationTests
{
    // -------------------------------------------------------------------------
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

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
                          ColumnCount = g.CountColumn(x => x.NumberOfEmployees!),
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
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Extensions;
using XrmToolkit.Dataverse.Linq.Model;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class SpecialFilterIntegrationTests
{
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
    // NoLock, LateMaterialize, QueryHints
    // -------------------------------------------------------------------------

    [Fact]
    public async Task WithNoLock_ReturnsRecords()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        var results = await Service.Queryable<CustomAccount>()
            .WithNoLock()
            .ToListAsync();
#pragma warning restore CS0618 // Type or member is obsolete

        results.Should().HaveCount(150);
    }

    [Fact]
    public async Task WithNoLock_WithFilter_ReturnsFilteredRecords()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        var results = await Service.Queryable<CustomAccount>()
            .WithNoLock()
            .Where(a => a.Name != null)
            .ToListAsync();
#pragma warning restore CS0618 // Type or member is obsolete

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
}

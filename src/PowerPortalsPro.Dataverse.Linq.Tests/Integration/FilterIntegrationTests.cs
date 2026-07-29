using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class FilterIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

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
    // Contains over collection types other than arrays
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ContainsWithStringList_ReturnsMatchingRecords()
    {
        var names = new List<string> { "Custom Account 001", "Custom Account 002" };

        var results = Service.Queryable<CustomAccount>()
            .Where(a => names.Contains(a.Name))
            .ToList();

        results.Should().HaveCount(2);
        results.Select(a => a.Name).Should().BeEquivalentTo(names);
    }

    [Fact]
    public void Where_ContainsWithGuidList_ReturnsMatchingRecords()
    {
        var ids = Service.Queryable<CustomAccount>()
            .Select(a => new { a.CustomAccountId })
            .ToList()
            .Take(2)
            .Select(a => a.CustomAccountId)
            .ToList();

        var results = Service.Queryable<CustomAccount>()
            .Where(a => ids.Contains(a.CustomAccountId))
            .ToList();

        results.Should().HaveCount(ids.Count);
        results.Select(a => a.CustomAccountId).Should().BeEquivalentTo(ids);
    }

    // -------------------------------------------------------------------------
    // Contains over a collection of enums — the option set integers must reach
    // the server, not the enum member names
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ContainsWithEnumList_ReturnsMatchingRecords()
    {
        var ratings = new List<CustomAccount.AccountRating_Enum>
        {
            CustomAccount.AccountRating_Enum.Hot,
            CustomAccount.AccountRating_Enum.Warm
        };

        var expected = Service.Queryable<CustomAccount>()
            .ToList()
            .Count(a => a.AccountRating.HasValue && ratings.Contains(a.AccountRating.Value));

        var results = Service.Queryable<CustomAccount>()
            .Where(a => ratings.Contains(a.AccountRating!.Value))
            .ToList();

        results.Should().HaveCount(expected);
        results.Should().NotBeEmpty();
        results.Should().OnlyContain(a => ratings.Contains(a.AccountRating!.Value));
    }

    [Fact]
    public void Where_ContainsWithEnumArray_ReturnsMatchingRecords()
    {
        var ratings = new[]
        {
            CustomAccount.AccountRating_Enum.Hot,
            CustomAccount.AccountRating_Enum.Warm
        };

        var expected = Service.Queryable<CustomAccount>()
            .ToList()
            .Count(a => a.AccountRating.HasValue && ratings.Contains(a.AccountRating.Value));

        var results = Service.Queryable<CustomAccount>()
            .Where(a => ratings.Contains(a.AccountRating!.Value))
            .ToList();

        results.Should().HaveCount(expected);
        results.Should().NotBeEmpty();
        results.Should().OnlyContain(a => ratings.Contains(a.AccountRating!.Value));
    }

    [Fact]
    public void Where_NegatedContainsWithEnumList_ExcludesMatchingRecords()
    {
        var ratings = new List<CustomAccount.AccountRating_Enum> { CustomAccount.AccountRating_Enum.Hot };

        var results = Service.Queryable<CustomAccount>()
            .Where(a => !ratings.Contains(a.AccountRating!.Value))
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(a => a.AccountRating != CustomAccount.AccountRating_Enum.Hot);
    }

    // -------------------------------------------------------------------------
    // Captured entity property — should evaluate as value, not column reference
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_LookupIdEqualsCapturedEntityProperty_ReturnsResults()
    {
        // Retrieve any account to use as a captured variable
        var account = Service.Queryable<CustomAccount>()
            .Select(a => new { a.CustomAccountId })
            .FirstOrDefault();

        if (account is null)
            return; // Skip if no test data

        // This should generate a value filter, not a column-to-column (valueof) comparison
        var contacts = Service.Queryable<CustomContact>()
            .Where(c => c.ParentAccount.Id == account.CustomAccountId)
            .Select(c => new { c.CustomContactId })
            .ToList();

        contacts.Should().NotBeNull();
    }
}

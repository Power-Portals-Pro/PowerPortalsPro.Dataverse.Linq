using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// <c>GetAttributeValue&lt;T&gt;(name)</c> where the attribute name is supplied by a
/// variable, property, method call, or other expression resolved at translation time.
/// </summary>
public partial class DynamicAttributeNameIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private sealed class AttributeNames
    {
        public string Name { get; } = "new_name";
        public string GetWebsite() => "new_website";
    }

    // -------------------------------------------------------------------------
    // Where — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_GetAttributeValueWithVariableName_FiltersOnThatAttribute()
    {
        var nameAttribute = "new_name";

        var expected = Service.Queryable("new_customaccount")
            .ToList()
            .First(e => !string.IsNullOrEmpty(e.GetAttributeValue<string>("new_name")))
            .GetAttributeValue<string>("new_name");

        var results = Service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(nameAttribute) == expected)
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.GetAttributeValue<string>("new_name").Should().Be(expected));
    }

    [Fact]
    public void Where_GetAttributeValueWithVariableName_MatchesConstantNameResults()
    {
        var nameAttribute = "new_name";

        var withConstant = Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToList();

        var withVariable = Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(nameAttribute)))
            .ToList();

        withVariable.Should().NotBeEmpty();
        withVariable.Select(e => e.Id).Should().BeEquivalentTo(withConstant.Select(e => e.Id));
    }

    [Fact]
    public void Where_GetAttributeValueWithMethodCallName_FiltersOnThatAttribute()
    {
        var names = new AttributeNames();

        var results = Service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(names.GetWebsite()) != null)
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.GetAttributeValue<string>("new_website").Should().NotBeNull());
    }

    [Fact]
    public void Where_GetAttributeValueWithLoopVariableName_FiltersEachAttribute()
    {
        foreach (var attribute in new[] { "new_name", "new_website" })
        {
            var results = Service.Queryable("new_customaccount")
                .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(attribute)))
                .ToList();

            results.Should().NotBeEmpty();
            results.Should().AllSatisfy(r =>
                r.GetAttributeValue<string>(attribute).Should().NotBeNullOrEmpty());
        }
    }

    [Fact]
    public void Where_GetAttributeValueEntityReferenceIdWithVariableName_FiltersOnLookup()
    {
        var lookupAttribute = "new_parentaccount";

        var accountId = Service.Queryable("new_customcontact")
            .ToList()
            .First(e => e.GetAttributeValue<EntityReference>("new_parentaccount") is not null)
            .GetAttributeValue<EntityReference>("new_parentaccount").Id;

        var results = Service.Queryable("new_customcontact")
            .Where(x => x.GetAttributeValue<EntityReference>(lookupAttribute).Id == accountId)
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.GetAttributeValue<EntityReference>("new_parentaccount").Id.Should().Be(accountId));
    }

    // -------------------------------------------------------------------------
    // Select — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void Select_GetAttributeValueWithVariableNames_ReturnsProjectedValues()
    {
        var names = new AttributeNames();
        var websiteAttribute = names.GetWebsite();

        var results = Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(names.Name)))
            .Select(x => new
            {
                Name = x.GetAttributeValue<string>(names.Name),
                Website = x.GetAttributeValue<string>(websiteAttribute),
            })
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
        results.Should().Contain(r => r.Website != null);
    }

    [Fact]
    public void OrderBy_GetAttributeValueWithVariableName_SortsOnThatAttribute()
    {
        var nameAttribute = "new_name";

        var results = Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(nameAttribute)))
            .OrderBy(x => x.GetAttributeValue<string>(nameAttribute))
            .Select(x => new { Name = x.GetAttributeValue<string>(nameAttribute) })
            .ToList();

        results.Should().NotBeEmpty();
        results.Select(r => r.Name).Should().BeInAscendingOrder(StringComparer.OrdinalIgnoreCase);
    }

    // -------------------------------------------------------------------------
    // Joins — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void Join_GetAttributeValueWithVariableName_ReturnsResults()
    {
        var lookupAttribute = "new_parentaccount";
        var nameAttribute = "new_name";

        var results = (from a in Service.Queryable("new_customaccount")
                       join c in Service.Queryable("new_customcontact")
                           on a.Id equals c.GetAttributeValue<EntityReference>(lookupAttribute).Id
                       select new { Account = a, Contact = c }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Account.Should().NotBeNull();
            r.Account.GetAttributeValue<string>(nameAttribute).Should().NotBeNullOrEmpty();
            r.Contact.Should().NotBeNull();
            r.Contact.Id.Should().NotBe(Guid.Empty);
        });
    }

    [Fact]
    public void LeftJoin_SelectGetAttributeValueWithVariableNames_MaterializesInnerColumns()
    {
        var firstNameAttribute = "new_firstname";
        var lastNameAttribute = "new_lastname";

        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new
                       {
                           a.Name,
                           FirstName = c.GetAttributeValue<string>(firstNameAttribute),
                           LastName = c.GetAttributeValue<string>(lastNameAttribute),
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());

        var withContacts = results.Where(r => r.FirstName != null).ToList();
        withContacts.Should().NotBeEmpty("some accounts have contacts");
        withContacts.Should().AllSatisfy(r =>
        {
            r.FirstName.Should().NotBeNullOrEmpty();
            r.LastName.Should().NotBeNullOrEmpty();
        });
    }
}

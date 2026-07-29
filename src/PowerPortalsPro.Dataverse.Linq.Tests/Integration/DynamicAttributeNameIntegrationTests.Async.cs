using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class DynamicAttributeNameIntegrationTests
{
    // -------------------------------------------------------------------------
    // Where / Select — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereGetAttributeValueWithVariableName_MatchesConstantNameResults()
    {
        var nameAttribute = "new_name";

        var withConstant = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToListAsync();

        var withVariable = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(nameAttribute)))
            .ToListAsync();

        withVariable.Should().NotBeEmpty();
        withVariable.Select(e => e.Id).Should().BeEquivalentTo(withConstant.Select(e => e.Id));
    }

    [Fact]
    public async Task ToListAsync_SelectGetAttributeValueWithVariableNames_ReturnsProjectedValues()
    {
        var names = new AttributeNames();
        var websiteAttribute = names.GetWebsite();

        var results = await Service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(names.Name)))
            .Select(x => new
            {
                Name = x.GetAttributeValue<string>(names.Name),
                Website = x.GetAttributeValue<string>(websiteAttribute),
            })
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNullOrEmpty());
        results.Should().Contain(r => r.Website != null);
    }

    [Fact]
    public async Task ToListAsync_WhereGetAttributeValueWithLoopVariableName_FiltersEachAttribute()
    {
        foreach (var attribute in new[] { "new_name", "new_website" })
        {
            var results = await Service.Queryable("new_customaccount")
                .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(attribute)))
                .ToListAsync();

            results.Should().NotBeEmpty();
            results.Should().AllSatisfy(r =>
                r.GetAttributeValue<string>(attribute).Should().NotBeNullOrEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Joins — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_JoinGetAttributeValueWithVariableName_ReturnsResults()
    {
        var lookupAttribute = "new_parentaccount";
        var nameAttribute = "new_name";

        var results = await (from a in Service.Queryable("new_customaccount")
                             join c in Service.Queryable("new_customcontact")
                                 on a.Id equals c.GetAttributeValue<EntityReference>(lookupAttribute).Id
                             select new { Account = a, Contact = c }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Account.GetAttributeValue<string>(nameAttribute).Should().NotBeNullOrEmpty();
            r.Contact.Id.Should().NotBe(Guid.Empty);
        });
    }

    [Fact]
    public async Task ToListAsync_LeftJoinSelectGetAttributeValueWithVariableNames_MaterializesInnerColumns()
    {
        var firstNameAttribute = "new_firstname";
        var lastNameAttribute = "new_lastname";

        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id into contacts
                             from c in contacts.DefaultIfEmpty()
                             select new
                             {
                                 a.Name,
                                 FirstName = c.GetAttributeValue<string>(firstNameAttribute),
                                 LastName = c.GetAttributeValue<string>(lastNameAttribute),
                             }).ToListAsync();

        results.Should().NotBeEmpty();

        var withContacts = results.Where(r => r.FirstName != null).ToList();
        withContacts.Should().NotBeEmpty("some accounts have contacts");
        withContacts.Should().AllSatisfy(r =>
        {
            r.FirstName.Should().NotBeNullOrEmpty();
            r.LastName.Should().NotBeNullOrEmpty();
        });
    }
}

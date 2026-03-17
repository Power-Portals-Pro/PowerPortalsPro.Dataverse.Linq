using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class UnboundEntityIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Unbound entity join
    // -------------------------------------------------------------------------

    [Fact]
    public void UnboundEntityJoin_WithGetAttributeValueIdKey_ReturnsResults()
    {
        var results = (from a in Service.Queryable("new_customaccount")
                       join c in Service.Queryable("new_customcontact")
                           on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id
                       select new { Account = a, Contact = c }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Account.Should().NotBeNull();
            r.Account.Id.Should().NotBe(Guid.Empty);
            r.Account.GetAttributeValue<string>("new_name").Should().NotBeNullOrEmpty();
            r.Contact.Should().NotBeNull();
            r.Contact.Id.Should().NotBe(Guid.Empty);
        });
    }
}

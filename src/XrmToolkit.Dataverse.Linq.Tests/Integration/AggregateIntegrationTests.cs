using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class AggregateIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

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
}

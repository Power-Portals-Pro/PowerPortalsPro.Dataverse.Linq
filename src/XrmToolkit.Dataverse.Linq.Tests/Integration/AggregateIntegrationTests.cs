using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class AggregateIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

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

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class ElementOperatorIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    // -------------------------------------------------------------------------
    // Terminal operators — First / FirstOrDefault / Single / SingleOrDefault
    // -------------------------------------------------------------------------

    [Fact]
    public void First_Sync_ReturnsFirstRecord()
    {
        var result = Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .First();

        result.Should().NotBeNull();
        result.Name.Should().NotBeNull();
    }

    [Fact]
    public void FirstOrDefault_Sync_WithNoMatch_ReturnsNull()
    {
        var result = Service.Queryable<CustomAccount>()
            .FirstOrDefault(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }
}

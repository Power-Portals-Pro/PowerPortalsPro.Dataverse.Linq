using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class ElementOperatorIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

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

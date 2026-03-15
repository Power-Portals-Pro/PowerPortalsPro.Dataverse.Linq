using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class BasicQueryIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    #if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif
}

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class JoinIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // Null record on join
    // -------------------------------------------------------------------------

    [Fact]
    public void Join_NullPropertyOnJoinedRecord_DoesNotThrow()
    {
        var result = (from c in Service.Queryable<CustomContact>()
                      join a in Service.Queryable<CustomAccount>()
                          on c.ParentAccount.Id equals a.CustomAccountId
                      select new
                      {
                          c.Name,
                          a.Website,
                      }).FirstOrDefault();

        result.Should().NotBeNull();
        result!.Name.Should().NotBeNullOrEmpty();
        // Website may be null for some accounts, but the query should not throw
    }
}

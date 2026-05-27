using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class LookupProjectionIntegrationTests
{
    [Fact]
    public async Task ProjectLookupFromWithFirstRowJoinAsync_NotNull()
    {
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                            .OrderByDescending(p => p.CreatedOn)
                                            .WithFirstRow()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             select new
                             {
                                 a.Name,
                                 ParticipantId = c.ParentAccount,
                                 LastInteractionOn = c.CreatedOn,
                             }).ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ParticipantId.Should().NotBeNull();
            r.ParticipantId!.Id.Should().NotBe(Guid.Empty);
            r.LastInteractionOn.Should().NotBeNull();
        });
    }
}

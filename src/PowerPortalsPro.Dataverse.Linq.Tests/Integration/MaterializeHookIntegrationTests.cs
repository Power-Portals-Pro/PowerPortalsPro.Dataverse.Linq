using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Integration coverage for the OnBeforeMaterialize / OnAfterMaterialize transform hooks
/// (inline and global) against a live org.
/// </summary>
public class MaterializeHookIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    [Fact]
    public void OnAfterMaterialize_EnrichesEachResult()
    {
        var results = Service.Queryable<CustomContact>()
            .Take(10)
            .OnAfterMaterialize((source, c) =>
            {
                // Enrich the result from the source row.
                c.VariableLengthString = source.GetAttributeValue<Guid>("new_customcontactid").ToString();
                return c;
            })
            .ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(c => c.VariableLengthString.Should().Be(c.Id.ToString()));
    }

    [Fact]
    public void OnBeforeMaterialize_RunsForEveryRow()
    {
        var seen = 0;

        var results = Service.Queryable<CustomContact>()
            .Take(10)
            .OnBeforeMaterialize(e => { seen++; return e; })
            .ToList();

        results.Should().NotBeEmpty();
        seen.Should().Be(results.Count);
    }

    [Fact]
    public void GlobalHooks_FireForEveryMaterializedRow()
    {
        var before = 0;
        var after = 0;
        DataverseQueryDiagnostics.BeforeMaterialize = e => { before++; return e; };
        DataverseQueryDiagnostics.AfterMaterialize = (e, o) => { after++; return o; };
        try
        {
            var results = Service.Queryable<CustomContact>().Take(5).ToList();

            results.Should().NotBeEmpty();
            before.Should().Be(results.Count);
            after.Should().Be(results.Count);
        }
        finally
        {
            DataverseQueryDiagnostics.BeforeMaterialize = null;
            DataverseQueryDiagnostics.AfterMaterialize = null;
        }
    }
}

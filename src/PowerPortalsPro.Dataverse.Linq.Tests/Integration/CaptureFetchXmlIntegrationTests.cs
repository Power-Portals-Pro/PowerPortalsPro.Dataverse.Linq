using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Integration tests for FetchXml capture: the per-query <c>CaptureFetchXml</c> callback,
/// the global <see cref="DataverseQueryDiagnostics"/> hook, and the non-executing
/// <c>ToFetchXml(q =&gt; q.Terminal())</c> inspector, exercised against a live org.
/// </summary>
public partial class CaptureFetchXmlIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    [Fact]
    public void CaptureFetchXml_OnList_CapturesFetchXmlAndReturnsResults()
    {
        string? captured = null;

        var results = Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .CaptureFetchXml(xml => captured = xml)
            .ToList();

        results.Should().NotBeEmpty();
        captured.Should().NotBeNull();
        captured.Should().Contain("<entity name=\"new_customaccount\"");
        captured.Should().Contain("operator=\"not-null\"");
    }

    [Fact]
    public void CaptureFetchXml_OnCount_CapturesAggregateFetchXml()
    {
        string? captured = null;

        var count = Service.Queryable<CustomAccount>()
            .CaptureFetchXml(xml => captured = xml)
            .Count();

        count.Should().BeGreaterThan(0);
        captured.Should().NotBeNull();
        captured.Should().Contain("aggregate=\"count\"");
    }

    [Fact]
    public void CaptureFetchXml_OnFirst_CapturesTop1FetchXml()
    {
        string? captured = null;

        var first = Service.Queryable<CustomContact>()
            .CaptureFetchXml(xml => captured = xml)
            .First();

        first.Should().NotBeNull();
        captured.Should().NotBeNull();
        captured.Should().Contain("top=\"1\"");
    }

    [Fact]
    public void CaptureFetchXml_MultiPage_FiresOncePerPageWithPagingCookie()
    {
        var captured = new List<string>();

        // 500 seeded contacts with a small page size forces server-side paging.
        var results = Service.Queryable<CustomContact>()
            .CaptureFetchXml(captured.Add)
            .WithPageSize(50)
            .ToList();

        results.Should().NotBeEmpty();
        captured.Count.Should().BeGreaterThan(1, "a small page size over many rows pages multiple times");
        captured[0].Should().NotContain("paging-cookie");
        captured.Skip(1).Should().AllSatisfy(xml => xml.Should().Contain("paging-cookie"));
    }

    [Fact]
    public void GlobalHook_FiresForEveryQuery()
    {
        var captured = new List<string>();
        void Handler(string xml) => captured.Add(xml);
        DataverseQueryDiagnostics.FetchXmlRequested += Handler;
        try
        {
            _ = Service.Queryable<CustomAccount>().Take(1).ToList();
            _ = Service.Queryable<CustomContact>().Count();
        }
        finally
        {
            DataverseQueryDiagnostics.FetchXmlRequested -= Handler;
        }

        captured.Should().HaveCountGreaterThanOrEqualTo(2);
        captured.Should().Contain(xml => xml.Contains("new_customaccount"));
        captured.Should().Contain(xml => xml.Contains("new_customcontact"));
    }

    [Fact]
    public void ToFetchXml_TerminalInspector_DoesNotExecute()
    {
        // The inspector must not issue a request: the global hook (which fires only on
        // real requests) must stay silent.
        var requests = new List<string>();
        void Handler(string xml) => requests.Add(xml);
        DataverseQueryDiagnostics.FetchXmlRequested += Handler;
        try
        {
            var fetchXml = Service.Queryable<CustomAccount>().ToFetchXml(q => q.Count());
            fetchXml.Should().Contain("aggregate=\"count\"");
        }
        finally
        {
            DataverseQueryDiagnostics.FetchXmlRequested -= Handler;
        }

        requests.Should().BeEmpty();
    }
}

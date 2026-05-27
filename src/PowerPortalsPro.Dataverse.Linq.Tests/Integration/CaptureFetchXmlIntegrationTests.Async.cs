using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class CaptureFetchXmlIntegrationTests
{
    [Fact]
    public async Task CaptureFetchXmlAsync_OnListAsync_CapturesFetchXml()
    {
        string? captured = null;

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .CaptureFetchXml(xml => captured = xml)
            .ToListAsync();

        results.Should().NotBeEmpty();
        captured.Should().NotBeNull();
        captured.Should().Contain("<entity name=\"new_customaccount\"");
    }

    [Fact]
    public async Task CaptureFetchXmlAsync_MultiPage_FiresOncePerPage()
    {
        var captured = new List<string>();

        var results = await Service.Queryable<CustomContact>()
            .CaptureFetchXml(captured.Add)
            .WithPageSize(50)
            .ToListAsync();

        results.Should().NotBeEmpty();
        captured.Count.Should().BeGreaterThan(1);
        captured.Skip(1).Should().AllSatisfy(xml => xml.Should().Contain("paging-cookie"));
    }

    [Fact]
    public async Task GlobalHookAsync_FiresForEveryRequest()
    {
        var captured = new List<string>();
        void Handler(string xml) => captured.Add(xml);
        DataverseQueryDiagnostics.FetchXmlRequested += Handler;
        try
        {
            _ = await Service.Queryable<CustomAccount>().Take(1).ToListAsync();
        }
        finally
        {
            DataverseQueryDiagnostics.FetchXmlRequested -= Handler;
        }

        captured.Should().Contain(xml => xml.Contains("new_customaccount"));
    }
}

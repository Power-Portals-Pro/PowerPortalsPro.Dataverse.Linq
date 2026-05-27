using FluentAssertions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Tests for FetchXml visibility on every execution path: the non-executing
/// <c>ToFetchXml(q =&gt; q.Terminal())</c> inspector, the per-query
/// <c>CaptureFetchXml</c> callback, and the global
/// <see cref="DataverseQueryDiagnostics"/> hook.
/// </summary>
public class CaptureFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Non-executing terminal inspector — ToFetchXml(q => q.Terminal())
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TerminalCount_ReturnsAggregateCountFetchXml()
    {
        var fetchXml = _service.Queryable<CustomAccount>().ToFetchXml(q => q.Count());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TerminalMaxWithSelector_ReturnsAggregateMaxFetchXml()
    {
        var fetchXml = _service.Queryable<CustomAccount>().ToFetchXml(q => q.Max(a => a.NumberOfEmployees));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TerminalFirstOrDefault_SetsTop1()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToFetchXml(q => q.FirstOrDefault());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TerminalWithComposedOperators_TranslatesWholeChain()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Where(a => a.NumberOfEmployees > 5).Count());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="5" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Per-query callback — CaptureFetchXml
    // -------------------------------------------------------------------------

    [Fact]
    public void CaptureFetchXml_OnList_InvokesCallbackWithRequestFetchXml()
    {
        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { MoreRecords = false });

        string? captured = null;
        _ = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .CaptureFetchXml(xml => captured = xml)
            .ToList();

        captured.Should().NotBeNull();
        captured.Should().Contain("<entity name=\"new_customaccount\"");
        captured.Should().Contain("operator=\"not-null\"");
    }

    [Fact]
    public void CaptureFetchXml_OnCount_InvokesCallbackWithAggregateFetchXml()
    {
        var aggregate = new Entity("new_customaccount");
        aggregate["count"] = new AliasedValue("new_customaccount", "count", 42);
        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { Entities = { aggregate }, MoreRecords = false });

        string? captured = null;
        var count = _service.Queryable<CustomAccount>()
            .CaptureFetchXml(xml => captured = xml)
            .Count();

        count.Should().Be(42);
        captured.Should().NotBeNull();
        captured.Should().Contain("aggregate=\"count\"");
    }

    [Fact]
    public void CaptureFetchXml_MultiPage_FiresOncePerRequestWithPagingCookie()
    {
        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(
                new EntityCollection { MoreRecords = true, PagingCookie = "<cookie/>" },
                new EntityCollection { MoreRecords = false });

        var captured = new List<string>();
        _ = _service.Queryable<CustomAccount>()
            .CaptureFetchXml(captured.Add)
            .ToList();

        captured.Should().HaveCount(2);
        captured[0].Should().NotContain("paging-cookie");
        captured[1].Should().Contain("paging-cookie");
        captured[1].Should().Contain("page=\"2\"");
    }

    // -------------------------------------------------------------------------
    // Global hook — DataverseQueryDiagnostics.FetchXmlRequested
    // -------------------------------------------------------------------------

    [Fact]
    public void GlobalHook_FiresForEveryQuery()
    {
        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { MoreRecords = false });

        var captured = new List<string>();
        void Handler(string xml) => captured.Add(xml);
        DataverseQueryDiagnostics.FetchXmlRequested += Handler;
        try
        {
            _ = _service.Queryable<CustomAccount>().ToList();
        }
        finally
        {
            DataverseQueryDiagnostics.FetchXmlRequested -= Handler;
        }

        captured.Should().ContainSingle()
            .Which.Should().Contain("<entity name=\"new_customaccount\"");
    }

    [Fact]
    public void GlobalHook_AfterUnsubscribe_DoesNotFire()
    {
        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { MoreRecords = false });

        var captured = new List<string>();
        void Handler(string xml) => captured.Add(xml);
        DataverseQueryDiagnostics.FetchXmlRequested += Handler;
        DataverseQueryDiagnostics.FetchXmlRequested -= Handler;

        _ = _service.Queryable<CustomAccount>().ToList();

        captured.Should().BeEmpty();
    }
}

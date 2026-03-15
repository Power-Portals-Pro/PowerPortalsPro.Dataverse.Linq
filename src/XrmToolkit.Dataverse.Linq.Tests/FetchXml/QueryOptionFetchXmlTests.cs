using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Model;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

public class QueryOptionFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // WithDatasource
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithDatasourceRetained_GeneratesDatasourceAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithDatasource(FetchDatasource.Retained)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" datasource="retained">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithDatasourceRetainedAndFilter_GeneratesDatasourceWithFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .WithDatasource(FetchDatasource.Retained)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" datasource="retained">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithLateMaterialize
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithLateMaterialize_GeneratesLateMaterializeAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithLateMaterialize()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" latematerialize="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLateMaterializeAndFilter_GeneratesLateMaterializeWithFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .WithLateMaterialize()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" latematerialize="true">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithNoLock
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithNoLock_GeneratesNoLockAttribute()
    {
#pragma warning disable CS0618 // Obsolete
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithNoLock()
            .ToFetchXml();
#pragma warning restore CS0618

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" no-lock="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithNoLockAndFilter_GeneratesNoLockWithFilter()
    {
#pragma warning disable CS0618 // Obsolete
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .WithNoLock()
            .ToFetchXml();
#pragma warning restore CS0618

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" no-lock="true">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithQueryHints
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithQueryHintsSingle_GeneratesOptionsAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithQueryHints(SqlQueryHint.ForceOrder)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" options="ForceOrder">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithQueryHintsMultiple_GeneratesCommaDelimitedOptionsAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithQueryHints(SqlQueryHint.LoopJoin, SqlQueryHint.DisableRowGoal, SqlQueryHint.NoPerformanceSpool)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" options="LoopJoin,DisableRowGoal,NO_PERFORMANCE_SPOOL">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithQueryHintsAndFilter_GeneratesOptionsWithFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .WithQueryHints(SqlQueryHint.HashJoin, SqlQueryHint.EnableHistAmendmentForAscKeys)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" options="HashJoin,ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithUseRawOrderBy
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithUseRawOrderBy_GeneratesUseRawOrderByAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithUseRawOrderBy()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" useraworderby="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithUseRawOrderByAndOrderBy_GeneratesUseRawOrderByWithOrder()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .WithUseRawOrderBy()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" useraworderby="true">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="false" />
              </entity>
            </fetch>
            """);
    }
}

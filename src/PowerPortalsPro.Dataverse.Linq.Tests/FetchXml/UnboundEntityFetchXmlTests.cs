using FluentAssertions;
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public class UnboundEntityFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Unbound entity queries
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundEntity_NoOperators_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable("new_customaccount").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundEntity_WithExplicitColumns_GeneratesSpecificAttributes()
    {
        var fetchXml = _service.Queryable("new_customaccount", "new_name", "new_website").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — GetAttributeValue (unbound)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundEntity_WhereNotIsNullOrEmpty_GeneratesNotNullAndNotEmptyFilter()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                  <condition attribute="new_name" operator="ne" value="" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundEntity_WhereIsNullOrEmpty_GeneratesNullOrEmptyFilter()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="or">
                  <condition attribute="new_name" operator="null" />
                  <condition attribute="new_name" operator="eq" value="" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Select — GetAttributeValue (unbound)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundEntity_SelectWithGetAttributeValue_GeneratesAttributes()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name") })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundEntity_WhereAndSelect_GeneratesFilterAndAttributes()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>("new_name")))
            .Select(x => new { Name = x.GetAttributeValue<string>("new_name"), Website = x.GetAttributeValue<string>("new_website") })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                  <condition attribute="new_name" operator="ne" value="" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Unbound entity join
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundEntityJoin_WithGetAttributeValueIdKey_GeneratesJoin()
    {
        var fetchXml = (from pt in _service.Queryable("plugintype")
                        join pa in _service.Queryable("pluginassembly")
                            on pt.GetAttributeValue<EntityReference>("pluginassemblyid").Id equals pa.Id
                        where pa.GetAttributeValue<string>("name") == "TestAssembly"
                        select new
                        {
                            pa,
                            pt,
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="plugintype">
                <all-attributes />
                <filter type="and">
                  <condition entityname="pa" attribute="name" operator="eq" value="TestAssembly" />
                </filter>
                <link-entity name="pluginassembly" from="pluginassemblyid" to="pluginassemblyid" alias="pa" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }
}

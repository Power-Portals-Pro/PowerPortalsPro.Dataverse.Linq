using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public class RecordCountFetchXmlTests : FetchXmlTestBase
{
    [Fact]
    public void ToFetchXml_WithReturnRecordCount_GeneratesReturnTotalRecordCountAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ReturnRecordCount(_ => { })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" returntotalrecordcount="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithReturnRecordCountAndFilter_GeneratesReturnTotalRecordCountWithFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .ReturnRecordCount(_ => { })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" returntotalrecordcount="true">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }
}

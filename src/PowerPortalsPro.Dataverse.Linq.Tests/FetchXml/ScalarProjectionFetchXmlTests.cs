using FluentAssertions;
using Microsoft.Xrm.Sdk;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// A select reads only the attributes its projection uses, whatever the projection's shape:
/// a single value as the whole body (<c>GetAttributeValue</c>, the indexer, <c>Id</c>), or
/// values nested in a method call, operator or conditional. Distinct then de-duplicates
/// those attributes on the server.
/// </summary>
public class ScalarProjectionFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Late-bound — a single value as the whole projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValue_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.GetAttributeValue<string>("name"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValueDistinct_GeneratesDistinctOnSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Where(a => a.GetAttributeValue<string>("name") != null)
            .Select(a => a.GetAttributeValue<string>("name"))
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="name" />
                <filter type="and">
                  <condition attribute="name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectIndexer_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a["name"])
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectIndexerInAnonymousType_GeneratesAttribute()
    {
        var column = "name";
        var fetchXml = _service.Queryable("account")
            .Select(a => new { Name = (string)a[column], Parent = a["parentaccountid"] })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="name" />
                <attribute name="parentaccountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectNullableId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => (Guid?)a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectActivityId_GeneratesPrimaryKeyFromMetadata()
    {
        var fetchXml = _service.Queryable("task")
            .Select(t => t.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="task">
                <attribute name="activityid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectId_MetadataUnavailable_FallsBackToLogicalNameId()
    {
        // A service that can't answer the metadata request (a stub, or a caller without the
        // privilege) still gets a query, keyed on {logicalname}id.
        var service = Substitute.For<IOrganizationService>();
        service.Execute(Arg.Any<OrganizationRequest>()).Returns(_ => throw new InvalidOperationException("No metadata."));

        var fetchXml = service.Queryable("new_widget")
            .Select(w => w.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_widget">
                <attribute name="new_widgetid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectLookupId_GeneratesLookupAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.GetAttributeValue<EntityReference>("parentaccountid").Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="parentaccountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectAnonymousWithId_GeneratesEachAttributeOnce()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => new
            {
                a.Id,
                Name = a.GetAttributeValue<string>("name"),
                Upper = a.GetAttributeValue<string>("name").ToUpper(),
            })
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="accountid" />
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Early-bound
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectGetAttributeValue_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.GetAttributeValue<string>("new_name"))
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectProperty_Distinct_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Values the projection computes from attributes
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectMethodOnProperty_GeneratesItsAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Website.ToUpper())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectComputedMembers_GeneratesEveryAttributeRead()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.Name,
                Website = a.Website.ToUpper(),
                Employees = a.NumberOfEmployees + 1,
                ParentName = a.ParentAccount.Name,
            })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <attribute name="new_numberofemployees" />
                <attribute name="new_parentaccount" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectLogicalName_NeedsNoAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, LogicalName = ((Entity)a).LogicalName })
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

    // -------------------------------------------------------------------------
    // Projections that need the whole entity
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectWholeEntityBesideProperty_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Account = a })
            .ToFetchXml();

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
    public void ToFetchXml_UnboundSelectAttributesCollection_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => new { Name = a.GetAttributeValue<string>("name"), a.Attributes.Count })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Terminal operators over a single-value projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundSelectNullableIdFirstOrDefault_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(r => (Guid?)r.Id).FirstOrDefault());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValueMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(a => a.GetAttributeValue<int?>("numberofemployees")).Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="account">
                <attribute name="numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectComputedValueMax_Throws()
    {
        // Max of the column would drop the + 1; the provider can't aggregate a computed value.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.NumberOfEmployees + 1).Max());

        act.Should().Throw<NotSupportedException>();
    }
}

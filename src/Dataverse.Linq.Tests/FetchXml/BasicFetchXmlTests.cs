using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using NSubstitute;

namespace Dataverse.Linq.Tests.FetchXml;

public class BasicFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Basic retrieval
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_NoOperators_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Distinct
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_Distinct_GeneratesDistinctFetch()
    {
        var fetchXml = _service.Queryable<CustomAccount>().Distinct().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_DistinctWithSelect_GeneratesDistinctWithProjectedAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name })
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

    // -------------------------------------------------------------------------
    // Column selection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithExplicitColumns_GeneratesSpecificAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>("new_name").ToFetchXml();

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
    public void ToFetchXml_WithMultipleExplicitColumns_GeneratesAllRequestedAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>("new_name", "new_website").ToFetchXml();

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
    // Select projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithSelectProjection_GeneratesProjectedAttributes()
    {
        var fetchXml = (from r in _service.Queryable<CustomAccount>()
                        select new { r.Name, r.Website }).ToFetchXml();

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

    [Fact]
    public void ToFetchXml_WithSelectProjectionIncludingEntityReference_GeneratesAllProjectedAttributes()
    {
        var fetchXml = (from r in _service.Queryable<CustomAccount>()
                        select new { r.Name, r.Website, r.PrimaryContact }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <attribute name="new_primarycontact" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Ordering
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithOrderByAscending_GeneratesOrderElement()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        orderby a.Name
                        select a).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="false" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithOrderByDescending_GeneratesDescendingOrderElement()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        orderby a.Name descending
                        select a).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="true" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithMultipleOrderClauses_GeneratesMultipleOrderElements()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        orderby a.Name descending, a.Website
                        select a).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="true" />
                <order attribute="new_website" descending="false" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithOrderByAndSelectProjection_GeneratesBoth()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        orderby a.Name
                        select new { a.Name, a.Website }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <order attribute="new_name" descending="false" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Distinct with projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_DistinctWithSelectProjection_GeneratesDistinctFetchWithAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Select with ternary / null-coalesce
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectWithTernary_ExtractsIdAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.CustomAccountId,
                BoolValue = (a.IsPreferredAccount ?? false) ? true : false,
            })
            .ToFetchXml();

        // The ternary expression evaluates client-side; only the attribute
        // accesses that resolve to a simple member are emitted.
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
    // Query composition — Where after Select
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_QueryComposition_WhereAfterSelect_GeneratesCorrectFetchXml()
    {
        var query = _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount
            {
                CustomAccountId = a.CustomAccountId,
                Name = a.Name,
            });

        query = query.Where(a => a.Name != null);

        var fetchXml = query.ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }
}

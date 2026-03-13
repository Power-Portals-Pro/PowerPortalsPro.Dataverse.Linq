using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using NSubstitute;

namespace Dataverse.Linq.Tests;

public class FetchXmlGenerationTests
{
    private readonly IOrganizationServiceAsync _service = Substitute.For<IOrganizationServiceAsync>();

    private static void AssertFetchXml(string actual, string expected) =>
        actual.ReplaceLineEndings("\n").Should().Be(expected.ReplaceLineEndings("\n"));

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
    // Inner join
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithInnerJoin_GeneratesLinkEntity()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, c.FirstName, c.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Left join
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithLeftJoin_GeneratesOuterLinkEntity()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new { a.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLeftJoin_WhereInnerIsNull_GeneratesNullFilter()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where c == null
                        select new { a.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
                <filter>
                  <condition entityname="c" attribute="new_customcontactid" operator="null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

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
}

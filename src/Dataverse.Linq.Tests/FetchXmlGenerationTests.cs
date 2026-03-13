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
                <filter type="and">
                  <condition entityname="c" attribute="new_customcontactid" operator="null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
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
    // Where — typed proxy
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereEqualToValue_GeneratesEqFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEqualToNull_GeneratesNotNullFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
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
    public void ToFetchXml_WhereEqualToNull_GeneratesNullFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="null" />
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
    // Complex queries — join + where + orderby + select
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ComplexJoinWithWhereOrderBySelect_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        join a in _service.Queryable<CustomAccount>()
                            on c.CustomContactId equals a.PrimaryContact.Id
                        where a.Website == null
                            && (a.Name.Contains("Account") || c.LastName.Contains("Last"))
                        orderby c.LastName
                        select new
                        {
                            account_name = a.Name,
                            account_website = a.Website,
                            contact_name = c.LastName
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <attribute name="new_lastname" />
                <order attribute="new_lastname" descending="false" />
                <filter type="and">
                  <condition entityname="a" attribute="new_website" operator="null" />
                  <filter type="or">
                    <condition entityname="a" attribute="new_name" operator="like" value="%Account%" />
                    <condition attribute="new_lastname" operator="like" value="%Last%" />
                  </filter>
                </filter>
                <link-entity name="new_customaccount" from="new_primarycontact" to="new_customcontactid" alias="a" link-type="inner">
                  <attribute name="new_name" />
                  <attribute name="new_website" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinWithWhereOnInnerEntity_GeneratesEntityNameOnCondition()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        join a in _service.Queryable<CustomAccount>()
                            on c.CustomContactId equals a.PrimaryContact.Id
                        where a.Name == "Test"
                        select new { c.LastName, a.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <attribute name="new_lastname" />
                <filter type="and">
                  <condition entityname="a" attribute="new_name" operator="eq" value="Test" />
                </filter>
                <link-entity name="new_customaccount" from="new_primarycontact" to="new_customcontactid" alias="a" link-type="inner">
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinWithContainsFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Test"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Test%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithStartsWithFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="Custom%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithEndsWithFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.EndsWith("001"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%001" />
                </filter>
              </entity>
            </fetch>
            """);
    }
}

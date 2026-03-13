using Dataverse.Linq.Extensions;
using Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using NSubstitute;
using System.Reflection;

namespace Dataverse.Linq.Tests;

public class FetchXmlGenerationTests
{
    private readonly IOrganizationServiceAsync _service = Substitute.For<IOrganizationServiceAsync>();

    private static void AssertFetchXml(string actual, string expected) =>
        actual.ReplaceLineEndings("\n").Should().Be(expected.ReplaceLineEndings("\n"));

    private static string TranslateToFetchXml<TEntity>(
        System.Linq.Expressions.Expression expression) where TEntity : Entity
    {
        var entityLogicalName = typeof(TEntity).GetCustomAttribute<EntityLogicalNameAttribute>()!.LogicalName;
        var query = Dataverse.Linq.Expressions.FetchXmlQueryTranslator.Translate<TEntity>(
            expression, null, entityLogicalName);
        return Dataverse.Linq.FetchXmlBuilder.Build(query);
    }

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

    // -------------------------------------------------------------------------
    // Where — NotEqual (non-null)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereNotEqualToValue_GeneratesNeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="ne" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — comparison operators (lt, le, gt, ge)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereLessThan_GeneratesLtFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees < 50)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="lt" value="50" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLessEqual_GeneratesLeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees <= 50)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="le" value="50" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGreaterThan_GeneratesGtFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees > 950)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="950" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGreaterEqual_GeneratesGeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees >= 950)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="ge" value="950" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — In / NotIn
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereIn_GeneratesInFilter()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002", "Custom Account 003" };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => names.Contains(a.Name))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="in">
                    <value>Custom Account 001</value>
                    <value>Custom Account 002</value>
                    <value>Custom Account 003</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotIn_GeneratesNotInFilter()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002" };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !names.Contains(a.Name))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-in">
                    <value>Custom Account 001</value>
                    <value>Custom Account 002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereInWithGuids_GeneratesInFilterOnPrimaryKey()
    {
        var ids = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222") };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => ids.Contains(a.CustomAccountId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="in">
                    <value>11111111-1111-1111-1111-111111111111</value>
                    <value>22222222-2222-2222-2222-222222222222</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotInWithGuids_GeneratesNotInFilterOnPrimaryKey()
    {
        var ids = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111") };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !ids.Contains(a.CustomAccountId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="not-in">
                    <value>11111111-1111-1111-1111-111111111111</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (parameterless)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereToday_GeneratesTodayFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Today())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="today" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereYesterday_GeneratesYesterdayFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Yesterday())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="yesterday" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereTomorrow_GeneratesTomorrowFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Tomorrow())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="tomorrow" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereThisYear_GeneratesThisYearFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.ThisYear())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="this-year" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLastMonth_GeneratesLastMonthFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.LastMonth())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-month" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLast7Days_GeneratesLast7DaysFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Last7Days())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-seven-days" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (single int argument)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereLastXDays_GeneratesLastXDaysFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.LastXDays(30))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-x-days" value="30" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNextXMonths_GeneratesNextXMonthsFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NextXMonths(6))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="next-x-months" value="6" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOlderThanXMonths_GeneratesOlderThanFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OlderThanXMonths(12))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="olderthan-x-months" value="12" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereInFiscalYear_GeneratesInFiscalYearFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.InFiscalYear(2025))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="in-fiscal-year" value="2025" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (single DateTime argument)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOn_GeneratesOnFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.On(new DateTime(2020, 6, 15)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on" value="2020-06-15T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnOrAfter_GeneratesOnOrAfterFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrAfter(new DateTime(2020, 1, 1)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on-or-after" value="2020-01-01T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnOrBefore_GeneratesOnOrBeforeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrBefore(new DateTime(2010, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on-or-before" value="2010-12-31T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (two int arguments)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereInFiscalPeriodAndYear_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.InFiscalPeriodAndYear(3, 2025))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="in-fiscal-period-and-year">
                    <value>3</value>
                    <value>2025</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (Between / NotBetween)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereBetween_GeneratesBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Between(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotBetween_GeneratesNotBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NotBetween(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="not-between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedBetween_GeneratesNotBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.Between(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="not-between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedNotBetween_GeneratesBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.NotBetween(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — Hierarchy operators
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereAbove_GeneratesAboveFilter()
    {
        var parentId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Above(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="above" value="11111111-1111-1111-1111-111111111111" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereAboveOrEqual_GeneratesEqOrAboveFilter()
    {
        var parentId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.AboveOrEqual(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq-or-above" value="11111111-1111-1111-1111-111111111111" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereUnder_GeneratesUnderFilter()
    {
        var parentId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Under(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="under" value="22222222-2222-2222-2222-222222222222" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereUnderOrEqual_GeneratesEqOrUnderFilter()
    {
        var parentId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.UnderOrEqual(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq-or-under" value="22222222-2222-2222-2222-222222222222" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotUnder_GeneratesNotUnderFilter()
    {
        var parentId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.NotUnder(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="not-under" value="33333333-3333-3333-3333-333333333333" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualUserOrUserHierarchy_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.CustomContactId.EqualUserOrUserHierarchy())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customcontactid" operator="eq-useroruserhierarchy" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualUserOrUserHierarchyAndTeams_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.CustomContactId.EqualUserOrUserHierarchyAndTeams())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customcontactid" operator="eq-useroruserhierarchyandteams" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — User / Business unit operators
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereEqualUserId_GeneratesEqUserIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.Owner.Id.EqualUserId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="ownerid" operator="eq-userid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEqualUserId_GeneratesNeUserIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.Owner.Id.NotEqualUserId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="ownerid" operator="ne-userid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualBusinessId_GeneratesEqBusinessIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.OwningBusinessUnit.Id.EqualBusinessId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="owningbusinessunit" operator="eq-businessid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEqualBusinessId_GeneratesNeBusinessIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.OwningBusinessUnit.Id.NotEqualBusinessId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="owningbusinessunit" operator="ne-businessid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithPageSize
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithPageSize_GeneratesCountAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithPageSize(50)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" count="50">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithPageSizeAndWhere_GeneratesCountAndFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .WithPageSize(25)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" count="25">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Terminal operators — First / FirstOrDefault / Single / SingleOrDefault
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_First_GeneratesTop1()
    {
        var baseExpr = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .Expression;

        var firstExpr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.First),
            [typeof(CustomAccount)], baseExpr);

        var fetchXml = TranslateToFetchXml<CustomAccount>(firstExpr);

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
    public void ToFetchXml_FirstOrDefault_GeneratesTop1()
    {
        var baseExpr = _service.Queryable<CustomAccount>().Expression;

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.FirstOrDefault),
            [typeof(CustomAccount)], baseExpr);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_Single_GeneratesTop2()
    {
        var baseExpr = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001")
            .Expression;

        var singleExpr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Single),
            [typeof(CustomAccount)], baseExpr);

        var fetchXml = TranslateToFetchXml<CustomAccount>(singleExpr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="2">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Custom Account 001" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SingleOrDefault_GeneratesTop2()
    {
        var baseExpr = _service.Queryable<CustomAccount>().Expression;

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.SingleOrDefault),
            [typeof(CustomAccount)], baseExpr);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="2">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_FirstWithPredicate_GeneratesFilterAndTop1()
    {
        var baseExpr = _service.Queryable<CustomAccount>().Expression;

        System.Linq.Expressions.Expression<Func<CustomAccount, bool>> predicate = a => a.Name == "Custom Account 001";
        var firstExpr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.First),
            [typeof(CustomAccount)], baseExpr, predicate);

        var fetchXml = TranslateToFetchXml<CustomAccount>(firstExpr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Custom Account 001" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — ContainValues / DoesNotContainValues (multi-select option set)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereContainValues_GeneratesContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.ContainValues(1, 2, 3))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="contain-values">
                    <value>1</value>
                    <value>2</value>
                    <value>3</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereDoesNotContainValues_GeneratesNotContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.DoesNotContainValues(4, 5))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="not-contain-values">
                    <value>4</value>
                    <value>5</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedContainValues_GeneratesNotContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors_OptionSetValues.ContainValues(1, 2))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="not-contain-values">
                    <value>1</value>
                    <value>2</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereContainValuesSingleValue_GeneratesContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors_OptionSetValues.ContainValues(42))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="contain-values">
                    <value>42</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — Column-to-column comparison (valueof)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereSameTableColumnEqual_GeneratesValueOfCondition()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FirstName == c.LastName)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_firstname" operator="eq" valueof="new_lastname" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereSameTableColumnNotEqual_GeneratesValueOfCondition()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FirstName != c.LastName)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_firstname" operator="ne" valueof="new_lastname" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereCrossTableColumnEqual_GeneratesValueOfWithAlias()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where a.Name == c.FirstName
                        select new { a.Name, c.FirstName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" valueof="c.new_firstname" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }
}

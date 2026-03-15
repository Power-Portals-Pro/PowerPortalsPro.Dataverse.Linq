using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

public class JoinFetchXmlTests : FetchXmlTestBase
{
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

    [Fact]
    public void ToFetchXml_WithChainedInnerJoin_GeneratesNestedLinkEntity()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        select new { AccountName = a.Name, c.FirstName, OpportunityName = o.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                    <attribute name="new_name" />
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithChainedInnerJoinAndWhere_GeneratesNestedLinkEntityWithFilter()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        where a.Name.Contains("Custom")
                        select new { AccountName = a.Name, c.FirstName, OpportunityName = o.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Custom%" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                    <attribute name="new_name" />
                  </link-entity>
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

    // -------------------------------------------------------------------------
    // Join with complex where and string interpolation projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_JoinWithComplexWhereAndInterpolation_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where a.PercentComplete != null
                            && (c.LastName.StartsWith("Last1")
                                || c.LastName.StartsWith("Last2"))
                            && (a.NumberOfEmployees > 30
                                || a.PercentComplete < 30)
                        select new
                        {
                            a.Name,
                            Combined = $"{c.FirstName} {c.LastName}",
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_percentcomplete" operator="not-null" />
                  <filter type="or">
                    <condition entityname="c" attribute="new_lastname" operator="like" value="Last1%" />
                    <condition entityname="c" attribute="new_lastname" operator="like" value="Last2%" />
                  </filter>
                  <filter type="or">
                    <condition attribute="new_numberofemployees" operator="gt" value="30" />
                    <condition attribute="new_percentcomplete" operator="lt" value="30" />
                  </filter>
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinWithComplexWhereOrderByAndInterpolation_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where a.PercentComplete != null
                            && (c.LastName.StartsWith("Last1")
                                || c.LastName.StartsWith("Last2"))
                            && (a.NumberOfEmployees > 30
                                || a.PercentComplete < 30)
                        orderby
                            a.Name
                            , c.LastName descending
                        select new
                        {
                            a.Name,
                            Combined = $"{c.FirstName} {c.LastName}",
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <order attribute="new_name" descending="false" />
                <order attribute="new_lastname" descending="true" entityname="c" />
                <filter type="and">
                  <condition attribute="new_percentcomplete" operator="not-null" />
                  <filter type="or">
                    <condition entityname="c" attribute="new_lastname" operator="like" value="Last1%" />
                    <condition entityname="c" attribute="new_lastname" operator="like" value="Last2%" />
                  </filter>
                  <filter type="or">
                    <condition attribute="new_numberofemployees" operator="gt" value="30" />
                    <condition attribute="new_percentcomplete" operator="lt" value="30" />
                  </filter>
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }
}

using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// A select after a join reads only the columns its projection uses on each entity, wherever
/// they sit: in a method call or operator, or as the whole projection. An entity it uses whole
/// gets every column, one it tests against null gets its primary key, and one it doesn't touch
/// gets none.
/// </summary>
public class JoinProjectionFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Left join, select folded into the join
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_LeftJoinSelectComputedMembers_GeneratesEveryAttributeRead()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new
                        {
                            a.Name,
                            WebsiteLength = a.Website.Length,
                            Employees = a.NumberOfEmployees + 1,
                            FullName = c.FirstName + " " + c.LastName,
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <attribute name="new_numberofemployees" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_LeftJoinSelectWholeEntitiesBesideColumns_GeneratesAllAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new { a.Name, Account = a, ContactName = c.Name, Contact = c }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_LeftJoinSelectInnerNullTest_GeneratesInnerPrimaryKey()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new { a.Name, HasContact = c != null }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_customcontactid" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundLeftJoinSelectGetAttributeValueDistinct_GeneratesSingleAttribute()
    {
        var fetchXml = (from a in _service.Queryable("new_customaccount")
                        join c in _service.Queryable("new_customcontact")
                            on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select a.GetAttributeValue<string>("new_name")).Distinct().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundLeftJoinSelectIdAndIndexer_GeneratesTheColumnsRead()
    {
        // c["new_name"] is read as the column alone, like GetAttributeValue; the contact's
        // primary key, which nothing reads, would split the distinct rows by contact.
        var fetchXml = (from a in _service.Queryable("new_customaccount")
                        join c in _service.Queryable("new_customcontact")
                            on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new { AccountId = a.Id, ContactName = (string)c["new_name"] }).Distinct().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Left join, where between the join and the select
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_LeftJoinWhereSelectComputedMembers_GeneratesEveryAttributeRead()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Name != null
                        select new
                        {
                            a.Name,
                            WebsiteLength = a.Website.Length,
                            Employees = a.NumberOfEmployees + 1,
                            ContactName = c == null ? null : c.Name.ToUpper(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <attribute name="new_numberofemployees" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_customcontactid" />
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_LeftJoinWhereSelectWholeOuterBesideColumn_GeneratesAllAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Name != null
                        select new { a.Name, Account = a }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundLeftJoinWhereSelectGetAttributeValueDistinct_GeneratesSingleAttribute()
    {
        var fetchXml = (from a in _service.Queryable("new_customaccount")
                        join c in _service.Queryable("new_customcontact")
                            on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where c == null
                        select a.GetAttributeValue<string>("new_name")).Distinct().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
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
    // Inner join
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_JoinSelectComputedMembers_GeneratesEveryAttributeRead()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, WebsiteLength = a.Website.Length, ContactName = c.Name.ToUpper() }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinSelectWholeRootBesideColumn_GeneratesAllAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, Account = a, ContactName = c.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinSelectRootPassedToMethod_GeneratesAllAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, Account = a.ToEntityReference() }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinSelectRootColumnSum_GeneratesAggregate()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select a.NumberOfEmployees).ToFetchXml(q => q.Sum());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="sum" aggregate="sum" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinSelectComputedValueSum_Throws()
    {
        // Summing the column would drop the + 1.
        var act = () => (from a in _service.Queryable<CustomAccount>()
                         join c in _service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select a.NumberOfEmployees + 1).ToFetchXml(q => q.Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_JoinSelectThenSelect_FoldsToTheColumnsRead()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where a.Name != null
                        select new { a.Name, c.LastName })
                       .Select(x => x.Name.ToUpper())
                       .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectOverJoinProjectionAcrossTake_RunsOnTheResults()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, c.LastName })
                       .Take(5)
                       .Select(x => x.LastName)
                       .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinSelectLinkColumnMax_Throws()
    {
        // The joined column isn't a root column; an aggregate without a selector reads the root.
        var act = () => (from a in _service.Queryable<CustomAccount>()
                         join c in _service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select c.LastName).ToFetchXml(q => q.Max());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_JoinSelectLinkColumnDistinct_GeneratesNoRootAttributes()
    {
        // Every root column would make each row distinct; the root's rows need none.
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select c.LastName).Distinct().ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }
}

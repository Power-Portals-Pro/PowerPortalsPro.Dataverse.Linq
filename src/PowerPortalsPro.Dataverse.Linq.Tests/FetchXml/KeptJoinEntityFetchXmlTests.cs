using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// After a join's select that keeps one of its entities (<c>select c</c>, <c>select a</c>), the
/// rows are that entity, and an operator's lambda reads it whatever it names its parameter:
/// <c>contacts.Where(x =&gt; x.FirstName == "Bob")</c> tests the contact's column, not the
/// account's, and <c>accounts.Where(c =&gt; c.Name == "X")</c> the account's, not the contact's.
/// </summary>
public class KeptJoinEntityFetchXmlTests : FetchXmlTestBase
{
    private IQueryable<CustomContact> Contacts =>
        from a in _service.Queryable<CustomAccount>()
        join c in _service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id
        select c;

    private IQueryable<CustomAccount> Accounts =>
        from a in _service.Queryable<CustomAccount>()
        join c in _service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id
        select a;

    private IQueryable<CustomContact> LeftJoinedContacts =>
        from a in _service.Queryable<CustomAccount>()
        join c in _service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id into contacts
        from c in contacts.DefaultIfEmpty()
        select c;

    // -------------------------------------------------------------------------
    // Where, OrderBy and predicates
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOnKeptInnerEntityByAnotherName_GeneratesConditionOnTheLink()
    {
        var fetchXml = Contacts.Where(x => x.FirstName == "Bob").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition entityname="c" attribute="new_firstname" operator="eq" value="Bob" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnKeptRootEntityNamedForTheJoinedOne_GeneratesConditionOnTheRoot()
    {
        // c is the account here, not the joined contact.
        var fetchXml = Accounts.Where(c => c.Name == "Contoso").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OrderByKeptInnerEntityByAnotherName_GeneratesOrderOnTheLink()
    {
        var fetchXml = Contacts.OrderBy(x => x.LastName).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <order attribute="new_lastname" descending="false" entityname="c" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_FirstOrDefaultWithPredicateOnKeptInnerEntity_GeneratesConditionOnTheLink()
    {
        var fetchXml = Contacts.ToFetchXml(q => q.FirstOrDefault(x => x.LastName == "Smith"));

        fetchXml.Should().Contain("<condition entityname=\"c\" attribute=\"new_lastname\" operator=\"eq\" value=\"Smith\" />");
    }

    [Fact]
    public void ToFetchXml_WhereOnKeptLeftJoinedEntityByAnotherName_GeneratesConditionOnTheLink()
    {
        var fetchXml = LeftJoinedContacts.Where(x => x.LastName == "Smith").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition entityname="c" attribute="new_lastname" operator="eq" value="Smith" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereAfterLeftJoinWhereThenSelectKept_GeneratesConditionOnTheLink()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id into cs
                       from c in cs.DefaultIfEmpty()
                       where a.Name != null
                       select c;

        var fetchXml = contacts.Where(x => x.LastName == "Smith").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                  <condition entityname="c" attribute="new_lastname" operator="eq" value="Smith" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Select, GroupBy, aggregates and joins
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectAfterTakeOnKeptInnerEntity_GeneratesTheLinksColumns()
    {
        var fetchXml = Contacts.Take(5).Select(x => new { x.FirstName, x.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectOnKeptLeftJoinedEntity_GeneratesTheLinksColumns()
    {
        var fetchXml = LeftJoinedContacts.Where(x => x.LastName != null).Select(x => x.FirstName).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition entityname="c" attribute="new_lastname" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByKeptInnerEntityByAnotherName_GroupsTheLinksColumn()
    {
        var fetchXml = Contacts
            .GroupBy(x => x.LastName)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" alias="key" groupby="true" />
                  <attribute name="new_customcontactid" alias="count" aggregate="count" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MaxOfKeptRootEntityNamedForTheJoinedOne_GeneratesAggregateOnTheRoot()
    {
        var fetchXml = Accounts.ToFetchXml(q => q.Max(c => c.NumberOfEmployees));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="max" aggregate="max" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinOnKeptInnerEntityByAnotherName_NestsTheJoinUnderTheLink()
    {
        var fetchXml = (from x in Contacts
                        join o in _service.Queryable<CustomOpportunity>() on x.CustomContactId equals o.Contact.Id
                        select new { x.FirstName, o.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
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
    public void ToFetchXml_JoinThenWhereOnKeptInnerEntityByAnotherName_RoutesTheConditionToTheLink()
    {
        var fetchXml = (from x in Contacts
                        join o in _service.Queryable<CustomOpportunity>() on x.CustomContactId equals o.Contact.Id
                        where x.LastName == "Smith"
                        select o.Name).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition entityname="c" attribute="new_lastname" operator="eq" value="Smith" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                    <attribute name="new_name" />
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinOnKeptInnerEntityReusingItsAlias_GivesTheNewLinkItsOwnAlias()
    {
        // contacts already has a link "c"; this query's c is the opportunity.
        var fetchXml = (from x in Contacts
                        join c in _service.Queryable<CustomOpportunity>() on x.CustomContactId equals c.Contact.Id
                        where c.Name == "Deal"
                        select new { x.FirstName, c.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <filter type="and">
                  <condition entityname="c1" attribute="new_name" operator="eq" value="Deal" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="c1" link-type="inner">
                    <attribute name="new_name" />
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // A kept root selects as a query on the root does
    // -------------------------------------------------------------------------

    private IQueryable<CustomAccount> LeftJoinedAccounts =>
        from a in _service.Queryable<CustomAccount>()
        join c in _service.Queryable<CustomContact>() on a.CustomAccountId equals c.ParentAccount.Id into contacts
        from c in contacts.DefaultIfEmpty()
        select a;

    [Fact]
    public void ToFetchXml_SelectAfterDistinctOnKeptRoot_KeepsThePrimaryKey()
    {
        var fetchXml = LeftJoinedAccounts.Distinct().Select(x => x.Name).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountChildrenOnKeptRoot_GeneratesRowAggregate()
    {
        var fetchXml = Accounts.Select(x => new { x.Name, Children = x.CountChildren() }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" alias="children" rowaggregate="CountChildren" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnKeptInnerEntityNamedLikeItsJoinVariable_GeneratesConditionAsBefore()
    {
        var fetchXml = Contacts.Where(c => c.FirstName == "Bob").ToFetchXml();

        fetchXml.Should().Be(Contacts.Where(x => x.FirstName == "Bob").ToFetchXml());
    }
}

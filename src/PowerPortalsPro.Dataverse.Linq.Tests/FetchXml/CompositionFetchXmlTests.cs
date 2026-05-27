using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Tests for query composition — building a query incrementally by applying further
/// LINQ operators to an already-constructed <see cref="IQueryable{T}"/>. Composition is
/// common when query fragments are shared, reused, or conditionally extended.
/// </summary>
public class CompositionFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Filter composition
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_IncrementalWhere_CombinesConditionsWithAnd()
    {
        var q = _service.Queryable<CustomAccount>().Where(a => a.Name == "X");
        q = q.Where(a => a.NumberOfEmployees > 5);

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="X" />
                  <condition attribute="new_numberofemployees" operator="gt" value="5" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ManyIncrementalWheres_CombinesAllConditions()
    {
        var q = _service.Queryable<CustomContact>();
        q = q.Where(c => c.FirstName != null);
        q = q.Where(c => c.LastName != null);
        q = q.Where(c => c.Status == CustomContact.CustomContact_Status.Active);

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_firstname" operator="not-null" />
                  <condition attribute="new_lastname" operator="not-null" />
                  <condition attribute="statecode" operator="eq" value="0" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Ordering / paging composition
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereThenOrderByThenTake_ComposesAll()
    {
        var q = _service.Queryable<CustomAccount>().Where(a => a.Name != null);
        q = q.OrderBy(a => a.Name);
        q = q.Take(10);

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical" top="10">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="false" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereThenOrderByDescendingThenThenBy_ComposesOrders()
    {
        var q = _service.Queryable<CustomContact>().Where(c => c.FirstName != null);
        q = q.OrderByDescending(c => c.LastName).ThenBy(c => c.FirstName);

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <order attribute="new_lastname" descending="true" />
                <order attribute="new_firstname" descending="false" />
                <filter type="and">
                  <condition attribute="new_firstname" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereThenWithPageSize_ComposesPaging()
    {
        var q = _service.Queryable<CustomAccount>().Where(a => a.Name != null);

        AssertFetchXml(q.WithPageSize(50).ToFetchXml(),
            """
            <fetch mapping="logical" count="50">
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
    public void ToFetchXml_WhereThenSelectColumnThenDistinct_ComposesDistinct()
    {
        var q = _service.Queryable<CustomAccount>().Where(a => a.Name != null).Select(a => a.Name);
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Distinct),
            [typeof(string)], q.Expression);

        AssertFetchXml(TranslateToFetchXml<CustomAccount>(expr),
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Join composition — project the inner (link) entity, then extend
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ProjectInnerThenWhere_RoutesFilterToLink()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var filtered = contacts.Where(c => c.FirstName == "Bob");

        AssertFetchXml(filtered.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
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
    public void ToFetchXml_ProjectInnerThenOrderBy_RoutesOrderToLink()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        AssertFetchXml(contacts.OrderBy(c => c.LastName).ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_lastname" descending="false" entityname="c" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ProjectInnerThenChainedWheres_RoutesAllToLink()
    {
        // `select c` (folded into the join's result selector) projects the whole inner
        // entity, so the link carries all-attributes. Both composed wheres route to it.
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select c;

        var q = contacts.Where(c => c.FirstName == "A").Where(c => c.LastName == "B");

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition entityname="c" attribute="new_firstname" operator="eq" value="A" />
                  <condition entityname="c" attribute="new_lastname" operator="eq" value="B" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ProjectInnerThenTake_ComposesTop()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        AssertFetchXml(contacts.Take(5).ToFetchXml(),
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ProjectInnerThenFirstOrDefault_SetsTop1()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.FirstOrDefault),
            [typeof(CustomContact)], contacts.Expression);

        AssertFetchXml(TranslateToFetchXml<CustomContact>(expr),
            """
            <fetch mapping="logical" top="1">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Projection composition — a later Select re-defines (narrows) the projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ProjectInnerThenReprojectColumns_NarrowsLinkColumns()
    {
        // First `select c` marks the link with all-attributes; the composed Select must
        // narrow it to just the requested columns (not leave <all-attributes/>).
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var projected = contacts.Select(c => new { c.FirstName, c.LastName });

        AssertFetchXml(projected.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Multi-step join composition
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ProjectInnerThenJoinThenSelectColumns_NestsAndNarrows()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var q = from c in contacts
                join o in _service.Queryable<CustomOpportunity>()
                    on c.CustomContactId equals o.Contact.Id
                select new { c.FirstName, o.Name };

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
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

    [Fact]
    public void ToFetchXml_ProjectInnerThenJoinThenProjectInnerThenWhere_RoutesFilterToLink()
    {
        var contacts = from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select c;

        var coContacts = from c in contacts
                         join o in _service.Queryable<CustomOpportunity>()
                             on c.CustomContactId equals o.Contact.Id
                         select c;

        AssertFetchXml(coContacts.Where(c => c.LastName != null).ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                  <condition entityname="c" attribute="new_lastname" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Composing a join onto a pre-filtered outer query
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_FilteredOuterThenJoin_PreservesOuterFilter()
    {
        var accounts = _service.Queryable<CustomAccount>().Where(a => a.Name != null);

        var q = from a in accounts
                join c in _service.Queryable<CustomContact>()
                    on a.CustomAccountId equals c.ParentAccount.Id
                select new { a.Name, c.FirstName };

        AssertFetchXml(q.ToFetchXml(),
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Left join composition
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_LeftJoinProjectOuterThenWhere_KeepsOuterLink()
    {
        var q = from a in _service.Queryable<CustomAccount>()
                join c in _service.Queryable<CustomContact>()
                    on a.CustomAccountId equals c.ParentAccount.Id into contacts
                from c in contacts.DefaultIfEmpty()
                select a;

        AssertFetchXml(q.Where(a => a.Name != null).ToFetchXml(),
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
}

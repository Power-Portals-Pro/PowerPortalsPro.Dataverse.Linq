using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Where a subquery (<c>Any</c>, <c>All</c>, <c>Exists</c>, <c>In</c>) goes: a link-entity links
/// from the entity that holds it, so one correlated with a joined entity goes in that entity, and
/// one under <c>||</c> stays in its filter.
/// </summary>
public class SubqueryPlacementFetchXmlTests : FetchXmlTestBase
{
    private IQueryable<CustomAccount> Accounts => _service.Queryable<CustomAccount>();

    private IQueryable<CustomContact> Contacts => _service.Queryable<CustomContact>();

    private IQueryable<CustomOpportunity> Opportunities => _service.Queryable<CustomOpportunity>();

    // -------------------------------------------------------------------------
    // Correlated with a joined entity
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_AnyOnInnerJoinedEntity_GoesInTheJoinedEntitysFilter()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        where Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                        select new { a.Name, c.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                  <filter type="and">
                    <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="any" />
                  </filter>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ExistsOnInnerJoinedEntity_GoesUnderTheJoinedEntity()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        where a.Name != null && Opportunities.Exists(o => o.Contact.Id == c.CustomContactId)
                        select new { a.Name, c.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="exists" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_AnyOnRootAfterJoin_StaysInTheRootFilter()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        where Contacts.Any(p => p.ParentAccount.Id == a.CustomAccountId && p.LastName == "Last1")
                        select c.LastName).ToFetchXml();

        fetchXml.Should().Contain(
            """<link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="p" link-type="any">""");
        fetchXml.IndexOf("alias=\"p\"").Should().BeLessThan(fetchXml.IndexOf("alias=\"c\""),
            "the subquery is in the root's filter, before the joined entity");
    }

    [Fact]
    public void ToFetchXml_AnyOnKeptJoinedEntity_GoesInTheJoinedEntitysFilter()
    {
        var contacts = from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                       select c;

        var fetchXml = contacts.Where(x => Opportunities.Any(o => o.Contact.Id == x.CustomContactId)).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <all-attributes />
                  <filter type="and">
                    <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="any" />
                  </filter>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_AnyOnJoinedEntityUnderOr_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         where a.Name == "X" || Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                         select a.Name).ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*can only be combined with &&*");
    }

    [Fact]
    public void ToFetchXml_AnyOnLeftJoinedEntity_Throws()
    {
        // In a left join's filter, an account whose contact has no opportunity would still come back.
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                         from c in contacts.DefaultIfEmpty()
                         where Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                         select a.Name).ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*joined through a left join*");
    }

    [Fact]
    public void ToFetchXml_AnyReadingTheOuterRowOutsideItsJoinCondition_Throws()
    {
        // The link-entity's filter has only the opportunity's columns.
        var act = () => Contacts
            .Where(c => Opportunities.Any(o => o.Contact.Id == c.CustomContactId && o.Name == c.LastName))
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*only in its join condition*");
    }

    [Fact]
    public void ToFetchXml_AnyOnEntityJoinedThroughALeftJoin_Throws()
    {
        // o is inner-joined, but under the left-joined c: its filter doesn't limit the accounts.
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                         from c in contacts.DefaultIfEmpty()
                         join o in Opportunities on c.CustomContactId equals o.Contact.Id
                         where Contacts.Any(p => p.CustomContactId == o.Contact.Id)
                         select a.Name).ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*joined through a left join*");
    }

    [Fact]
    public void ToFetchXml_AnyReadingTheOuterRowBeforeItsJoinCondition_Throws()
    {
        // The first comparison isn't taken for the join because it comes first.
        var act = () => Contacts
            .Where(c => Opportunities.Any(o => o.Name == c.LastName && o.Contact.Id == c.CustomContactId))
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*only in its join condition*");
    }

    [Fact]
    public void ToFetchXml_SubqueryNamedLikeAJoin_GetsItsOwnAlias()
    {
        var fetchXml = Accounts
            .Join(Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
            .Where(x => Opportunities.Any(c => c.Contact.Id == x.c.CustomContactId))
            .Select(x => x.a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <filter type="and">
                    <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="c1" link-type="any" />
                  </filter>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TwoSubqueriesWithOneName_GetTheirOwnAliases()
    {
        var fetchXml = Accounts
            .Where(a => Contacts.Any(c => c.ParentAccount.Id == a.CustomAccountId)
                && !Contacts.Any(c => c.ParentAccount.Id == a.CustomAccountId && c.LastName == null))
            .Select(a => a.Name)
            .ToFetchXml();

        fetchXml.Should().Contain("alias=\"c\" link-type=\"any\"");
        fetchXml.Should().Contain("alias=\"c1\" link-type=\"not any\"");
    }

    [Fact]
    public void ToFetchXml_ExistsInALeftJoin_LeavesTheLeftJoinedEntityItsColumnsAndNullTest()
    {
        // The Exists link is the last link added; the left join's c is still the contact.
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where Contacts.Exists(p => p.ParentAccount.Id == a.CustomAccountId) && c == null
                        select new { a.Name, c.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition entityname="c" attribute="new_customcontactid" operator="null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_lastname" />
                </link-entity>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="p" link-type="exists" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // A later Where is ANDed with an earlier one's ||
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereAfterWhereWithOr_AndsThePredicates()
    {
        var fetchXml = Accounts
            .Where(a => a.Name == "X" || a.Name == "Y")
            .Where(a => a.NumberOfEmployees > 15)
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="15" />
                  <filter type="or">
                    <condition attribute="new_name" operator="eq" value="X" />
                    <condition attribute="new_name" operator="eq" value="Y" />
                  </filter>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_ExistsAfterWhereWithOr_StaysBesideTheLinks()
    {
        var fetchXml = Accounts
            .Where(a => a.Name == "X" || a.Name == "Y")
            .Where(a => Contacts.Exists(c => c.ParentAccount.Id == a.CustomAccountId))
            .Select(a => a.Name)
            .ToFetchXml();

        fetchXml.Should().Contain("link-type=\"exists\"");
        fetchXml.Should().Contain("<filter type=\"or\">");
    }

    [Fact]
    public void ToFetchXml_CountWithPredicateAfterWhereWithOr_AndsThePredicates()
    {
        var fetchXml = Accounts
            .Where(a => a.Name == "X" || a.Name == "Y")
            .ToFetchXml(q => q.Count(a => a.NumberOfEmployees > 15));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="15" />
                  <filter type="or">
                    <condition attribute="new_name" operator="eq" value="X" />
                    <condition attribute="new_name" operator="eq" value="Y" />
                  </filter>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Exists and In under || or in another subquery
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ExistsUnderOr_StaysInTheFilterAsAny()
    {
        var fetchXml = Accounts
            .Where(a => a.Name == "Contoso" || Contacts.Exists(c => c.ParentAccount.Id == a.CustomAccountId))
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="or">
                  <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="any" />
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_InUnderOr_StaysInTheFilterAsAny()
    {
        var fetchXml = Accounts
            .Where(a => a.Name == "Contoso" || Contacts.In(c => c.ParentAccount.Id == a.CustomAccountId))
            .Select(a => a.Name)
            .ToFetchXml();

        fetchXml.Should().Contain(
            """<link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="any" />""");
        fetchXml.Should().NotContain("link-type=\"in\"");
    }

    [Fact]
    public void ToFetchXml_ExistsInsideAny_StaysInTheSubquerysFilter()
    {
        // The inner Exists was dropped: it went to a query that's thrown away.
        var fetchXml = Accounts
            .Where(a => Contacts.Any(c => c.ParentAccount.Id == a.CustomAccountId
                && Opportunities.Exists(o => o.Contact.Id == c.CustomContactId)))
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="any">
                    <filter type="and">
                      <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="any" />
                    </filter>
                  </link-entity>
                </filter>
              </entity>
            </fetch>
            """);
    }
}

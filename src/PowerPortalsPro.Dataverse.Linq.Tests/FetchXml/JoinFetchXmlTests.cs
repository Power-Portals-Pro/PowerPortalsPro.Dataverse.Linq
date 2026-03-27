using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

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

    [Fact]
    public void ToFetchXml_WithLeftJoin_SelectWholeEntities_GeneratesAllAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Status == CustomAccount.CustomAccount_Status.Active
                        select new { Account = a, Contact = c }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="statecode" operator="eq" value="0" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLeftJoin_SelectProjectedProperties_GeneratesCorrectAttributes()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Status == CustomAccount.CustomAccount_Status.Active
                        select new { Account = new { a.CustomAccountId, a.Name }, Contact = new { c.CustomContactId, c.Name } }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="statecode" operator="eq" value="0" />
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
    public void ToFetchXml_WithLeftJoin_WhereOnOuterEntity_GeneratesFilter()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Status == CustomAccount.CustomAccount_Status.Active
                        select new { a.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="statecode" operator="eq" value="0" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLeftJoin_WhereOnOuterAndInnerNull_GeneratesFilters()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        where a.Name.Contains("Test") && c == null
                        select new { a.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Test%" />
                  <condition entityname="c" attribute="new_customcontactid" operator="null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLeftJoin_TernarySelectOnInner_IncludesInnerColumns()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new
                        {
                            a.Name,
                            ContactName = c != null ? c.FirstName : null
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithLeftJoin_TernarySelectWithNewObject_IncludesInnerColumns()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new
                        {
                            a.Name,
                            Contact = c != null
                                ? new { c.FirstName, c.LastName }
                                : null
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
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

    // -------------------------------------------------------------------------
    // Chained left joins
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithChainedLeftJoin_GeneratesNestedOuterLinkEntities()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id into opportunities
                        from o in opportunities.DefaultIfEmpty()
                        join a2 in _service.Queryable<CustomAccount>()
                            on o.CustomOpportunityId equals a2.PrimaryContact.Id into accounts
                        from a2 in accounts.DefaultIfEmpty()
                        join c2 in _service.Queryable<CustomContact>()
                            on a2.CustomAccountId equals c2.ParentAccount.Id into contacts2
                        from c2 in contacts2.DefaultIfEmpty()
                        select new { AccountName = a.Name, c.FirstName, OpportunityName = o.Name, a2.Website, c2.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="outer">
                    <attribute name="new_name" />
                    <link-entity name="new_customaccount" from="new_primarycontact" to="new_customopportunityid" alias="a2" link-type="outer">
                      <attribute name="new_website" />
                      <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c2" link-type="outer">
                        <attribute name="new_lastname" />
                      </link-entity>
                    </link-entity>
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithMultipleLeftJoinsOnRoot_GeneratesSiblingOuterLinkEntities()
    {
        // Four left joins all keyed on the root entity produce sibling link entities
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.PrimaryContact.Id equals c.CustomContactId into contacts
                        from c in contacts.DefaultIfEmpty()
                        join o in _service.Queryable<CustomOpportunity>()
                            on a.CustomAccountId equals o.Contact.Id into opportunities
                        from o in opportunities.DefaultIfEmpty()
                        join a2 in _service.Queryable<CustomAccount>()
                            on a.ParentAccount.Id equals a2.CustomAccountId into parentAccounts
                        from a2 in parentAccounts.DefaultIfEmpty()
                        join c2 in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c2.ParentAccount.Id into moreContacts
                        from c2 in moreContacts.DefaultIfEmpty()
                        select new { a.Name, c.FirstName, OpportunityName = o.Name, ParentName = a2.Name, c2.LastName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_customcontactid" to="new_primarycontact" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                </link-entity>
                <link-entity name="new_customopportunity" from="new_contact" to="new_customaccountid" alias="o" link-type="outer">
                  <attribute name="new_name" />
                </link-entity>
                <link-entity name="new_customaccount" from="new_customaccountid" to="new_parentaccount" alias="a2" link-type="outer">
                  <attribute name="new_name" />
                </link-entity>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c2" link-type="outer">
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithFiveChainedLeftJoins_GeneratesMixedNestedAndSiblingOuterLinkEntities()
    {
        // Five left joins: nested chain (Account → Contact → Opportunity) plus siblings on root and on Contact
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id into opportunities
                        from o in opportunities.DefaultIfEmpty()
                        join a2 in _service.Queryable<CustomAccount>()
                            on a.ParentAccount.Id equals a2.CustomAccountId into parentAccounts
                        from a2 in parentAccounts.DefaultIfEmpty()
                        join c2 in _service.Queryable<CustomContact>()
                            on c.CustomContactId equals c2.ParentAccount.Id into subContacts
                        from c2 in subContacts.DefaultIfEmpty()
                        join o2 in _service.Queryable<CustomOpportunity>()
                            on a2.CustomAccountId equals o2.Contact.Id into parentOpportunities
                        from o2 in parentOpportunities.DefaultIfEmpty()
                        select new { a.Name, c.FirstName, OpportunityName = o.Name, ParentAccountName = a2.Name, c2.LastName, ParentOppName = o2.Name }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="outer">
                    <attribute name="new_name" />
                  </link-entity>
                  <link-entity name="new_customcontact" from="new_parentaccount" to="new_customcontactid" alias="c2" link-type="outer">
                    <attribute name="new_lastname" />
                  </link-entity>
                </link-entity>
                <link-entity name="new_customaccount" from="new_customaccountid" to="new_parentaccount" alias="a2" link-type="outer">
                  <attribute name="new_name" />
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customaccountid" alias="o2" link-type="outer">
                    <attribute name="new_name" />
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithChainedLeftJoinAndTernarySelect_IncludesColumnsFromAllBranches()
    {
        // Ternary expressions in the select should include columns from all branches
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        join o in _service.Queryable<CustomOpportunity>()
                            on a.PrimaryContact.Id equals o.Contact.Id into opportunities
                        from o in opportunities.DefaultIfEmpty()
                        select new
                        {
                            a.Name,
                            ContactName = c != null ? c.FirstName : null,
                            Detail = o != null ? o.Name : c != null ? c.LastName : null,
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
                <link-entity name="new_customopportunity" from="new_contact" to="new_primarycontact" alias="o" link-type="outer">
                  <attribute name="new_name" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithFirstRow — matchfirstrowusingcrossapply
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithFirstRow_GeneratesMatchFirstRowLinkType()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>().WithFirstRow()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, c.FirstName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="matchfirstrowusingcrossapply">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithFirstRowAndWhere_GeneratesMatchFirstRowWithFilter()
    {
        // Where clause after the join places the filter on the root entity
        // with entityname pointing to the link alias.
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>().WithFirstRow()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where c.ContactRating_OptionSetValue != null
                        select new { a.Name, c.FirstName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition entityname="c" attribute="new_contactrating" operator="not-null" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="matchfirstrowusingcrossapply">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }
}

using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// An operator after a select reads its lambda through the select: a member the select copied
/// from a column (<c>Name = a.Name</c>) tests or orders that column, and a member it computed
/// (<c>Name = "x" + a.Name</c>) throws rather than testing the raw column.
/// </summary>
public class OperatorAfterSelectFetchXmlTests : FetchXmlTestBase
{
    private IQueryable<CustomAccount> Accounts => _service.Queryable<CustomAccount>();

    private IQueryable<CustomContact> Contacts => _service.Queryable<CustomContact>();

    // -------------------------------------------------------------------------
    // Where — a member copied from a column tests the column
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOnAnonymousMember_GeneratesConditionOnItsColumn()
    {
        var fetchXml = Accounts
            .Select(a => new { a.Name, a.Website })
            .Where(x => x.Name == "Contoso")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnSelectedValue_GeneratesConditionOnItsColumn()
    {
        var fetchXml = Accounts
            .Select(a => a.Name)
            .Where(name => name.StartsWith("Con"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="Con%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnEntityMemberCopiedFromItsColumn_GeneratesConditionOnTheColumn()
    {
        var fetchXml = Accounts
            .Select(a => new CustomAccount { Name = a.Name })
            .Where(x => x.Name == "Contoso")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnEntityMemberCopiedFromAnotherColumn_GeneratesConditionOnTheSourceColumn()
    {
        // Name holds the description here, so the condition tests the description.
        var fetchXml = Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => x.Name == "Contoso")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_description" />
                <filter type="and">
                  <condition attribute="new_description" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereThroughLet_GeneratesConditionOnTheColumn()
    {
        var fetchXml = (from a in Accounts
                        let name = a.Name
                        where name == "Contoso"
                        select a).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereAfterSelectOfTheEntity_GeneratesConditionAsBefore()
    {
        var fetchXml = Accounts
            .Select(a => a)
            .Where(x => x.Name == "Contoso")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnIdOfEntityWhoseKeyTheSelectSets_GeneratesConditionOnTheKey()
    {
        var id = new Guid("11111111-2222-3333-4444-555555555555");

        var fetchXml = Accounts
            .Select(a => new CustomAccount { CustomAccountId = a.CustomAccountId, Name = a.Name })
            .Where(x => x.Id == id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq" value="11111111-2222-3333-4444-555555555555" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnColumnReadByNameOffTheSelectedEntity_TranslatesAsTheMember()
    {
        var byMember = Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => x.Name == "Contoso")
            .ToFetchXml();

        Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => x.GetAttributeValue<string>("new_name") == "Contoso")
            .ToFetchXml()
            .Should().Be(byMember);

        Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => (string)x["new_name"] == "Contoso")
            .ToFetchXml()
            .Should().Be(byMember);
    }

    [Fact]
    public void ToFetchXml_WhereComparingMemberToComputedMember_Throws()
    {
        var act = () => Accounts
            .Select(a => new { a.Name, Upper = a.Name.ToUpper() })
            .Where(x => x.Name == x.Upper)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Where — a computed member throws
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOnComputedEntityMember_Throws()
    {
        // Name is "x" + the stored name; a condition on new_name would test the stored one.
        var act = () => Accounts
            .Select(a => new CustomAccount { Name = "x" + a.Name })
            .Where(x => x.Name == "xContoso")
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereOnComputedAnonymousMember_Throws()
    {
        var act = () => Accounts
            .Select(a => new { Upper = a.Name.ToUpper() })
            .Where(x => x.Upper.StartsWith("CON"))
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereOnEntityMemberTheSelectDoesNotSet_Throws()
    {
        // The select leaves Website null on every entity it makes, whatever the column holds.
        var act = () => Accounts
            .Select(a => new CustomAccount { Name = a.Name })
            .Where(x => x.Website == null)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereOnTruncatedMember_Throws()
    {
        // (int?) truncates 1000.50 to 1000, which a condition on the decimal column wouldn't match.
        var act = () => Accounts
            .Select(a => new { Limit = (int?)a.CreditLimit })
            .Where(x => x.Limit == 1000)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Casts in a condition
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereTruncatingCast_Throws()
    {
        var act = () => Accounts.Where(a => (int?)a.CreditLimit == 1000).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    public static TheoryData<string, Func<IQueryable<CustomAccount>, IQueryable<CustomAccount>>> RoundingCasts => new()
    {
        // (float)16777217 is 16777216, a decimal's double only near it.
        { "int to float", q => q.Where(a => (float?)a.NumberOfEmployees == 16777217f) },
        { "decimal to double", q => q.Where(a => (double?)a.CreditLimit > 1000.5) },
        { "decimal to int", q => q.Where(a => (int?)a.PercentComplete == 3) },
        { "to a column", q => q.Where(a => (int?)a.CreditLimit == a.NumberOfEmployees) },
    };

    [Theory]
    [MemberData(nameof(RoundingCasts))]
    public void ToFetchXml_WhereCastThatMayChangeTheValue_Throws(
        string cast, Func<IQueryable<CustomAccount>, IQueryable<CustomAccount>> query)
    {
        var act = () => query(Accounts).ToFetchXml();

        act.Should().Throw<NotSupportedException>(cast);
    }

    [Fact]
    public void ToFetchXml_WhereIntToDouble_GeneratesConditionOnTheColumn()
    {
        // Every int is a double exactly; the compiler casts the column so it compares to 5.5.
        var fetchXml = Accounts.Where(a => a.NumberOfEmployees > 5.5).ToFetchXml();

        fetchXml.Should().Contain("<condition attribute=\"new_numberofemployees\" operator=\"gt\" value=\"5.5\" />");
    }

    [Fact]
    public void ToFetchXml_WhereWideningCast_GeneratesConditionOnTheColumn()
    {
        var fetchXml = Accounts.Where(a => (long?)a.NumberOfEmployees > 10L).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="10" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEnumComparison_GeneratesConditionOnTheColumn()
    {
        var fetchXml = Accounts
            .Where(a => a.AccountRating == CustomAccount.AccountRating_Enum.Hot)
            .ToFetchXml();

        fetchXml.Should().Contain("<condition attribute=\"new_accountrating\" operator=\"eq\"");
    }

    // -------------------------------------------------------------------------
    // OrderBy, predicates and aggregates
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_OrderByAnonymousMember_GeneratesOrderOnItsColumn()
    {
        var fetchXml = Accounts
            .Select(a => new { a.Name, a.Website })
            .OrderBy(x => x.Name)
            .ThenByDescending(x => x.Website)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <order attribute="new_name" descending="false" />
                <order attribute="new_website" descending="true" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OrderByComputedMember_Throws()
    {
        var act = () => Accounts
            .Select(a => new { a.Name, Length = a.Name.Length })
            .OrderBy(x => x.Length)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_FirstOrDefaultWithPredicateOnAnonymousMember_GeneratesCondition()
    {
        var fetchXml = Accounts
            .Select(a => new { a.Name })
            .ToFetchXml(q => q.FirstOrDefault(x => x.Name == "Contoso"));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountWithPredicateOnComputedMember_Throws()
    {
        var act = () => Accounts
            .Select(a => new { Upper = a.Name.ToUpper() })
            .ToFetchXml(q => q.Count(x => x.Upper == "CONTOSO"));

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_CountWithPredicateOnAnonymousMember_GeneratesCondition()
    {
        var fetchXml = Accounts
            .Select(a => new { a.Name })
            .ToFetchXml(q => q.Count(x => x.Name == "Contoso"));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MaxOfAnonymousMember_GeneratesAggregateOnItsColumn()
    {
        var fetchXml = Accounts
            .Select(a => new { a.NumberOfEmployees })
            .ToFetchXml(q => q.Max(x => x.NumberOfEmployees));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SumOfComputedEntityMember_Throws()
    {
        var act = () => Accounts
            .Select(a => new CustomAccount { NumberOfEmployees = a.NumberOfEmployees + 1 })
            .ToFetchXml(q => q.Sum(x => x.NumberOfEmployees));

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // After a join's select — a member copied from a joined column tests that column
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOnJoinSelectMember_GeneratesConditionOnTheJoinedColumn()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, c.LastName })
            .Where(x => x.LastName == "Smith" && x.Name == "Contoso")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition entityname="c" attribute="new_lastname" operator="eq" value="Smith" />
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OrderByJoinSelectMember_GeneratesOrderOnTheJoinedColumn()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name, Surname = c.LastName })
            .OrderBy(x => x.Surname)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <order attribute="new_lastname" descending="false" entityname="c" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnLeftJoinSelectedEntities_GeneratesConditionsOnEachEntity()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new { a, c })
            .Where(x => x.a.Name == "Contoso" && x.c.LastName == "Smith")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
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
    public void ToFetchXml_WhereOnComputedJoinSelectMember_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         select new { FullName = c.FirstName + " " + c.LastName })
            .Where(x => x.FullName == "Ann Smith")
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // After a select on a join or on groups, and GroupBy or Join after a select
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereAfterJoinSelectMakingAnEntity_Throws()
    {
        // The condition would test the account's new_lastname, not the contact's upper-cased one.
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         select new CustomContact { LastName = c.LastName.ToUpper() })
            .Where(x => x.LastName == "SMITH")
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_MaxAfterJoinSelectMakingAnEntity_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         select new CustomAccount { NumberOfEmployees = a.NumberOfEmployees + 1 })
            .ToFetchXml(q => q.Max(x => x.NumberOfEmployees));

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_OrderByAfterGroupedSelect_Throws()
    {
        var act = () => Accounts
            .GroupBy(a => a.Name)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereAfterGroupedSelect_Throws()
    {
        // The condition would test a column of the rows, not the groups' count.
        var act = () => Accounts
            .GroupBy(a => a.AccountRating)
            .Select(g => new { g.Key, Count = g.Count() })
            .Where(x => x.Count > 1)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>().WithMessage("*can't follow a Select on groups*");
    }

    [Fact]
    public void ToFetchXml_GroupByAfterSelectMakingAnEntity_Throws()
    {
        var act = () => Accounts
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .GroupBy(x => x.Name)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_JoinAfterSelectMakingAnEntity_Throws()
    {
        var act = () => Accounts
            .Select(a => new CustomAccount { CustomAccountId = a.CustomAccountId, Name = a.Name.ToUpper() })
            .Join(Contacts, x => x.CustomAccountId, c => c.ParentAccount.Id, (x, c) => new { x.Name, c.LastName })
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // A joined column in an aggregate's selector
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_MaxOfJoinedColumn_Throws()
    {
        // The aggregate would go on the account's numberofemployees, not the contact's.
        var act = () => _service.Queryable("account")
            .Join(_service.Queryable("contact"),
                a => a.Id,
                c => c.GetAttributeValue<EntityReference>("parentcustomerid").Id,
                (a, c) => new { a, c })
            .ToFetchXml(q => q.Max(x => x.c.GetAttributeValue<int?>("numberofemployees")));

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_MaxOfRootColumnAfterJoin_GeneratesAggregateOnTheRoot()
    {
        var fetchXml = _service.Queryable("account")
            .Join(_service.Queryable("contact"),
                a => a.Id,
                c => c.GetAttributeValue<EntityReference>("parentcustomerid").Id,
                (a, c) => new { a, c })
            .ToFetchXml(q => q.Max(x => x.a.GetAttributeValue<int?>("numberofemployees")));

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="account">
                <attribute name="numberofemployees" alias="max" aggregate="max" />
                <link-entity name="contact" from="parentcustomerid" to="accountid" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }
}

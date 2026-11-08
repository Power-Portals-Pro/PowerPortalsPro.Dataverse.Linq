using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// An aggregate drops the order of the rows it reads, which Dataverse rejects in an aggregate
/// query and which doesn't change the result; a WithFirstRow join's order, which picks the
/// joined row, throws.
/// </summary>
public class AggregateOrderFetchXmlTests : FetchXmlTestBase
{
    private IQueryable<CustomAccount> Accounts => _service.Queryable<CustomAccount>();

    private IQueryable<CustomContact> Contacts => _service.Queryable<CustomContact>();

    [Fact]
    public void ToFetchXml_CountAfterOrderBy_DropsTheOrder()
    {
        var fetchXml = Accounts.OrderBy(a => a.Name).ToFetchXml(q => q.Count());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MaxAfterOrderByOnJoinedColumn_DropsTheOrder()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                        orderby c.LastName
                        select a.NumberOfEmployees).ToFetchXml(q => q.Max());

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
    public void ToFetchXml_GroupByAfterOrderBy_Throws()
    {
        // LINQ returns the groups in the order their keys first appear, which the OrderBy sets.
        var act = () => Accounts
            .OrderBy(a => a.Name)
            .GroupBy(a => a.AccountRating)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_OrderByGroupKeyAfterSelect_GeneratesTheOrder()
    {
        var fetchXml = (from a in Accounts
                        group a by a.AccountRating into g
                        orderby g.Key
                        select new { g.Key, Count = g.Count() }).ToFetchXml();

        fetchXml.Should().Contain("<order alias=\"key\" descending=\"false\" />");
    }

    [Fact]
    public void ToFetchXml_SumOverUnorderedWithFirstRowJoin_GeneratesTheAggregate()
    {
        var fetchXml = (from a in Accounts
                        join c in Contacts.WithFirstRow() on a.CustomAccountId equals c.ParentAccount.Id
                        select a.NumberOfEmployees).ToFetchXml(q => q.Sum());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="sum" aggregate="sum" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="matchfirstrowusingcrossapply" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountOverOrderedWithFirstRowJoin_Throws()
    {
        // The order picks each account's contact; an aggregate query can't order its rows.
        var act = () => (from a in Accounts
                         join c in Contacts.OrderByDescending(p => p.CreatedOn).WithFirstRow()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select new { a, c }).ToFetchXml(q => q.Count());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_GroupByOverOrderedWithFirstRowJoin_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts.OrderByDescending(p => p.CreatedOn).WithFirstRow()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         group c by c.LastName into g
                         select new { g.Key, Count = g.Count() }).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }
}

using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public class PagingFetchXmlTests : FetchXmlTestBase
{
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
    // Take
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_Take_GeneratesTopAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Take(10)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="10">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TakeWithWhereAndOrderBy_GeneratesTopWithFilterAndOrder()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Custom"))
            .OrderByDescending(a => a.Name)
            .Take(5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="true" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Custom%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Theory]
    [InlineData(5, 20)]
    [InlineData(20, 5)]
    public void ToFetchXml_TakeAfterTake_GeneratesTheSmallerTop(int first, int second)
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Take(first)
            .Take(second)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SingleAfterTakeOne_GeneratesTopOne()
    {
        // Single() asks for two rows to find a second, but Take(1) leaves it only one.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Take(1)
            .ToFetchXml(q => q.Single());

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
    public void ToFetchXml_SelectAfterTake_GeneratesTopWithTheColumn()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .Take(5)
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <order attribute="new_name" descending="false" />
              </entity>
            </fetch>
            """);
    }

    public static TheoryData<string, Func<IQueryable<CustomAccount>, IQueryable<CustomAccount>, object?>> OperatorsAfterTake => new()
    {
        { "Where", (taken, _) => taken.Where(a => a.Name == "Contoso").ToFetchXml() },
        { "OrderBy", (taken, _) => taken.OrderBy(a => a.Name).ToFetchXml() },
        { "Where after Select", (taken, _) => taken.Select(a => new { a.Name }).Where(x => x.Name == "Contoso").ToFetchXml() },
        { "GroupBy", (taken, _) => taken.GroupBy(a => a.Name).Select(g => new { g.Key, Count = g.Count() }).ToFetchXml() },
        { "First with a predicate", (taken, _) => taken.ToFetchXml(q => q.First(a => a.Name == "Contoso")) },
        { "Count", (taken, _) => taken.ToFetchXml(q => q.Count()) },
        { "Max", (taken, _) => taken.ToFetchXml(q => q.Max(a => a.NumberOfEmployees)) },
        { "Sum of a Select", (taken, _) => taken.Select(a => a.NumberOfEmployees).ToFetchXml(q => q.Sum()) },
        { "Join", (taken, all) => taken.Join(all, a => a.CustomAccountId, p => p.ParentAccount.Id, (a, p) => new { a.Name, Parent = p.Name }).ToFetchXml() },
    };

    [Theory]
    [MemberData(nameof(OperatorsAfterTake))]
    public void ToFetchXml_OperatorAfterTake_Throws(
        string operation, Func<IQueryable<CustomAccount>, IQueryable<CustomAccount>, object?> query)
    {
        // Dataverse would apply each to every row, then take the first five.
        var accounts = _service.Queryable<CustomAccount>();

        var act = () => query(accounts.Take(5), accounts);

        act.Should().Throw<NotSupportedException>(operation).WithMessage("*after Take()*");
    }

    // -------------------------------------------------------------------------
    // WithPage
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithPage_GeneratesPageAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithPage(3)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" page="3">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithPageAndPageSize_GeneratesPageAndCountAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithPageSize(50)
            .WithPage(2)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" count="50" page="2">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // WithAggregateLimit
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WithAggregateLimit_GeneratesAggregateLimitAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .WithAggregateLimit(10000)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregatelimit="10000">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithAggregateLimitOnGroupByQuery_GeneratesAggregateLimitAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .GroupBy(a => a.AccountRating)
            .Select(g => new { Rating = g.Key, Count = g.Count() })
            .WithAggregateLimit(25000)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true" aggregatelimit="25000">
              <entity name="new_customaccount">
                <attribute name="new_accountrating" alias="rating" groupby="true" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }
}

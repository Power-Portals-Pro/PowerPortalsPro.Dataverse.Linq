using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using NSubstitute;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

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

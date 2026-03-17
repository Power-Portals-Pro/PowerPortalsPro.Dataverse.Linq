using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public class AggregateFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Aggregate operators — Min / Max / Sum / Average / Count
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_MinWithSelector_GeneratesAggregateMin()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Min),
            [typeof(CustomAccount), typeof(int?)],
            queryable.Expression,
            System.Linq.Expressions.Expression.Quote((System.Linq.Expressions.Expression<Func<CustomAccount, int?>>)(a => a.NumberOfEmployees)));

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="min" aggregate="min" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MaxWithSelector_GeneratesAggregateMax()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Max),
            [typeof(CustomAccount), typeof(int?)],
            queryable.Expression,
            System.Linq.Expressions.Expression.Quote((System.Linq.Expressions.Expression<Func<CustomAccount, int?>>)(a => a.NumberOfEmployees)));

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

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
    public void ToFetchXml_SumWithSelector_GeneratesAggregateSum()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Sum),
            [typeof(CustomAccount)],
            queryable.Expression,
            System.Linq.Expressions.Expression.Quote((System.Linq.Expressions.Expression<Func<CustomAccount, decimal?>>)(a => a.PercentComplete)));

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_percentcomplete" alias="sum" aggregate="sum" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_AverageWithSelector_GeneratesAggregateAvg()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Average),
            [typeof(CustomAccount)],
            queryable.Expression,
            System.Linq.Expressions.Expression.Quote((System.Linq.Expressions.Expression<Func<CustomAccount, decimal?>>)(a => a.PercentComplete)));

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_percentcomplete" alias="avg" aggregate="avg" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_Count_GeneratesAggregateCount()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            [typeof(CustomAccount)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

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
    public void ToFetchXml_CountWithPredicate_GeneratesAggregateCountWithFilter()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            [typeof(CustomAccount)],
            queryable.Expression,
            System.Linq.Expressions.Expression.Quote((System.Linq.Expressions.Expression<Func<CustomAccount, bool>>)(a => a.Name.Contains("ABC"))));

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%ABC%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MinWithSelectAndWhere_GeneratesAggregateWithFilter()
    {
        var queryable = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("ABC"))
            .Select(a => a.NumberOfEmployees);

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Min),
            [typeof(int?)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="min" aggregate="min" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%ABC%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MinOnMoneyValue_ResolvesMoneyAttribute()
    {
        var queryable = _service.Queryable<CustomAccount>()
            .Select(a => a.CreditLimitMoney.Value);

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.Min),
            [typeof(decimal)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_creditlimit" alias="min" aggregate="min" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_LongCount_GeneratesAggregateCount()
    {
        var queryable = _service.Queryable<CustomAccount>();
        var expr = System.Linq.Expressions.Expression.Call(
            typeof(Queryable), nameof(Queryable.LongCount),
            [typeof(CustomAccount)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

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
    public void ToFetchXml_CountColumn_GeneratesAggregateCountColumn()
    {
        var queryable = _service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees);

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(ServiceClientExtensions.CountColumn),
            [typeof(int?)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="countcolumn" aggregate="countcolumn" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountColumnWithWhere_GeneratesAggregateCountColumnWithFilter()
    {
        var queryable = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("ABC"))
            .Select(a => a.NumberOfEmployees);

        var expr = System.Linq.Expressions.Expression.Call(
            typeof(ServiceClientExtensions),
            nameof(ServiceClientExtensions.CountColumn),
            [typeof(int?)],
            queryable.Expression);

        var fetchXml = TranslateToFetchXml<CustomAccount>(expr);

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="countcolumn" aggregate="countcolumn" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%ABC%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // GroupBy — grouped aggregate queries
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_GroupByWithDateYear_GeneratesGroupedAggregate()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        join c in _service.Queryable<CustomContact>()
                            on o.Contact.Id equals c.CustomContactId
                        where c.FirstName.Contains("Jane")
                        where o.StatusReason_OptionSetValue.Value == (int)CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.Year into g
                        orderby g.Key ascending
                        select new
                        {
                            Year = g.Key,
                            Count = g.Count(),
                            TotalRevenue = g.Sum(x => x.ActualRevenue),
                            AverageRevenue = g.Average(x => x.ActualRevenue),
                            TotalEstimatedRevenue = g.Sum(x => x.EstimatedRevenue),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="year" groupby="true" dategrouping="year" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <attribute name="new_actualrevenue" alias="totalrevenue" aggregate="sum" />
                <attribute name="new_actualrevenue" alias="averagerevenue" aggregate="avg" />
                <attribute name="new_estimatedrevenue" alias="totalestimatedrevenue" aggregate="sum" />
                <order alias="year" descending="false" />
                <filter type="and">
                  <condition entityname="c" attribute="new_firstname" operator="like" value="%Jane%" />
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
                <link-entity name="new_customcontact" from="new_customcontactid" to="new_contact" alias="c" link-type="inner" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SimpleGroupBy_GeneratesGroupedAggregate()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        group a by a.AccountRating_OptionSetValue.Value into g
                        select new
                        {
                            Rating = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_accountrating" alias="rating" groupby="true" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByWithCountColumn_GeneratesCountColumnAggregate()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        group a by a.AccountRating_OptionSetValue.Value into g
                        select new
                        {
                            Rating = g.Key,
                            Count = g.Count(),
                            DescriptionCount = g.CountColumn(x => x.Description),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_accountrating" alias="rating" groupby="true" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <attribute name="new_description" alias="descriptioncount" aggregate="countcolumn" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByConstant_GeneratesAggregateWithoutGroupBy()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        group a by 1 into g
                        select new
                        {
                            Average = g.Average(x => x.NumberOfEmployees),
                            Count = g.Count(),
                            ColumnCount = g.CountColumn(x => x.NumberOfEmployees!.Value),
                            Maximum = g.Max(x => x.NumberOfEmployees),
                            Minimum = g.Min(x => x.NumberOfEmployees),
                            Sum = g.Sum(x => x.NumberOfEmployees),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="average" aggregate="avg" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
                <attribute name="new_numberofemployees" alias="columncount" aggregate="countcolumn" />
                <attribute name="new_numberofemployees" alias="maximum" aggregate="max" />
                <attribute name="new_numberofemployees" alias="minimum" aggregate="min" />
                <attribute name="new_numberofemployees" alias="sum" aggregate="sum" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinGroupByWithAggregateOnLinkEntity_PlacesAttributesOnLink()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        group o by c.CustomContactId into g
                        select new
                        {
                            ContactId = g.Key,
                            Count = g.Count(),
                            TotalRevenue = g.Sum(x => x.ActualRevenue),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customcontact">
                <attribute name="new_customcontactid" alias="contactid" groupby="true" />
                <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                  <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                  <attribute name="new_actualrevenue" alias="totalrevenue" aggregate="sum" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_MultiJoinGroupByWithAggregateOnNestedLinkEntity_PlacesAttributesCorrectly()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        group o by a.CustomAccountId into g
                        select new
                        {
                            AccountId = g.Key,
                            Count = g.Count(),
                            MaxRevenue = g.Max(x => x.ActualRevenue),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="accountid" groupby="true" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                    <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                    <attribute name="new_actualrevenue" alias="maxrevenue" aggregate="max" />
                  </link-entity>
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_JoinGroupByWithCompositeKey_PlacesAttributesCorrectly()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        join o in _service.Queryable<CustomOpportunity>()
                            on c.CustomContactId equals o.Contact.Id
                        group new { c, o }
                            by new { c.CustomContactId } into g
                        select new
                        {
                            ContactId = g.Key.CustomContactId,
                            Count = g.Count(),
                            TotalRevenue = g.Sum(x => x.o.ActualRevenue),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customcontact">
                <attribute name="new_customcontactid" alias="contactid" groupby="true" />
                <link-entity name="new_customopportunity" from="new_contact" to="new_customcontactid" alias="o" link-type="inner">
                  <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                  <attribute name="new_actualrevenue" alias="totalrevenue" aggregate="sum" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // RowAggregate — CountChildren
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_CountChildren_GeneratesRowAggregate()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.Name,
                NumberOfChildren = a.CountChildren()
            })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" alias="numberofchildren" rowaggregate="CountChildren" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountChildrenWithFilter_GeneratesRowAggregateWithFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Custom"))
            .Select(a => new
            {
                a.Name,
                Children = a.CountChildren()
            })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" alias="children" rowaggregate="CountChildren" />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Custom%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // GroupBy — Date grouping variants
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_GroupByDateQuarter_GeneratesQuarterGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.Quarter() into g
                        select new
                        {
                            Quarter = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="quarter" groupby="true" dategrouping="quarter" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByDateMonth_GeneratesMonthGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.Month into g
                        select new
                        {
                            Month = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="month" groupby="true" dategrouping="month" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByDateDay_GeneratesDayGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.Day into g
                        select new
                        {
                            Day = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="day" groupby="true" dategrouping="day" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByDateWeek_GeneratesWeekGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.Week() into g
                        select new
                        {
                            Week = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="week" groupby="true" dategrouping="week" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByFiscalPeriod_GeneratesFiscalPeriodGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.FiscalPeriod() into g
                        select new
                        {
                            FiscalPeriod = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="fiscalperiod" groupby="true" dategrouping="fiscal-period" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByFiscalYear_GeneratesFiscalYearGrouping()
    {
        var fetchXml = (from o in _service.Queryable<CustomOpportunity>()
                        where o.StatusReason == CustomOpportunity.CustomOpportunity_StatusReason.Won
                        group o by o.ActualCloseDate!.Value.FiscalYear() into g
                        select new
                        {
                            FiscalYear = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customopportunity">
                <attribute name="new_actualclosedate" alias="fiscalyear" groupby="true" dategrouping="fiscal-year" />
                <attribute name="new_customopportunityid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="statuscode" operator="eq" value="2" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // GroupBy — Deep (group root by linked entity key)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_GroupByDeep_GroupRootByLinkedEntityKey_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        join a in _service.Queryable<CustomAccount>()
                            on c.ParentAccount.Id equals a.CustomAccountId
                        group c by a.CustomAccountId into g
                        select new
                        {
                            AccountId = g.Key,
                            Count = g.Count(),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customcontact">
                <attribute name="new_customcontactid" alias="count" aggregate="count" />
                <link-entity name="new_customaccount" from="new_customaccountid" to="new_parentaccount" alias="a" link-type="inner">
                  <attribute name="new_customaccountid" alias="accountid" groupby="true" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Composite key with date grouping and navigation property
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_GroupByCompositeKeyWithDateAndNavProperty_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        where c.CreatedOn != null
                        group c by new
                        {
                            Year = c.CreatedOn!.Value.Year,
                            Month = c.CreatedOn!.Value.Month,
                            Account = c.ParentAccount,
                        } into g
                        select new
                        {
                            AccountId = g.Key.Account.Id,
                            g.Key.Year,
                            g.Key.Month,
                            Count = g.Count()
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customcontact">
                <attribute name="new_parentaccount" alias="accountid" groupby="true" />
                <attribute name="createdon" alias="year" groupby="true" dategrouping="year" />
                <attribute name="createdon" alias="month" groupby="true" dategrouping="month" />
                <attribute name="new_customcontactid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="createdon" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByCompositeKeyWithConstructorProjection_GeneratesCorrectFetchXml()
    {
        var fetchXml = (from c in _service.Queryable<CustomContact>()
                        where c.CreatedOn != null
                        group c by new
                        {
                            Year = c.CreatedOn!.Value.Year,
                            Month = c.CreatedOn!.Value.Month,
                            Account = c.ParentAccount,
                        } into g
                        select new GroupTestResult(
                            g.Key.Account.Id,
                            g.Key.Year,
                            g.Key.Month,
                            g.Count())).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customcontact">
                <attribute name="new_parentaccount" alias="accountid" groupby="true" />
                <attribute name="createdon" alias="year" groupby="true" dategrouping="year" />
                <attribute name="createdon" alias="month" groupby="true" dategrouping="month" />
                <attribute name="new_customcontactid" alias="count" aggregate="count" />
                <filter type="and">
                  <condition attribute="createdon" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }
}

internal record GroupTestResult(Guid AccountId, int Year, int Month, int Count);

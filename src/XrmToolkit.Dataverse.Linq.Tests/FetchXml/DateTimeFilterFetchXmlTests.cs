using XrmToolkit.Dataverse.Linq.Extensions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using NSubstitute;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

public class DateTimeFilterFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Where — DateTime operators (parameterless)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereToday_GeneratesTodayFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Today())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="today" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereYesterday_GeneratesYesterdayFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Yesterday())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="yesterday" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereTomorrow_GeneratesTomorrowFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Tomorrow())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="tomorrow" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereThisYear_GeneratesThisYearFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.ThisYear())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="this-year" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLastMonth_GeneratesLastMonthFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.LastMonth())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-month" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLast7Days_GeneratesLast7DaysFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Last7Days())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-seven-days" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (single int argument)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereLastXDays_GeneratesLastXDaysFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.LastXDays(30))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="last-x-days" value="30" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNextXMonths_GeneratesNextXMonthsFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NextXMonths(6))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="next-x-months" value="6" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOlderThanXMonths_GeneratesOlderThanFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OlderThanXMonths(12))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="olderthan-x-months" value="12" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereInFiscalYear_GeneratesInFiscalYearFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.InFiscalYear(2025))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="in-fiscal-year" value="2025" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (single DateTime argument)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereOn_GeneratesOnFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.On(new DateTime(2020, 6, 15)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on" value="2020-06-15T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnOrAfter_GeneratesOnOrAfterFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrAfter(new DateTime(2020, 1, 1)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on-or-after" value="2020-01-01T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereOnOrBefore_GeneratesOnOrBeforeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.OnOrBefore(new DateTime(2010, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="on-or-before" value="2010-12-31T00:00:00" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (two int arguments)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereInFiscalPeriodAndYear_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.InFiscalPeriodAndYear(3, 2025))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="in-fiscal-period-and-year">
                    <value>3</value>
                    <value>2025</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — DateTime operators (Between / NotBetween)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereBetween_GeneratesBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.Between(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotBetween_GeneratesNotBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.DateCompanyWasOrganized.NotBetween(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="not-between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedBetween_GeneratesNotBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.Between(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="not-between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedNotBetween_GeneratesBetweenFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !a.DateCompanyWasOrganized.NotBetween(new DateTime(2010, 1, 1), new DateTime(2015, 12, 31)))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_datecompanywasorganized" operator="between">
                    <value>2010-01-01T00:00:00</value>
                    <value>2015-12-31T00:00:00</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }
}

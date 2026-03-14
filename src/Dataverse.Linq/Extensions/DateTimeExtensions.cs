namespace Dataverse.Linq.Extensions;

/// <summary>
/// Placeholder extension methods for DateTime/DateTime? that are translated into
/// FetchXml condition operators by the LINQ query provider. These methods should
/// only be used inside LINQ Where clauses — they throw at runtime if invoked directly.
/// </summary>
public static class DateTimeExtensions
{
    private static bool Throw() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");

    private static int ThrowInt() =>
        throw new NotImplementedException("This method is a placeholder for the Dataverse LINQ query provider and cannot be invoked directly.");

    // -------------------------------------------------------------------------
    // Date grouping methods (return int for use as group-by keys in LINQ)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>dategroup-week</c> grouping operator.
    /// </summary>
    /// <returns>The ISO week number of the date.</returns>
    public static int Week(this DateTime dateTime) => ThrowInt();
    /// <inheritdoc cref="Week(DateTime)" />
    public static int Week(this DateTime? dateTime) => ThrowInt();

    /// <summary>
    /// Maps to the FetchXml <c>dategroup-quarter</c> grouping operator.
    /// </summary>
    /// <returns>The quarter (1-4) of the date.</returns>
    public static int Quarter(this DateTime dateTime) => ThrowInt();
    /// <inheritdoc cref="Quarter(DateTime)" />
    public static int Quarter(this DateTime? dateTime) => ThrowInt();

    /// <summary>
    /// Maps to the FetchXml <c>dategroup-fiscalperiod</c> grouping operator.
    /// </summary>
    /// <returns>The fiscal period number of the date.</returns>
    public static int FiscalPeriod(this DateTime dateTime) => ThrowInt();
    /// <inheritdoc cref="FiscalPeriod(DateTime)" />
    public static int FiscalPeriod(this DateTime? dateTime) => ThrowInt();

    /// <summary>
    /// Maps to the FetchXml <c>dategroup-fiscalyear</c> grouping operator.
    /// </summary>
    /// <returns>The fiscal year of the date.</returns>
    public static int FiscalYear(this DateTime dateTime) => ThrowInt();
    /// <inheritdoc cref="FiscalYear(DateTime)" />
    public static int FiscalYear(this DateTime? dateTime) => ThrowInt();

    // -------------------------------------------------------------------------
    // No-arg operators (parameterless — the attribute is the 'this' parameter)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>last-seven-days</c> condition operator.
    /// </summary>
    public static bool Last7Days(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="Last7Days(DateTime)" />
    public static bool Last7Days(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-fiscal-period</c> condition operator.
    /// </summary>
    public static bool LastFiscalPeriod(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="LastFiscalPeriod(DateTime)" />
    public static bool LastFiscalPeriod(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-fiscal-year</c> condition operator.
    /// </summary>
    public static bool LastFiscalYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="LastFiscalYear(DateTime)" />
    public static bool LastFiscalYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-month</c> condition operator.
    /// </summary>
    public static bool LastMonth(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="LastMonth(DateTime)" />
    public static bool LastMonth(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-week</c> condition operator.
    /// </summary>
    public static bool LastWeek(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="LastWeek(DateTime)" />
    public static bool LastWeek(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-year</c> condition operator.
    /// </summary>
    public static bool LastYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="LastYear(DateTime)" />
    public static bool LastYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-seven-days</c> condition operator.
    /// </summary>
    public static bool Next7Days(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="Next7Days(DateTime)" />
    public static bool Next7Days(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-fiscal-period</c> condition operator.
    /// </summary>
    public static bool NextFiscalPeriod(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="NextFiscalPeriod(DateTime)" />
    public static bool NextFiscalPeriod(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-fiscal-year</c> condition operator.
    /// </summary>
    public static bool NextFiscalYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="NextFiscalYear(DateTime)" />
    public static bool NextFiscalYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-month</c> condition operator.
    /// </summary>
    public static bool NextMonth(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="NextMonth(DateTime)" />
    public static bool NextMonth(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-week</c> condition operator.
    /// </summary>
    public static bool NextWeek(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="NextWeek(DateTime)" />
    public static bool NextWeek(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-year</c> condition operator.
    /// </summary>
    public static bool NextYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="NextYear(DateTime)" />
    public static bool NextYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>this-fiscal-period</c> condition operator.
    /// </summary>
    public static bool ThisFiscalPeriod(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="ThisFiscalPeriod(DateTime)" />
    public static bool ThisFiscalPeriod(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>this-fiscal-year</c> condition operator.
    /// </summary>
    public static bool ThisFiscalYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="ThisFiscalYear(DateTime)" />
    public static bool ThisFiscalYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>this-month</c> condition operator.
    /// </summary>
    public static bool ThisMonth(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="ThisMonth(DateTime)" />
    public static bool ThisMonth(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>this-week</c> condition operator.
    /// </summary>
    public static bool ThisWeek(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="ThisWeek(DateTime)" />
    public static bool ThisWeek(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>this-year</c> condition operator.
    /// </summary>
    public static bool ThisYear(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="ThisYear(DateTime)" />
    public static bool ThisYear(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>today</c> condition operator.
    /// </summary>
    public static bool Today(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="Today(DateTime)" />
    public static bool Today(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>tomorrow</c> condition operator.
    /// </summary>
    public static bool Tomorrow(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="Tomorrow(DateTime)" />
    public static bool Tomorrow(this DateTime? dateTime) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>yesterday</c> condition operator.
    /// </summary>
    public static bool Yesterday(this DateTime dateTime) => Throw();
    /// <inheritdoc cref="Yesterday(DateTime)" />
    public static bool Yesterday(this DateTime? dateTime) => Throw();

    // -------------------------------------------------------------------------
    // Single int argument (LastX / NextX / OlderThanX / InFiscal*)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>last-x-days</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of days to look back.</param>
    public static bool LastXDays(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXDays(DateTime, int)" />
    public static bool LastXDays(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-fiscal-periods</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of fiscal periods to look back.</param>
    public static bool LastXFiscalPeriods(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXFiscalPeriods(DateTime, int)" />
    public static bool LastXFiscalPeriods(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-fiscal-years</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of fiscal years to look back.</param>
    public static bool LastXFiscalYears(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXFiscalYears(DateTime, int)" />
    public static bool LastXFiscalYears(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-hours</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of hours to look back.</param>
    public static bool LastXHours(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXHours(DateTime, int)" />
    public static bool LastXHours(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-months</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of months to look back.</param>
    public static bool LastXMonths(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXMonths(DateTime, int)" />
    public static bool LastXMonths(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-weeks</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of weeks to look back.</param>
    public static bool LastXWeeks(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXWeeks(DateTime, int)" />
    public static bool LastXWeeks(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>last-x-years</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of years to look back.</param>
    public static bool LastXYears(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="LastXYears(DateTime, int)" />
    public static bool LastXYears(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-days</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of days to look ahead.</param>
    public static bool NextXDays(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXDays(DateTime, int)" />
    public static bool NextXDays(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-fiscal-periods</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of fiscal periods to look ahead.</param>
    public static bool NextXFiscalPeriods(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXFiscalPeriods(DateTime, int)" />
    public static bool NextXFiscalPeriods(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-fiscal-years</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of fiscal years to look ahead.</param>
    public static bool NextXFiscalYears(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXFiscalYears(DateTime, int)" />
    public static bool NextXFiscalYears(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-hours</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of hours to look ahead.</param>
    public static bool NextXHours(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXHours(DateTime, int)" />
    public static bool NextXHours(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-months</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of months to look ahead.</param>
    public static bool NextXMonths(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXMonths(DateTime, int)" />
    public static bool NextXMonths(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-weeks</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of weeks to look ahead.</param>
    public static bool NextXWeeks(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXWeeks(DateTime, int)" />
    public static bool NextXWeeks(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>next-x-years</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of years to look ahead.</param>
    public static bool NextXYears(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="NextXYears(DateTime, int)" />
    public static bool NextXYears(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>olderthan-x-months</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="value">The number of months threshold.</param>
    public static bool OlderThanXMonths(this DateTime dateTime, int value) => Throw();
    /// <inheritdoc cref="OlderThanXMonths(DateTime, int)" />
    public static bool OlderThanXMonths(this DateTime? dateTime, int value) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>in-fiscal-year</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="fiscalYear">The fiscal year to match.</param>
    public static bool InFiscalYear(this DateTime dateTime, int fiscalYear) => Throw();
    /// <inheritdoc cref="InFiscalYear(DateTime, int)" />
    public static bool InFiscalYear(this DateTime? dateTime, int fiscalYear) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>in-fiscal-period</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="fiscalPeriod">The fiscal period to match.</param>
    public static bool InFiscalPeriod(this DateTime dateTime, int fiscalPeriod) => Throw();
    /// <inheritdoc cref="InFiscalPeriod(DateTime, int)" />
    public static bool InFiscalPeriod(this DateTime? dateTime, int fiscalPeriod) => Throw();

    // -------------------------------------------------------------------------
    // Single DateTime argument (On / OnOrBefore / OnOrAfter)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>on</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="specifiedDate">The date to compare against.</param>
    public static bool On(this DateTime dateTime, DateTime specifiedDate) => Throw();
    /// <inheritdoc cref="On(DateTime, DateTime)" />
    public static bool On(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>on-or-before</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="specifiedDate">The date to compare against.</param>
    public static bool OnOrBefore(this DateTime dateTime, DateTime specifiedDate) => Throw();
    /// <inheritdoc cref="OnOrBefore(DateTime, DateTime)" />
    public static bool OnOrBefore(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>on-or-after</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="specifiedDate">The date to compare against.</param>
    public static bool OnOrAfter(this DateTime dateTime, DateTime specifiedDate) => Throw();
    /// <inheritdoc cref="OnOrAfter(DateTime, DateTime)" />
    public static bool OnOrAfter(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    // -------------------------------------------------------------------------
    // Two int arguments (InFiscalPeriodAndYear / InOrBefore / InOrAfter)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>in-fiscal-period-and-year</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="fiscalPeriod">The fiscal period to match.</param>
    /// <param name="year">The year to match.</param>
    public static bool InFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    /// <inheritdoc cref="InFiscalPeriodAndYear(DateTime, int, int)" />
    public static bool InFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>in-or-before-fiscal-period-and-year</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="fiscalPeriod">The fiscal period to compare against.</param>
    /// <param name="year">The year to compare against.</param>
    public static bool InOrBeforeFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    /// <inheritdoc cref="InOrBeforeFiscalPeriodAndYear(DateTime, int, int)" />
    public static bool InOrBeforeFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>in-or-after-fiscal-period-and-year</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="fiscalPeriod">The fiscal period to compare against.</param>
    /// <param name="year">The year to compare against.</param>
    public static bool InOrAfterFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    /// <inheritdoc cref="InOrAfterFiscalPeriodAndYear(DateTime, int, int)" />
    public static bool InOrAfterFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    // -------------------------------------------------------------------------
    // Two DateTime arguments (Between / NotBetween)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps to the FetchXml <c>between</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="from">The start of the date range (inclusive).</param>
    /// <param name="to">The end of the date range (inclusive).</param>
    public static bool Between(this DateTime dateTime, DateTime from, DateTime to) => Throw();
    /// <inheritdoc cref="Between(DateTime, DateTime, DateTime)" />
    public static bool Between(this DateTime? dateTime, DateTime from, DateTime to) => Throw();

    /// <summary>
    /// Maps to the FetchXml <c>not-between</c> condition operator.
    /// </summary>
    /// <param name="dateTime">The date value to evaluate.</param>
    /// <param name="from">The start of the excluded date range.</param>
    /// <param name="to">The end of the excluded date range.</param>
    public static bool NotBetween(this DateTime dateTime, DateTime from, DateTime to) => Throw();
    /// <inheritdoc cref="NotBetween(DateTime, DateTime, DateTime)" />
    public static bool NotBetween(this DateTime? dateTime, DateTime from, DateTime to) => Throw();
}

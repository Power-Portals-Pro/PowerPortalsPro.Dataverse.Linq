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

    // -------------------------------------------------------------------------
    // No-arg operators (parameterless — the attribute is the 'this' parameter)
    // -------------------------------------------------------------------------

    public static bool Last7Days(this DateTime dateTime) => Throw();
    public static bool Last7Days(this DateTime? dateTime) => Throw();

    public static bool LastFiscalPeriod(this DateTime dateTime) => Throw();
    public static bool LastFiscalPeriod(this DateTime? dateTime) => Throw();

    public static bool LastFiscalYear(this DateTime dateTime) => Throw();
    public static bool LastFiscalYear(this DateTime? dateTime) => Throw();

    public static bool LastMonth(this DateTime dateTime) => Throw();
    public static bool LastMonth(this DateTime? dateTime) => Throw();

    public static bool LastWeek(this DateTime dateTime) => Throw();
    public static bool LastWeek(this DateTime? dateTime) => Throw();

    public static bool LastYear(this DateTime dateTime) => Throw();
    public static bool LastYear(this DateTime? dateTime) => Throw();

    public static bool Next7Days(this DateTime dateTime) => Throw();
    public static bool Next7Days(this DateTime? dateTime) => Throw();

    public static bool NextFiscalPeriod(this DateTime dateTime) => Throw();
    public static bool NextFiscalPeriod(this DateTime? dateTime) => Throw();

    public static bool NextFiscalYear(this DateTime dateTime) => Throw();
    public static bool NextFiscalYear(this DateTime? dateTime) => Throw();

    public static bool NextMonth(this DateTime dateTime) => Throw();
    public static bool NextMonth(this DateTime? dateTime) => Throw();

    public static bool NextWeek(this DateTime dateTime) => Throw();
    public static bool NextWeek(this DateTime? dateTime) => Throw();

    public static bool NextYear(this DateTime dateTime) => Throw();
    public static bool NextYear(this DateTime? dateTime) => Throw();

    public static bool ThisFiscalPeriod(this DateTime dateTime) => Throw();
    public static bool ThisFiscalPeriod(this DateTime? dateTime) => Throw();

    public static bool ThisFiscalYear(this DateTime dateTime) => Throw();
    public static bool ThisFiscalYear(this DateTime? dateTime) => Throw();

    public static bool ThisMonth(this DateTime dateTime) => Throw();
    public static bool ThisMonth(this DateTime? dateTime) => Throw();

    public static bool ThisWeek(this DateTime dateTime) => Throw();
    public static bool ThisWeek(this DateTime? dateTime) => Throw();

    public static bool ThisYear(this DateTime dateTime) => Throw();
    public static bool ThisYear(this DateTime? dateTime) => Throw();

    public static bool Today(this DateTime dateTime) => Throw();
    public static bool Today(this DateTime? dateTime) => Throw();

    public static bool Tomorrow(this DateTime dateTime) => Throw();
    public static bool Tomorrow(this DateTime? dateTime) => Throw();

    public static bool Yesterday(this DateTime dateTime) => Throw();
    public static bool Yesterday(this DateTime? dateTime) => Throw();

    // -------------------------------------------------------------------------
    // Single int argument (LastX / NextX / OlderThanX / InFiscal*)
    // -------------------------------------------------------------------------

    public static bool LastXDays(this DateTime dateTime, int value) => Throw();
    public static bool LastXDays(this DateTime? dateTime, int value) => Throw();

    public static bool LastXFiscalPeriods(this DateTime dateTime, int value) => Throw();
    public static bool LastXFiscalPeriods(this DateTime? dateTime, int value) => Throw();

    public static bool LastXFiscalYears(this DateTime dateTime, int value) => Throw();
    public static bool LastXFiscalYears(this DateTime? dateTime, int value) => Throw();

    public static bool LastXHours(this DateTime dateTime, int value) => Throw();
    public static bool LastXHours(this DateTime? dateTime, int value) => Throw();

    public static bool LastXMonths(this DateTime dateTime, int value) => Throw();
    public static bool LastXMonths(this DateTime? dateTime, int value) => Throw();

    public static bool LastXWeeks(this DateTime dateTime, int value) => Throw();
    public static bool LastXWeeks(this DateTime? dateTime, int value) => Throw();

    public static bool LastXYears(this DateTime dateTime, int value) => Throw();
    public static bool LastXYears(this DateTime? dateTime, int value) => Throw();

    public static bool NextXDays(this DateTime dateTime, int value) => Throw();
    public static bool NextXDays(this DateTime? dateTime, int value) => Throw();

    public static bool NextXFiscalPeriods(this DateTime dateTime, int value) => Throw();
    public static bool NextXFiscalPeriods(this DateTime? dateTime, int value) => Throw();

    public static bool NextXFiscalYears(this DateTime dateTime, int value) => Throw();
    public static bool NextXFiscalYears(this DateTime? dateTime, int value) => Throw();

    public static bool NextXHours(this DateTime dateTime, int value) => Throw();
    public static bool NextXHours(this DateTime? dateTime, int value) => Throw();

    public static bool NextXMonths(this DateTime dateTime, int value) => Throw();
    public static bool NextXMonths(this DateTime? dateTime, int value) => Throw();

    public static bool NextXWeeks(this DateTime dateTime, int value) => Throw();
    public static bool NextXWeeks(this DateTime? dateTime, int value) => Throw();

    public static bool NextXYears(this DateTime dateTime, int value) => Throw();
    public static bool NextXYears(this DateTime? dateTime, int value) => Throw();

    public static bool OlderThanXMonths(this DateTime dateTime, int value) => Throw();
    public static bool OlderThanXMonths(this DateTime? dateTime, int value) => Throw();

    public static bool InFiscalYear(this DateTime dateTime, int fiscalYear) => Throw();
    public static bool InFiscalYear(this DateTime? dateTime, int fiscalYear) => Throw();

    public static bool InFiscalPeriod(this DateTime dateTime, int fiscalPeriod) => Throw();
    public static bool InFiscalPeriod(this DateTime? dateTime, int fiscalPeriod) => Throw();

    // -------------------------------------------------------------------------
    // Single DateTime argument (On / OnOrBefore / OnOrAfter)
    // -------------------------------------------------------------------------

    public static bool On(this DateTime dateTime, DateTime specifiedDate) => Throw();
    public static bool On(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    public static bool OnOrBefore(this DateTime dateTime, DateTime specifiedDate) => Throw();
    public static bool OnOrBefore(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    public static bool OnOrAfter(this DateTime dateTime, DateTime specifiedDate) => Throw();
    public static bool OnOrAfter(this DateTime? dateTime, DateTime specifiedDate) => Throw();

    // -------------------------------------------------------------------------
    // Two int arguments (InFiscalPeriodAndYear / InOrBefore / InOrAfter)
    // -------------------------------------------------------------------------

    public static bool InFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    public static bool InFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    public static bool InOrBeforeFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    public static bool InOrBeforeFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    public static bool InOrAfterFiscalPeriodAndYear(this DateTime dateTime, int fiscalPeriod, int year) => Throw();
    public static bool InOrAfterFiscalPeriodAndYear(this DateTime? dateTime, int fiscalPeriod, int year) => Throw();

    // -------------------------------------------------------------------------
    // Two DateTime arguments (Between / NotBetween)
    // -------------------------------------------------------------------------

    public static bool Between(this DateTime dateTime, DateTime from, DateTime to) => Throw();
    public static bool Between(this DateTime? dateTime, DateTime from, DateTime to) => Throw();

    public static bool NotBetween(this DateTime dateTime, DateTime from, DateTime to) => Throw();
    public static bool NotBetween(this DateTime? dateTime, DateTime from, DateTime to) => Throw();
}

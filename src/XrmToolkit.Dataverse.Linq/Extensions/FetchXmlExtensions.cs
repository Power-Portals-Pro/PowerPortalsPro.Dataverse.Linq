using Microsoft.Xrm.Sdk.Query;

namespace XrmToolkit.Dataverse.Linq.Extensions;

internal static class FetchXmlExtensions
{
    public static string ToFetchXml(this ConditionOperator op)
    {
        return op switch
        {
            ConditionOperator.Equal => "eq",
            ConditionOperator.NotEqual
                or ConditionOperator.NotOn => "ne",
            ConditionOperator.LessThan => "lt",
            ConditionOperator.LessEqual => "le",
            ConditionOperator.GreaterThan => "gt",
            ConditionOperator.GreaterEqual => "ge",
            ConditionOperator.Null => "null",
            ConditionOperator.NotNull => "not-null",
            ConditionOperator.BeginsWith
                or ConditionOperator.Like
                or ConditionOperator.Contains => "like",
            ConditionOperator.DoesNotBeginWith
                or ConditionOperator.DoesNotContain
                or ConditionOperator.DoesNotEndWith
                or ConditionOperator.NotLike => "not-like",
            ConditionOperator.In => "in",
            ConditionOperator.NotIn => "not-in",
            ConditionOperator.Between => "between",
            ConditionOperator.NotBetween => "not-between",
            ConditionOperator.EqualBusinessId => "eq-businessid",
            ConditionOperator.EqualUserId => "eq-userid",
            ConditionOperator.NotEqualBusinessId => "ne-businessid",
            ConditionOperator.NotEqualUserId => "ne-userid",
            ConditionOperator.Last7Days => "last-seven-days",
            ConditionOperator.LastFiscalPeriod => "last-fiscal-period",
            ConditionOperator.LastFiscalYear => "last-fiscal-year",
            ConditionOperator.LastMonth => "last-month",
            ConditionOperator.LastWeek => "last-week",
            ConditionOperator.LastXDays => "last-x-days",
            ConditionOperator.LastXFiscalPeriods => "last-x-fiscal-periods",
            ConditionOperator.LastXFiscalYears => "last-x-fiscal-years",
            ConditionOperator.LastXHours => "last-x-hours",
            ConditionOperator.LastXMonths => "last-x-months",
            ConditionOperator.LastXWeeks => "last-x-weeks",
            ConditionOperator.LastXYears => "last-x-years",
            ConditionOperator.LastYear => "last-year",
            ConditionOperator.Next7Days => "next-seven-days",
            ConditionOperator.NextFiscalPeriod => "next-fiscal-period",
            ConditionOperator.NextFiscalYear => "next-fiscal-year",
            ConditionOperator.NextMonth => "next-month",
            ConditionOperator.NextWeek => "next-week",
            ConditionOperator.NextXDays => "next-x-days",
            ConditionOperator.NextXFiscalPeriods => "next-x-fiscal-periods",
            ConditionOperator.NextXFiscalYears => "next-x-fiscal-years",
            ConditionOperator.NextXHours => "next-x-hours",
            ConditionOperator.NextXMonths => "next-x-months",
            ConditionOperator.NextXWeeks => "next-x-weeks",
            ConditionOperator.NextXYears => "next-x-years",
            ConditionOperator.NextYear => "next-year",
            ConditionOperator.InFiscalPeriod => "in-fiscal-period",
            ConditionOperator.InFiscalPeriodAndYear => "in-fiscal-period-and-year",
            ConditionOperator.InFiscalYear => "in-fiscal-year",
            ConditionOperator.InOrAfterFiscalPeriodAndYear => "in-or-after-fiscal-period-and-year",
            ConditionOperator.InOrBeforeFiscalPeriodAndYear => "in-or-before-fiscal-period-and-year",
            ConditionOperator.OlderThanXMonths => "olderthan-x-months",
            ConditionOperator.On => "on",
            ConditionOperator.OnOrAfter => "on-or-after",
            ConditionOperator.OnOrBefore => "on-or-before",
            ConditionOperator.ThisFiscalPeriod => "this-fiscal-period",
            ConditionOperator.ThisFiscalYear => "this-fiscal-year",
            ConditionOperator.ThisMonth => "this-month",
            ConditionOperator.ThisWeek => "this-week",
            ConditionOperator.ThisYear => "this-year",
            ConditionOperator.Today => "today",
            ConditionOperator.Tomorrow => "tomorrow",
            ConditionOperator.Yesterday => "yesterday",
            ConditionOperator.Above => "above",
            ConditionOperator.AboveOrEqual => "eq-or-above",
            ConditionOperator.Under => "under",
            ConditionOperator.UnderOrEqual => "eq-or-under",
            ConditionOperator.NotUnder => "not-under",
            ConditionOperator.EqualUserOrUserHierarchy => "eq-useroruserhierarchy",
            ConditionOperator.EqualUserOrUserHierarchyAndTeams => "eq-useroruserhierarchyandteams",
            ConditionOperator.ContainValues => "contain-values",
            ConditionOperator.DoesNotContainValues => "not-contain-values",
            _ => throw new NotSupportedException($"The '{op}' operator is not supported.")
        };
    }
}

# PowerPortalsPro.Dataverse.Linq

A strongly-typed LINQ query provider for Microsoft Dataverse (Dynamics 365 / Power Platform) that translates standard LINQ expressions into [FetchXml](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/overview) and executes them against the Dataverse API. Write queries using familiar C# syntax instead of hand-crafting FetchXml strings.

## Installation

```
dotnet add package PowerPortalsPro.Dataverse.Linq
```

Supports **.NET 10+** (with full async support) and **.NET Framework 4.6.2+** (sync only).

## Quick Start

```csharp
using PowerPortalsPro.Dataverse.Linq;

var accounts = await service.Queryable<Account>()
    .Where(a => a.Name.StartsWith("Contoso"))
    .OrderBy(a => a.Name)
    .ToListAsync();
```

## Entry Points

The `Queryable()` extension method creates a LINQ queryable from your service connection. On .NET 10+ it extends `IOrganizationServiceAsync`; on .NET Framework 4.6.2+ it extends `IOrganizationService`.

```csharp
// Typed query — entity type must have [EntityLogicalName] attribute
var accounts = await service.Queryable<Account>().ToListAsync();

// Typed query with explicit column set
var accounts = await service.Queryable<Account>("name", "revenue").ToListAsync();

// Unbound query — no proxy class required
var results = await service.Queryable("account", "name", "revenue").ToListAsync();
```

## Filtering

Standard C# comparison operators, null checks, and string methods translate directly to [FetchXml conditions](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/filter-rows). All supported [condition operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators) are listed below.

### Comparison and Equality

Translates to FetchXml operators [`eq`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq), [`ne`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#ne), [`lt`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#lt), [`le`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#le), [`gt`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#gt), [`ge`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#ge), [`null`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#null), [`not-null`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#not-null).

```csharp
.Where(a => a.Name == "Contoso")
.Where(a => a.Revenue > 1000000 && a.Revenue <= 5000000)
.Where(a => a.Description != null)
```

### String Methods

`Contains`, `StartsWith`, and `EndsWith` translate to [`like` / `not-like`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#like) with appropriate wildcard patterns. `string.IsNullOrEmpty()` translates to a `null` OR `eq ""` filter.

```csharp
.Where(a => a.Name.Contains("Corp"))
.Where(a => a.Name.StartsWith("A"))
.Where(a => a.Name.EndsWith("Inc"))
.Where(a => !string.IsNullOrEmpty(a.Email))
```

### String Length

`string.Length` comparisons translate to `like` / `not-like` patterns using underscore wildcards.

```csharp
.Where(a => a.Name.Length == 10)
.Where(a => a.Name.Length > 5)
```

### In / Not In

`Contains()` on a collection translates to [`in`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in) / [`not-in`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#not-in).

```csharp
var names = new[] { "Contoso", "Fabrikam", "Northwind" };
.Where(a => names.Contains(a.Name))
.Where(a => !names.Contains(a.Name))

// Also works with Guid collections
var ids = new[] { id1, id2, id3 };
.Where(a => ids.Contains(a.AccountId))
```

### Column-to-Column Comparison

Compare two columns in the same row using the FetchXml [`valueof`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/condition) attribute. Also works across joined entities.

```csharp
.Where(c => c.FirstName == c.LastName)
.Where(o => o.ActualRevenue > o.EstimatedRevenue)
```

### Negation

Prefix any boolean filter with `!` to negate it:

```csharp
.Where(a => !a.Name.Contains("Test"))
.Where(a => !a.CreatedOn.LastXDays(30))
```

### Subquery Filtering (Any, All, Exists)

These operators filter rows based on the existence or properties of related records, translating to FetchXml [link-entity](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/link-entity) elements with special `link-type` values. Each predicate must include a join condition relating the inner entity to the outer entity.

#### Any — at least one match

`Any()` translates to `link-type="any"` (nested inside a `<filter>`). Returns parent rows where **at least one** related child matches.

```csharp
// Contacts that are the primary contact of an account named "Contoso"
await service.Queryable<Contact>()
    .Where(c => service.Queryable<Account>().Any(
        a => a.PrimaryContactId.Id == c.ContactId && a.Name == "Contoso"))
    .ToListAsync();

// Negate with ! → link-type="not any" (no matching children)
await service.Queryable<Contact>()
    .Where(c => !service.Queryable<Account>().Any(
        a => a.PrimaryContactId.Id == c.ContactId))
    .ToListAsync();
```

#### All — every child matches

`All()` translates to `link-type="all"`. Returns parent rows where **every** related child satisfies the condition. Parents with no children are excluded (no vacuous truth).

The LINQ predicate is automatically negated in the generated FetchXml (including DeMorgan's law for nested `&&`/`||` conditions), because FetchXml `link-type="all"` uses [inverted filter semantics](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/link-entity) — the filter describes what would cause a child to *fail*.

```csharp
// Contacts where ALL linked accounts have a non-null rating
await service.Queryable<Contact>()
    .Where(c => service.Queryable<Account>().All(
        a => a.PrimaryContactId.Id == c.ContactId
             && a.AccountRating != null))
    .ToListAsync();

// Negate with ! → link-type="not all" (at least one child fails)
await service.Queryable<Contact>()
    .Where(c => !service.Queryable<Account>().All(
        a => a.PrimaryContactId.Id == c.ContactId
             && a.Name == "Contoso"))
    .ToListAsync();

// Complex predicate with OR — DeMorgan applied automatically
// All(Name == "Contoso" || Rating != null) generates:
//   link-type="all" with filter: Name != "Contoso" AND Rating == null
await service.Queryable<Contact>()
    .Where(c => service.Queryable<Account>().All(
        a => a.PrimaryContactId.Id == c.ContactId
             && (a.Name == "Contoso" || a.AccountRating != null)))
    .ToListAsync();
```

`All()` and `!All()` are complementary within the set of parents that have at least one related child.

#### Exists and In — semi-join operators

`Exists()` and `In()` translate to [`link-type="exists"`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/link-entity) and `link-type="in"` respectively, placed as direct children of the `<entity>` element (not inside a `<filter>`). Both are [semi-joins](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/link-entity) — they filter parent rows based on the existence of matching child rows without returning columns from the related entity.

`Exists()` and `In()` are semantically equivalent but may have different performance characteristics depending on the Dataverse query optimizer. `Exists` uses a correlated subquery while `In` uses an `IN` subquery. Try both if you encounter performance issues with large datasets.

```csharp
// Accounts that have at least one active contact (EXISTS subquery)
await service.Queryable<Account>()
    .Where(a => service.Queryable<Contact>().Exists(
        c => c.ParentCustomerId.Id == a.AccountId && c.StateCode == 0))
    .ToListAsync();

// Same query using IN subquery
await service.Queryable<Account>()
    .Where(a => service.Queryable<Contact>().In(
        c => c.ParentCustomerId.Id == a.AccountId && c.StateCode == 0))
    .ToListAsync();

// Negate with ! — Dataverse doesn't support "not exists" or "not in" as
// link-types, so negation automatically falls back to link-type="not any"
await service.Queryable<Account>()
    .Where(a => !service.Queryable<Contact>().Exists(
        c => c.ParentCustomerId.Id == a.AccountId))
    .ToListAsync();

// Join-only (no filter) — check for any related record
await service.Queryable<Account>()
    .Where(a => service.Queryable<Contact>().Exists(
        c => c.ParentCustomerId.Id == a.AccountId))
    .ToListAsync();
```

#### Summary

| LINQ Operator | FetchXml `link-type` | Placement | Behavior |
|---|---|---|---|
| `.Any(predicate)` | `any` | Inside `<filter>` | At least one child matches |
| `!.Any(predicate)` | `not any` | Inside `<filter>` | No children match |
| `.All(predicate)` | `all` | Inside `<filter>` | All children match (conditions negated) |
| `!.All(predicate)` | `not all` | Inside `<filter>` | At least one child fails (conditions negated) |
| `.Exists(predicate)` | `exists` | Direct child of `<entity>` | Semi-join via correlated subquery |
| `.In(predicate)` | `in` | Direct child of `<entity>` | Semi-join via IN subquery |
| `!.Exists(predicate)` | `not any` (fallback) | Inside `<filter>` | No matching children |
| `!.In(predicate)` | `not any` (fallback) | Inside `<filter>` | No matching children |

### DateTime Operators

Extension methods in `PowerPortalsPro.Dataverse.Linq` map to all [FetchXml datetime condition operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#datetime-data):

```csharp
using PowerPortalsPro.Dataverse.Linq;
```

**Parameterless operators:**

| Method | FetchXml Operator |
|--------|------------------|
| `.Today()` | [`today`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#today) |
| `.Yesterday()` | [`yesterday`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#yesterday) |
| `.Tomorrow()` | [`tomorrow`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#tomorrow) |
| `.ThisWeek()` | [`this-week`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#this-week) |
| `.ThisMonth()` | [`this-month`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#this-month) |
| `.ThisYear()` | [`this-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#this-year) |
| `.ThisFiscalPeriod()` | [`this-fiscal-period`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#this-fiscal-period) |
| `.ThisFiscalYear()` | [`this-fiscal-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#this-fiscal-year) |
| `.Last7Days()` | [`last-seven-days`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-seven-days) |
| `.LastWeek()` | [`last-week`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-week) |
| `.LastMonth()` | [`last-month`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-month) |
| `.LastYear()` | [`last-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-year) |
| `.LastFiscalPeriod()` | [`last-fiscal-period`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-fiscal-period) |
| `.LastFiscalYear()` | [`last-fiscal-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-fiscal-year) |
| `.Next7Days()` | [`next-seven-days`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-seven-days) |
| `.NextWeek()` | [`next-week`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-week) |
| `.NextMonth()` | [`next-month`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-month) |
| `.NextYear()` | [`next-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-year) |
| `.NextFiscalPeriod()` | [`next-fiscal-period`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-fiscal-period) |
| `.NextFiscalYear()` | [`next-fiscal-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-fiscal-year) |

**Parameterized operators:**

| Method | FetchXml Operator |
|--------|------------------|
| `.LastXDays(n)` | [`last-x-days`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-days) |
| `.LastXWeeks(n)` | [`last-x-weeks`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-weeks) |
| `.LastXMonths(n)` | [`last-x-months`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-months) |
| `.LastXYears(n)` | [`last-x-years`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-years) |
| `.LastXHours(n)` | [`last-x-hours`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-hours) |
| `.LastXFiscalPeriods(n)` | [`last-x-fiscal-periods`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-fiscal-periods) |
| `.LastXFiscalYears(n)` | [`last-x-fiscal-years`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#last-x-fiscal-years) |
| `.NextXDays(n)` | [`next-x-days`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-days) |
| `.NextXWeeks(n)` | [`next-x-weeks`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-weeks) |
| `.NextXMonths(n)` | [`next-x-months`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-months) |
| `.NextXYears(n)` | [`next-x-years`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-years) |
| `.NextXHours(n)` | [`next-x-hours`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-hours) |
| `.NextXFiscalPeriods(n)` | [`next-x-fiscal-periods`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-fiscal-periods) |
| `.NextXFiscalYears(n)` | [`next-x-fiscal-years`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#next-x-fiscal-years) |
| `.OlderThanXMonths(n)` | [`olderthan-x-months`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#olderthan-x-months) |
| `.On(date)` | [`on`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#on) |
| `.OnOrAfter(date)` | [`on-or-after`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#on-or-after) |
| `.OnOrBefore(date)` | [`on-or-before`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#on-or-before) |
| `.Between(from, to)` | [`between`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#between) |
| `.NotBetween(from, to)` | [`not-between`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#not-between) |
| `.InFiscalYear(year)` | [`in-fiscal-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in-fiscal-year) |
| `.InFiscalPeriod(period)` | [`in-fiscal-period`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in-fiscal-period) |
| `.InFiscalPeriodAndYear(period, year)` | [`in-fiscal-period-and-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in-fiscal-period-and-year) |
| `.InOrBeforeFiscalPeriodAndYear(p, y)` | [`in-or-before-fiscal-period-and-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in-or-before-fiscal-period-and-year) |
| `.InOrAfterFiscalPeriodAndYear(p, y)` | [`in-or-after-fiscal-period-and-year`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#in-or-after-fiscal-period-and-year) |

All DateTime methods have both `DateTime` and `DateTime?` overloads.

### User and Business Unit Operators

Extension methods for [user/business unit condition operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#unique-identifier-data):

```csharp
using PowerPortalsPro.Dataverse.Linq;
```

| Method | FetchXml Operator |
|--------|------------------|
| `.EqualUserId()` | [`eq-userid`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-userid) |
| `.NotEqualUserId()` | [`ne-userid`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#ne-userid) |
| `.EqualBusinessId()` | [`eq-businessid`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-businessid) |
| `.NotEqualBusinessId()` | [`ne-businessid`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#ne-businessid) |

```csharp
.Where(a => a.OwnerId.Id.EqualUserId())         // Current user's records
.Where(a => a.OwningBusinessUnit.Id.EqualBusinessId())  // Current business unit
```

### Hierarchy Operators

Extension methods for [hierarchical condition operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#hierarchical-data):

```csharp
using PowerPortalsPro.Dataverse.Linq;
```

| Method | FetchXml Operator |
|--------|------------------|
| `.Above(id)` | [`above`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#above) |
| `.AboveOrEqual(id)` | [`eq-or-above`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-or-above) |
| `.Under(id)` | [`under`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#under) |
| `.UnderOrEqual(id)` | [`eq-or-under`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-or-under) |
| `.NotUnder(id)` | [`not-under`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#not-under) |
| `.EqualUserOrUserHierarchy()` | [`eq-useroruserhierarchy`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-useroruserhierarchy) |
| `.EqualUserOrUserHierarchyAndTeams()` | [`eq-useroruserhierarchyandteams`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#eq-useroruserhierarchyandteams) |

```csharp
.Where(a => a.AccountId.Under(parentId))
.Where(a => a.AccountId.AboveOrEqual(childId))
```

### Multi-Select Option Sets

Extension methods for [choice column operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#choice-data) ([`contain-values`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#contain-values) / [`not-contain-values`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators#not-contain-values)):

```csharp
using PowerPortalsPro.Dataverse.Linq;

// ContainsValues — contain-values operator
.Where(c => c.PreferredMethods.ContainsValues(Method.Email, Method.Phone))

// Negated — not-contain-values operator
.Where(c => !c.PreferredMethods.ContainsValues(Method.Email))

// Single-item Contains — contain-values for one value
.Where(c => c.PreferredMethods.Contains(Method.Email))

// Equals — eq/ne for single value, in/not-in for multiple
.Where(c => c.PreferredMethods.Equals(Method.Email))
.Where(c => c.PreferredMethods.Equals(new[] { Method.Email, Method.Phone }))
```

## Projections

Select specific columns using [FetchXml attribute elements](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/select-columns). Only the selected columns are retrieved from Dataverse.

```csharp
// Anonymous type projection
var results = await service.Queryable<Account>()
    .Select(a => new { a.Name, a.Revenue, a.PrimaryContact })
    .ToListAsync();

// Project into entity type
var results = await service.Queryable<Account>()
    .Select(a => new Account { AccountId = a.AccountId, Name = a.Name })
    .ToListAsync();

// Ternary / null-coalesce expressions are supported in projections
.Select(a => new { a.AccountId, IsPreferred = a.IsPreferred ?? false })
```

## Joins

Inner joins and left joins using standard LINQ `join` syntax translate to [FetchXml link-entity elements](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/join-tables).

```csharp
// Inner join
var results = await (from a in service.Queryable<Account>()
                     join c in service.Queryable<Contact>()
                         on a.AccountId equals c.ParentCustomerId.Id
                     where a.Name != null
                     orderby c.LastName
                     select new { AccountName = a.Name, ContactName = c.FullName })
                    .ToListAsync();

// Left join (DefaultIfEmpty)
var results = await (from a in service.Queryable<Account>()
                     join c in service.Queryable<Contact>()
                         on a.AccountId equals c.ParentCustomerId.Id into contacts
                     from c in contacts.DefaultIfEmpty()
                     select new { a.Name })
                    .ToListAsync();

// Three-way join
var results = await (from a in service.Queryable<Account>()
                     join c in service.Queryable<Contact>()
                         on a.PrimaryContactId.Id equals c.ContactId
                     join pa in service.Queryable<Account>()
                         on c.ParentCustomerId.Id equals pa.AccountId
                     select new { a.Name, ContactName = c.FullName, ParentName = pa.Name })
                    .ToListAsync();

// Join with explicit column sets
var results = await (from a in service.Queryable<Account>("name", "createdon")
                     join c in service.Queryable<Contact>("firstname", "lastname")
                         on a.PrimaryContactId.Id equals c.ContactId
                     select new { a.Name, c.FirstName, c.LastName })
                    .ToListAsync();

// Sub-query: join on pre-filtered queryables
var activeAccounts = service.Queryable<Account>().Where(a => a.StateCode == 0);
var results = await (from a in activeAccounts
                     join c in service.Queryable<Contact>()
                         on a.PrimaryContactId.Id equals c.ContactId
                     select new { a.Name, c.FullName })
                    .ToListAsync();

// First-row join — returns only the first matching child per parent
// Translates to link-type="matchfirstrowusingcrossapply"
var results = await (from c in service.Queryable<Contact>()
                     join t in service.Queryable<Task>().WithFirstRow()
                         on c.ContactId equals t.RegardingObjectId.Id
                     select new { c.FullName, t.Subject })
                    .ToListAsync();
```

## Ordering

Translates to [FetchXml order elements](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/order-rows).

```csharp
.OrderBy(a => a.Name)
.OrderByDescending(a => a.Revenue)

// Multiple sort criteria (including across joined entities)
from a in service.Queryable<Account>()
join c in service.Queryable<Contact>() on a.PrimaryContactId.Id equals c.ContactId
orderby c.LastName, a.Name
select new { a.Name, c.LastName }
```

## Paging

Control [FetchXml paging](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/page-results) with `WithPageSize`, `WithPage`, and `Take`.

```csharp
// Page size and page number
var page2 = await service.Queryable<Account>()
    .OrderBy(a => a.Name)
    .WithPageSize(50)
    .WithPage(2)
    .ToListAsync();

// Top N records (FetchXml top attribute)
var top10 = await service.Queryable<Account>()
    .OrderByDescending(a => a.Revenue)
    .Take(10)
    .ToListAsync();
```

### ForEachPage / ForEachPageAsync

When calling `ToList()` or `ToListAsync()`, all pages are fetched automatically and combined into a single list. This is simple but loads the entire result set into memory at once, which may not be desirable for large data sets.

`ForEachPage` and `ForEachPageAsync` give you control over how each page is processed as it arrives from Dataverse. This is useful when you need to:

- **Process records in batches** without loading everything into memory
- **Stream results** to an external system as they arrive
- **Apply back-pressure** or throttling between pages
- **Track progress** across large data sets

Use `WithPageSize()` to control how many records are returned per page. Without it, Dataverse uses its default page size (up to 5,000 records), meaning all results may come back in a single page.

```csharp
// Process 100 records at a time
await service.Queryable<Account>()
    .Where(a => a.StateCode == 0)
    .WithPageSize(100)
    .ForEachPageAsync(async page =>
    {
        Console.WriteLine($"Processing batch of {page.Count} accounts...");
        foreach (var account in page)
            await ProcessAccountAsync(account);
    });
```

`ForEachPageAsync` accepts a `Func<List<T>, Task>` callback and supports a `CancellationToken`. A synchronous `ForEachPage` overload accepting `Action<List<T>>` is also available. Both methods work with `Where`, `Select`, `OrderBy`, and other query operators — the filtering and projection are applied server-side in the FetchXml, so each page contains only the matching, projected results.

## Distinct

Translates to the [FetchXml `distinct` attribute](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/overview#return-distinct-results):

```csharp
var uniqueNames = await service.Queryable<Account>()
    .Select(a => a.Name)
    .Distinct()
    .ToListAsync();
```

## Aggregations

Standard LINQ aggregate methods translate to [FetchXml aggregate queries](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data).

| LINQ Method | FetchXml Aggregate |
|-------------|-------------------|
| `.Count()` | [`count`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data) |
| `.LongCount()` | `count` (returns `long`) |
| `.CountColumn()` | [`countcolumn`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data#distinct-column-values) |
| `.Min(selector)` | [`min`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data) |
| `.Max(selector)` | [`max`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data) |
| `.Sum()` | [`sum`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data) |
| `.Average(selector)` | [`avg`](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data) |

```csharp
var count = await service.Queryable<Account>().CountAsync();
var count = await service.Queryable<Account>().CountAsync(a => a.Revenue > 0);
var max   = await service.Queryable<Account>().MaxAsync(a => a.Revenue);
var sum   = await service.Queryable<Account>().Select(a => a.NumberOfEmployees).SumAsync();
var avg   = await service.Queryable<Account>().Select(a => a.PercentComplete).AverageAsync();

// CountColumnAsync — counts non-null values only
var nonNull = await service.Queryable<Account>()
    .Select(a => a.NumberOfEmployees)
    .CountColumnAsync();

// CountChildren — row aggregate for hierarchical entities
var results = await service.Queryable<Account>()
    .Select(a => new { a.Name, Children = a.CountChildren() })
    .ToListAsync();
```

## GroupBy

`GroupBy` translates to [FetchXml grouping with aggregation](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data#grouping).

```csharp
// Simple group with aggregates
var results = await (from a in service.Queryable<Account>()
                     group a by a.IndustryCode into g
                     select new
                     {
                         Industry = g.Key,
                         Count = g.Count(),
                         TotalRevenue = g.Sum(x => x.Revenue),
                         AverageRevenue = g.Average(x => x.Revenue),
                         MaxEmployees = g.Max(x => x.NumberOfEmployees),
                         MinEmployees = g.Min(x => x.NumberOfEmployees),
                         DescriptionCount = g.CountColumn(x => x.Description),
                     }).ToListAsync();

// Group by constant — aggregate without grouping
var totals = await (from a in service.Queryable<Account>()
                    group a by 1 into g
                    select new
                    {
                        Count = g.Count(),
                        Total = g.Sum(x => x.Revenue),
                    }).FirstAsync();

// Join + GroupBy — aggregate on linked entity
var results = await (from c in service.Queryable<Contact>()
                     join o in service.Queryable<Opportunity>()
                         on c.ContactId equals o.ParentContactId.Id
                     group o by c.ContactId into g
                     select new
                     {
                         ContactId = g.Key,
                         Count = g.Count(),
                         TotalRevenue = g.Sum(x => x.ActualRevenue),
                     }).ToListAsync();
```

### Date Grouping

Group by [date parts](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data#grouping-by-parts-of-a-date) using the `dategrouping` FetchXml attribute:

| C# Expression | FetchXml `dategrouping` |
|---------------|------------------------|
| `.Value.Year` | `year` |
| `.Value.Month` | `month` |
| `.Value.Day` | `day` |
| `.Week()` | `week` |
| `.Quarter()` | `quarter` |
| `.FiscalPeriod()` | `fiscal-period` |
| `.FiscalYear()` | `fiscal-year` |

```csharp
using PowerPortalsPro.Dataverse.Linq;

var byYear = await (from o in service.Queryable<Opportunity>()
                    group o by o.ActualCloseDate.Value.Year into g
                    orderby g.Key
                    select new { Year = g.Key, Count = g.Count() })
                   .ToListAsync();

var byQuarter = await (from o in service.Queryable<Opportunity>()
                       group o by o.ActualCloseDate.Value.Quarter() into g
                       select new { Quarter = g.Key, Count = g.Count() })
                      .ToListAsync();

var byFiscalYear = await (from o in service.Queryable<Opportunity>()
                          group o by o.ActualCloseDate.Value.FiscalYear() into g
                          select new { FiscalYear = g.Key, Count = g.Count() })
                         .ToListAsync();
```

### OptionSet Grouping

Group by `OptionSetValue.Value` for choice columns:

```csharp
var results = await (from o in service.Queryable<Opportunity>()
                     group o by o.StatusReason.Value into g
                     select new { Status = g.Key, Count = g.Count() })
                    .ToListAsync();
```

## Unbound Queries

Query without a typed proxy class using `GetAttributeValue`:

```csharp
var results = await service.Queryable("account", "name", "revenue")
    .Where(e => !string.IsNullOrEmpty(e.GetAttributeValue<string>("name")))
    .Select(e => new
    {
        Name = e.GetAttributeValue<string>("name"),
        Revenue = e.GetAttributeValue<Money>("revenue")
    })
    .ToListAsync();
```

## Query Options

Configure [FetchXml attributes](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/fetch) on the query:

| Method | FetchXml Attribute | Description |
|--------|-------------------|-------------|
| `.WithPageSize(n)` | `count` | [Page size](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/page-results) |
| `.WithPage(n)` | `page` | [Page number](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/page-results) (1-based) |
| `.WithAggregateLimit(n)` | `aggregatelimit` | [Aggregate row limit](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data#per-query-limit) (1-50,000) |
| `.WithDatasource(FetchDatasource.Retained)` | `datasource` | Query [long-term retained data](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/long-term-retention) |
| `.WithLateMaterialize()` | `latematerialize` | [Late materialization](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/optimize-performance#late-materialize-query) optimization |
| `.WithQueryHints(...)` | `options` | [SQL query hints](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/fetch) (ForceOrder, HashJoin, etc.) |
| `.WithUseRawOrderBy()` | `useraworderby` | Sort choice columns by integer value |
| `.WithFirstRow()` | `matchfirstrowusingcrossapply` | On join inner source: return only the first matching child row |
| `.WithNoLock()` | `no-lock` | Deprecated, no effect |

```csharp
await service.Queryable<Account>()
    .WithPageSize(100)
    .WithLateMaterialize()
    .WithQueryHints(SqlQueryHint.ForceOrder, SqlQueryHint.DisableRowGoal)
    .ToListAsync();
```

Available `SqlQueryHint` values: `ForceOrder`, `DisableRowGoal`, `EnableOptimizerHotfixes`, `LoopJoin`, `MergeJoin`, `HashJoin`, `NoPerformanceSpool`, `EnableHistAmendmentForAscKeys`.

## Inspect Generated FetchXml

Use `ToFetchXml()` to see the generated FetchXml without executing the query:

```csharp
var fetchXml = service.Queryable<Account>()
    .Where(a => a.Name != null)
    .OrderBy(a => a.Name)
    .Select(a => new { a.Name, a.Revenue })
    .ToFetchXml();

Console.WriteLine(fetchXml);
```

## Async Operations (.NET 10+)

All query execution methods have async counterparts. These are only available when targeting .NET 10+.

| Sync | Async |
|------|-------|
| `.ToList()` | `.ToListAsync()` |
| `.First()` | `.FirstAsync()` |
| `.FirstOrDefault()` | `.FirstOrDefaultAsync()` |
| `.Single()` | `.SingleAsync()` |
| `.SingleOrDefault()` | `.SingleOrDefaultAsync()` |
| `.Count()` | `.CountAsync()` |
| `.LongCount()` | `.LongCountAsync()` |
| `.Min(selector)` | `.MinAsync(selector)` |
| `.Max(selector)` | `.MaxAsync(selector)` |
| `.Sum()` | `.SumAsync()` |
| `.Average()` | `.AverageAsync()` |
| `.CountColumn()` | `.CountColumnAsync()` |
| `.ForEachPage(action)` | `.ForEachPageAsync(func)` |

`SumAsync` and `AverageAsync` have overloads for `int`, `int?`, `decimal`, and `decimal?`. `FirstAsync`, `FirstOrDefaultAsync`, `SingleAsync`, `SingleOrDefaultAsync`, and `CountAsync` have overloads accepting a predicate.

```csharp
var accounts = await service.Queryable<Account>().ToListAsync();
var first = await service.Queryable<Account>().FirstAsync(a => a.Name == "Contoso");
var count = await service.Queryable<Account>().CountAsync();
var sum = await service.Queryable<Account>().Select(a => a.Revenue).SumAsync();
```

See the [ForEachPage / ForEachPageAsync](#foreachpage--foreachpageasync) section for async paged processing examples.

## Requirements

| Target | SDK Package |
|--------|------------|
| .NET 10+ | [Microsoft.PowerPlatform.Dataverse.Client](https://www.nuget.org/packages/Microsoft.PowerPlatform.Dataverse.Client) |
| .NET Framework 4.6.2+ | [Microsoft.CrmSdk.CoreAssemblies](https://www.nuget.org/packages/Microsoft.CrmSdk.CoreAssemblies) |

## FetchXml Documentation

For more information about FetchXml, see the official Microsoft documentation:

- [Query data using FetchXml](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/overview) - Overview
- [FetchXml reference](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/) - Element reference
- [Select columns](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/select-columns)
- [Join tables](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/join-tables)
- [Order rows](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/order-rows)
- [Filter rows](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/filter-rows)
- [Condition operators](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/reference/operators) - Full operator reference
- [Page results](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/page-results)
- [Aggregate data](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/aggregate-data)
- [Count rows](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/count-rows)
- [Optimize performance](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/optimize-performance)
- [Query hierarchical data](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/query-hierarchical-data)

## License

See the EULA for the license terms.

# XrmToolkit.Dataverse.Linq

A strongly-typed LINQ query provider for Microsoft Dataverse (Dynamics 365 / Power Platform) that translates standard LINQ expressions into FetchXml and executes them against the Dataverse API. Write queries using familiar C# syntax instead of hand-crafting FetchXml strings.

## Installation

```
dotnet add package XrmToolkit.Dataverse.Linq
```

Supports **.NET 10+** (with full async support) and **.NET Framework 4.6.2+** (sync only).

## Quick Start

```csharp
using XrmToolkit.Dataverse.Linq;

// Create a queryable from any IOrganizationService / IOrganizationServiceAsync
var accounts = service.Queryable<Account>()
    .Where(a => a.Name.StartsWith("Contoso"))
    .OrderBy(a => a.Name)
    .ToList();

// Async support (.NET 10+)
var accounts = await service.Queryable<Account>()
    .Where(a => a.Name.StartsWith("Contoso"))
    .OrderBy(a => a.Name)
    .ToListAsync();
```

## Features

### Filtering

Standard comparison, null checks, string methods, and `Contains` (in-list) are all supported:

```csharp
// Equality, comparison, null checks
.Where(a => a.Name == "Contoso" && a.Revenue > 1000000 && a.Description != null)

// String methods
.Where(a => a.Name.Contains("Corp") || a.Name.StartsWith("A") || a.Name.EndsWith("Inc"))

// In / Not In
var targetNames = new[] { "Contoso", "Fabrikam", "Northwind" };
.Where(a => targetNames.Contains(a.Name))

// IsNullOrEmpty
.Where(a => !string.IsNullOrEmpty(a.Email))

// String length
.Where(a => a.Name.Length > 10)
```

### Projections

```csharp
// Anonymous type projection (only selected columns are retrieved)
var results = service.Queryable<Account>()
    .Select(a => new { a.Name, a.Revenue, a.PrimaryContact })
    .ToList();

// Project into entity type
var results = service.Queryable<Account>()
    .Select(a => new Account { AccountId = a.AccountId, Name = a.Name })
    .ToList();
```

### Joins

Inner joins and left joins using standard LINQ syntax:

```csharp
// Inner join
var results = (from a in service.Queryable<Account>()
               join c in service.Queryable<Contact>()
                   on a.AccountId equals c.ParentCustomerId.Id
               where a.Name != null
               orderby c.LastName
               select new { AccountName = a.Name, ContactName = c.FullName })
              .ToList();

// Left join
var results = (from a in service.Queryable<Account>()
               join c in service.Queryable<Contact>()
                   on a.AccountId equals c.ParentCustomerId.Id into contacts
               from c in contacts.DefaultIfEmpty()
               select new { a.Name })
              .ToList();
```

### Ordering and Paging

```csharp
var page = service.Queryable<Account>()
    .OrderBy(a => a.Name)
    .WithPageSize(50)
    .WithPage(2)
    .ToList();

// Take (top N)
var top10 = service.Queryable<Account>()
    .OrderByDescending(a => a.Revenue)
    .Take(10)
    .ToList();
```

### Aggregations

```csharp
var count = service.Queryable<Account>().Count();
var maxRevenue = service.Queryable<Account>().Max(a => a.Revenue);
var totalEmployees = service.Queryable<Account>()
    .Select(a => a.NumberOfEmployees)
    .Sum();
```

### GroupBy

```csharp
var results = (from a in service.Queryable<Account>()
               group a by a.IndustryCode into g
               select new
               {
                   Industry = g.Key,
                   Count = g.Count(),
                   TotalRevenue = g.Sum(x => x.Revenue),
                   AverageRevenue = g.Average(x => x.Revenue),
               }).ToList();

// Group by date parts (Year, Month, Day, Week, Quarter, FiscalYear, FiscalPeriod)
var byYear = (from o in service.Queryable<Opportunity>()
              group o by o.ActualCloseDate.Value.Year into g
              select new { Year = g.Key, Count = g.Count() })
             .ToList();
```

### DateTime Operators

FetchXml date condition operators are available as extension methods:

```csharp
using XrmToolkit.Dataverse.Linq.Extensions;

.Where(a => a.CreatedOn.LastXDays(30))
.Where(a => a.CreatedOn.Between(startDate, endDate))
.Where(a => a.CreatedOn.ThisYear())
.Where(a => a.CreatedOn.OlderThanXMonths(6))
.Where(a => a.CreatedOn.OnOrAfter(cutoffDate))
.Where(a => a.CreatedOn.InFiscalYear(2025))
```

### User and Business Unit Operators

```csharp
using XrmToolkit.Dataverse.Linq.Extensions;

.Where(a => a.OwnerId.Id.EqualUserId())        // Current user's records
.Where(a => a.OwnerId.Id.EqualBusinessId())     // Current business unit
```

### Hierarchy Operators

```csharp
using XrmToolkit.Dataverse.Linq.Extensions;

.Where(a => a.AccountId.Under(parentId))
.Where(a => a.AccountId.AboveOrEqual(childId))
```

### Multi-Select Option Sets

```csharp
using XrmToolkit.Dataverse.Linq.Extensions;

.Where(c => c.PreferredContactMethods.ContainsValues(ContactMethod.Email, ContactMethod.Phone))
```

### Subquery Filtering (Any)

```csharp
// Contacts that are the primary contact of an account named "Contoso"
var results = service.Queryable<Contact>()
    .Where(c => service.Queryable<Account>().Any(
        a => a.PrimaryContactId.Id == c.ContactId && a.Name == "Contoso"))
    .ToList();
```

### Unbound Queries

Query without a typed proxy class using `GetAttributeValue`:

```csharp
var results = service.Queryable("account", "name", "revenue")
    .Where(e => !string.IsNullOrEmpty(e.GetAttributeValue<string>("name")))
    .Select(e => new { Name = e.GetAttributeValue<string>("name") })
    .ToList();
```

### Query Options

```csharp
service.Queryable<Account>()
    .WithPageSize(100)                                    // FetchXml count attribute
    .WithPage(3)                                          // FetchXml page attribute
    .WithAggregateLimit(50000)                            // Aggregate row limit
    .WithLateMaterialize()                                // Late materialization
    .WithDatasource(FetchDatasource.Retained)             // Long-term retained data
    .WithQueryHints(SqlQueryHint.ForceOrder)              // SQL query hints
    .WithUseRawOrderBy()                                  // Sort choice columns by value
    .ToList();
```

### Inspect Generated FetchXml

```csharp
var fetchXml = service.Queryable<Account>()
    .Where(a => a.Name != null)
    .OrderBy(a => a.Name)
    .ToFetchXml();

// Returns the FetchXml string without executing the query
```

### Async Operations (.NET 10+)

All query operations have async counterparts:

```csharp
await service.Queryable<Account>().ToListAsync();
await service.Queryable<Account>().FirstAsync(a => a.Name == "Contoso");
await service.Queryable<Account>().FirstOrDefaultAsync();
await service.Queryable<Account>().SingleAsync(a => a.AccountId == id);
await service.Queryable<Account>().CountAsync();
await service.Queryable<Account>().Select(a => a.Revenue).SumAsync();

// Page-by-page async processing
await service.Queryable<Account>()
    .WithPageSize(100)
    .ForEachPageAsync(async page =>
    {
        foreach (var account in page)
            await ProcessAccountAsync(account);
    });
```

## Requirements

| Target | SDK Package |
|--------|------------|
| .NET 10+ | `Microsoft.PowerPlatform.Dataverse.Client` |
| .NET Framework 4.6.2+ | `Microsoft.CrmSdk.CoreAssemblies` |

## License

This project is licensed under the MIT License.

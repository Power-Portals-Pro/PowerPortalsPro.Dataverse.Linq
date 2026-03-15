# XrmToolkit.Dataverse.Linq

A LINQ query provider for Microsoft Dataverse that translates LINQ expressions into FetchXml queries.

## Installation

```
dotnet add package XrmToolkit.Dataverse.Linq
```

## Usage

```csharp
using XrmToolkit.Dataverse.Linq;
using Microsoft.PowerPlatform.Dataverse.Client;

var client = new ServiceClient("your-connection-string");

// Query Dataverse entities using LINQ
var accounts = client.Query<Account>()
    .Where(a => a.Name.StartsWith("Contoso"))
    .OrderBy(a => a.Name)
    .ToList();
```

## Features

- Full LINQ support translated to FetchXml
- Filtering with `Where` clauses
- Sorting with `OrderBy` / `OrderByDescending`
- Projections with `Select`
- Joins via `Join` and navigation properties
- Paging with `Take` and `Skip`
- Aggregations (count, sum, avg, min, max)
- Hierarchy and multi-select attribute support
- Date/time filtering extensions

## Requirements

- .NET 10.0 or later
- Microsoft.PowerPlatform.Dataverse.Client 1.2.10 or later

## License

This project is licensed under the MIT License.

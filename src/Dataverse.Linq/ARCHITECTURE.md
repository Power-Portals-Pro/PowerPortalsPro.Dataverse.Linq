# Dataverse.Linq — Target Architecture

## Current State and Limitations

The current implementation uses a set of small, ad-hoc expression parsers
(`JoinExpressionParser`, `SelectExpressionParser`) that each handle one narrow
LINQ pattern and hard-code the FetchXml generation inline. This works for the
scenarios covered so far, but it cannot scale to a full query provider because:

- Every new LINQ operator (`Where`, `OrderBy`, `Take`, `Skip`, `GroupBy`, etc.)
  requires a new parser with its own FetchXml generation logic.
- The parsers are not composable — a `Where` applied after a `Join` would
  require a new combined parser rather than reusing existing ones.
- Transparent-identifier nesting (introduced by the C# compiler in multi-clause
  queries) has to be dealt with separately in each parser.
- There is no internal representation of the query; generation logic is
  scattered rather than centralised.

---

## Target Architecture

The goal is a proper, composable query provider modelled after the design used
by EF Core's InMemory and relational providers: a single **visitor** traverses
the entire LINQ expression tree and builds an **internal query model**, which is
then **serialised** to FetchXml by a dedicated builder.

```
LINQ expression tree
        │
        ▼
┌───────────────────────────┐
│  FetchXmlQueryTranslator  │   ExpressionVisitor subclass
│  (single entry point)     │   Handles all LINQ operators
└───────────┬───────────────┘
            │  produces
            ▼
┌───────────────────────────┐
│      FetchXmlQuery        │   Internal model (see below)
│  (entity + filters +      │
│   orders + links + ...)   │
└───────────┬───────────────┘
            │  serialised by
            ▼
┌───────────────────────────┐
│      FetchXmlBuilder      │   Pure model → XML transformer
│   Build(FetchXmlQuery)    │   No expression logic here
└───────────┬───────────────┘
            │
            ▼
        FetchXml string
            │
            ▼
┌───────────────────────────┐
│  DataverseQueryProvider   │   IAsyncQueryProvider
│  ExecuteAsync / Execute   │   Handles paging, ToEntity<T>,
│                           │   projection, async/sync paths
└───────────────────────────┘
```

---

## Internal Query Model

The model mirrors the FetchXml schema and is the single source of truth between
translation and serialisation.

```csharp
// Root query object — one per LINQ query
internal sealed class FetchXmlQuery
{
    public string              EntityLogicalName { get; set; }
    public List<FetchAttribute> Attributes       { get; } = [];
    public bool                AllAttributes     { get; set; } = true;
    public FetchFilter?        Filter            { get; set; }
    public List<FetchOrder>    Orders            { get; } = [];
    public List<FetchLinkEntity> Links           { get; } = [];
    public int?                Top               { get; set; }   // Take()
    public int?                Skip              { get; set; }
    public bool                Distinct          { get; set; }
    public bool                Aggregate         { get; set; }
    public Delegate?           Projector         { get; set; }   // post-query projection
    public Type?               ProjectionType    { get; set; }
}

// A single <attribute> or <attribute alias="..." aggregate="...">
internal sealed class FetchAttribute
{
    public string  Name      { get; set; }
    public string? Alias     { get; set; }
    public string? Aggregate { get; set; }   // count, sum, avg, min, max
    public bool    GroupBy   { get; set; }
}

// <filter type="and|or"> with nested conditions and sub-filters
internal sealed class FetchFilter
{
    public FilterType         Type       { get; set; } = FilterType.And;
    public List<FetchCondition> Conditions { get; } = [];
    public List<FetchFilter>  Filters    { get; } = [];
}

internal enum FilterType { And, Or }

// <condition attribute="..." operator="..." value="...">
internal sealed class FetchCondition
{
    public string  EntityAlias { get; set; }   // for linked-entity conditions
    public string  Attribute   { get; set; }
    public string  Operator    { get; set; }   // eq, ne, lt, le, gt, ge, null, not-null,
                                               // like, not-like, in, not-in, ...
    public object? Value       { get; set; }
    public List<object> Values { get; } = []; // for in / not-in
}

// <order attribute="..." descending="true|false">
internal sealed class FetchOrder
{
    public string Attribute  { get; set; }
    public bool   Descending { get; set; }
}

// <link-entity name="..." from="..." to="..." alias="..." link-type="inner|outer">
internal sealed class FetchLinkEntity
{
    public string              Name               { get; set; }
    public string              From               { get; set; }
    public string              To                 { get; set; }
    public string              Alias              { get; set; }
    public string              LinkType           { get; set; }  // "inner" | "outer"
    public List<FetchAttribute> Attributes        { get; } = [];
    public bool                AllAttributes      { get; set; }
    public FetchFilter?        Filter             { get; set; }
    public List<FetchLinkEntity> Links            { get; } = [];  // nested joins
}
```

---

## FetchXmlQueryTranslator

A single `ExpressionVisitor` subclass that is the heart of the provider. It
visits the LINQ expression tree top-down and populates a `FetchXmlQuery`.

### LINQ operators to handle

| LINQ operator              | FetchXml equivalent                                  |
|----------------------------|------------------------------------------------------|
| `Where(predicate)`         | `<filter>` / `<condition>`                          |
| `Select(projection)`       | `<attribute>` list; compiles projector delegate      |
| `OrderBy` / `OrderByDescending` | `<order descending="false|true">`              |
| `ThenBy` / `ThenByDescending`   | additional `<order>` elements                  |
| `Take(n)`                  | `<fetch top="n">`                                    |
| `Skip(n)`                  | paging (FetchXml page + paging-cookie)               |
| `Join`                     | `<link-entity link-type="inner">`                    |
| `GroupJoin + SelectMany`   | `<link-entity link-type="outer">`                    |
| `Distinct()`               | `<fetch distinct="true">`                            |
| `GroupBy` + aggregates     | `<fetch aggregate="true">` + `<attribute aggregate>` |
| `Count()` / `Any()`        | aggregate count condition                            |
| `First` / `Single`         | Top=1 + post-execution enforcement                   |

### Predicate translation

The visitor must translate arbitrary C# binary and unary expressions into
`FetchCondition` objects:

| C# expression                         | FetchXml operator |
|---------------------------------------|-------------------|
| `e.Field == value`                    | `eq`              |
| `e.Field != value`                    | `ne`              |
| `e.Field > value`                     | `gt`              |
| `e.Field >= value`                    | `ge`              |
| `e.Field < value`                     | `lt`              |
| `e.Field <= value`                    | `le`              |
| `e.Field == null`                     | `null`            |
| `e.Field != null`                     | `not-null`        |
| `list.Contains(e.Field)`              | `in`              |
| `!list.Contains(e.Field)`             | `not-in`          |
| `e.Field.Contains("x")`              | `like` (`%x%`)    |
| `e.Field.StartsWith("x")`            | `like` (`x%`)     |
| `e.Field.EndsWith("x")`              | `like` (`%x`)     |
| `predA && predB`                      | nested `<filter type="and">` |
| `predA \|\| predB`                    | nested `<filter type="or">`  |
| `!pred`                               | inverted operator or `<filter type="and">` with negated conditions |

### Transparent identifier handling

The C# compiler introduces anonymous transparent-identifier types when combining
multiple `from`, `join`, or `let` clauses. The visitor must:

1. Detect compiler-generated types (`Name.StartsWith("<>")`).
2. Recursively descend to find the real entity property path.
3. Rewrite member accesses against those paths when building projector delegates
   or extracting attribute names.

The existing `FindOuterPropertyPath` / `IsOuterEntityAccess` helpers in
`JoinExpressionParser` are the prototype for this logic and should be
centralised in the translator.

---

## FetchXmlBuilder

Becomes a pure model-to-XML transformer with no expression logic. Takes a
`FetchXmlQuery` and emits the FetchXml string.

```csharp
internal static class FetchXmlBuilder
{
    internal static string Build(FetchXmlQuery query) { ... }

    private static XElement BuildEntity(FetchXmlQuery query) { ... }
    private static XElement BuildLinkEntity(FetchLinkEntity link) { ... }
    private static XElement BuildFilter(FetchFilter filter) { ... }
    private static XElement BuildCondition(FetchCondition condition) { ... }
    private static XElement BuildOrder(FetchOrder order) { ... }
    private static XElement BuildAttribute(FetchAttribute attr) { ... }
}
```

---

## DataverseQueryProvider

Stays as the `IAsyncQueryProvider` entry point. Its `Execute<TResult>` and
`ExecuteAsync<TResult>` methods simply:

1. Call `FetchXmlQueryTranslator.Translate(expression)` to get a `FetchXmlQuery`.
2. Call `FetchXmlBuilder.Build(query)` to get the FetchXml string.
3. Execute paged retrieval via `RetrieveAll` / `RetrieveAllAsync`.
4. Apply `ToEntity<T>()` and the compiled projector delegate.

```csharp
internal class DataverseQueryProvider<T> : IAsyncQueryProvider where T : Entity
{
    public TResult Execute<TResult>(Expression expression)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression);
        var fetchXml = FetchXmlBuilder.Build(query);
        var entities = RetrieveAll(fetchXml);
        return ApplyProjection<TResult>(entities, query);
    }

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
    {
        var query = FetchXmlQueryTranslator.Translate<T>(expression);
        return (TResult)(object)FetchAndProjectAsync<...>(query, ct);
    }
}
```

---

## Migration Path

To reach the target without breaking existing tests, the migration can proceed
incrementally:

1. **Define the internal model** (`FetchXmlQuery`, `FetchFilter`, etc.) without
   removing any existing code.
2. **Implement `FetchXmlQueryTranslator`** for the operators already working
   today: `Select` (projection), `Join` (inner + outer), `Where` (inner-is-null
   filter).
3. **Switch `FetchXmlBuilder`** to accept `FetchXmlQuery` and delete the current
   `Build(entityName, columns)` / `BuildJoin(JoinInfo)` overloads.
4. **Extend the translator** operator by operator: `Where` (general predicates),
   `OrderBy`, `Take`, `Skip`, `Distinct`, `GroupBy` + aggregates.
5. **Delete** the now-redundant `JoinExpressionParser`, `SelectExpressionParser`,
   and `JoinInfo`.

Each step should keep all integration tests passing.

---

## Reference

- **EF Core InMemory provider** (`dotnet/efcore` →
  `src/EFCore.InMemory`) — closest analogue; uses
  `InMemoryQueryableMethodTranslatingExpressionVisitor` to translate LINQ
  methods to in-memory operations and `ShapedQueryCompilingExpressionVisitor`
  to compile the result shaper.
- **EF Core relational provider** (`dotnet/efcore` →
  `src/EFCore.Relational`) — uses `RelationalQueryableMethodTranslatingExpressionVisitor`
  and `SelectExpression` as the internal SQL model; the `SelectExpression` →
  `FetchXmlQuery` analogy is direct.
- **FetchXml reference** — Microsoft docs:
  https://learn.microsoft.com/en-us/power-apps/developer/data-platform/fetchxml/overview

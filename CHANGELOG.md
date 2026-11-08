# Changelog

All notable changes to PowerPortalsPro.Dataverse.Linq will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

- Fixed a select of a single value — `x.GetAttributeValue<T>("col")`, the indexer `x["col"]` or `x.Id` as the whole projection (`Select(a => a.GetAttributeValue<string>("name"))`) — reading every column (`<all-attributes />`). With `Distinct()` that returned duplicates: the fetch was distinct over every column, the primary key included, so each row was distinct. It now reads only that column, `Id` as the table's primary key (`accountid`, or `activityid` for an activity), so `Distinct()` de-duplicates on the server. The same applies to `First()`, `Single()` and the rest over such a select, and a no-selector aggregate over one (`.Select(a => a.GetAttributeValue<int?>("numberofemployees")).Max()`) now translates instead of throwing.
- Fixed a select dropping columns its projection reads in a method call, operator or conditional (`new { a.Name, Website = a.Website.ToUpper() }`, `a.NumberOfEmployees + 1`, `a.IsPreferred ?? false ? … : …`): only plain property reads were requested, so the others came back null and threw or gave wrong values. Every column the projection reads is now requested. A select that also uses the entity whole (`new { a.Name, Account = a }`) reads every column, so the entity it returns is complete.
- Fixed the same in a select after a join or left join, which collected its columns separately:
  - Columns read in a method call or member chain (`a.Website.Length`) were not requested on a left join, and an operator (`a.NumberOfEmployees + 1`, `c.FirstName + " " + c.LastName`) threw `NotSupportedException`. On an inner join the columns were requested, but the select threw while building its results.
  - An entity used whole beside one of its columns (`select new { a.Name, Account = a }`) came back with only that column.
  - A single value as the whole projection (`select a.GetAttributeValue<string>("name")`, `select a.Id`) read every column of the root.
  - An entity the select doesn't read now gets no columns. A select of only joined columns used to read every column of the root, so `(from a … join c … select c.LastName).Distinct()` returned a row per account and contact rather than each last name once.
  - A left join's `c == null` / `c != null` in the projection now reads the joined row's primary key. `select new { HasContact = c != null }` threw, and `c == null ? … : c.Name` took a matched contact whose name was null for no match.
- Fixed a select on an earlier select's results. A select over the values another made (`.Select(a => a.Name).Take(5).Select(n => n.ToUpper())`) threw while reading the results, and one whose parameter is an entity re-read the row, dropping what the first select computed (`.Select(a => new Account { Name = a.Name.ToUpper() }).Take(5).Select(x => x.Name)` returned the stored names). Two selects in a row are now combined into one, which requests only the columns the second reads (`.Select(a => new { a.Name, a.Website }).Select(x => x.Name)` reads only `name`). Otherwise, and whenever the first select made something other than the query's own entities, the second runs on the first's results, as LINQ runs it. The same holds after a join or `GroupBy`. A select that keeps the query's entities (`Select(a => a)`, a join's `select c`) is still read against the row.
  - After a select that runs on results, `Where`, `OrderBy`, `GroupBy`, `Distinct`, `Join`, a predicate on `First` and the rest, and any aggregate (`Count`, `Sum`, …) throw `NotSupportedException`: Dataverse doesn't have the values they would test or aggregate.
- Fixed an aggregate without a selector (`.Select(a => a.NumberOfEmployees + 1).Sum()`) aggregating the column and dropping what the select computed. It now aggregates the column only when the select's value is that column's unchanged — a property, `GetAttributeValue`, the indexer, `Id`, or a cast that only lifts to `Nullable`, boxes, unboxes or switches an enum and its number. A computed value or a numeric cast (`(long?)a.NumberOfEmployees`, whose sum wouldn't overflow where the column's can) throws `NotSupportedException`, as do `Sum()` and `Average()` after `Distinct()`, which Dataverse would compute over every row. A joined column's aggregate throws too, rather than aggregating a root column.
- Changed the indexer, `x["col"]`, in a select to return null for a column the row doesn't have, as `GetAttributeValue` does, rather than throwing `KeyNotFoundException`. A select requests only the columns it reads, and Dataverse leaves a null column out of the row.
- Changed `OnAfterMaterialize` placed before a later `Select` that produces another type, or before one that runs on results, to throw `NotSupportedException`. The hook used to be skipped without a word, or run after that `Select` rather than before it; place it after the last `Select`.
- The `Select` overload that passes each element's index now throws `NotSupportedException` when the query is translated, rather than failing while reading results.
- Fixed `Count()` and `LongCount()` (and `CountAsync()`) of a select's distinct values (`Select(c => c.LastName).Distinct().Count()`) counting every row rather than the distinct values. They now count the distinct rows Dataverse returns, so a value null in some rows counts once, as in LINQ. A predicate on such a count throws `NotSupportedException`; filter with `Where` before the `Select`.
- Changed `Distinct()` after a select that computes its values from columns it doesn't also return as they are (`Select(a => a.Name.Substring(0, 1)).Distinct()`, `select c.FirstName + " " + c.LastName`, a left join's `HasContact = c != null`) to throw `NotSupportedException`. Dataverse removes duplicate columns, not duplicate computed values, so such a query returned duplicates. A computed value beside its column (`new { a.Name, Initial = a.Name.Substring(0, 1) }`) is unaffected. Take `Distinct()` of the columns, then compute the values after `AsEnumerable()`. `Distinct()` over a select that reads no column (`Select(a => 1)`), or after `Take()` (Dataverse removes duplicates before it takes the first rows), throws too.
- Fixed a select after `Distinct()` (`Distinct().Select(a => a.Name)`) de-duplicating the narrower columns it reads, merging accounts that share a name. It now keeps each entity's row with its primary key, as LINQ keeps every distinct entity. A join after `Distinct()`, or a select after `Distinct()` on a join, throws `NotSupportedException`; apply `Distinct()` after it.

## [1.0.20] - 2026-10-28

- Fixed a joined activity — an appointment, email, task or any other, selected whole from a join (`select new { a, Task = t }`) — coming back with an empty `Id`. A joined entity's `Id` is now read from its primary key as its metadata names it (`activityid` for every activity) rather than `{logicalname}id`.

## [1.0.19] - 2026-10-28

- Fixed `Count()` / `LongCount()` (and their async forms) failing on activity tables — appointments, emails, tasks and the rest — with "entity doesn't contain attribute with Name = 'appointmentid'". The count is now taken over the table's primary key as its metadata names it (`activityid` for every activity) rather than `{logicalname}id`. The same applies to grouped counts, `CountChildren()`, and a `where x == null` test on a left join's unmatched rows.

## [1.0.18] - 2026-10-04

- Fixed `Entity.Id` on a late-bound (`Queryable("logicalname")`) entity in joins that mix typed and late-bound sources. The base `Entity` type carries no logical name, so the primary key could not be resolved once the entity was reached through a join; each joined entity's logical name is now tracked and used instead. This fixes:
  - Chained joins keyed on a late-bound `Id` (e.g. `on c.Id equals o.Contact.Id`, or `equals a.Id` as the inner key), which threw `NotSupportedException` ("Could not resolve outer join key through transparent identifier"). Filters such as `where c.Id == id` on a late-bound joined entity are also resolved this way.
  - Projecting a late-bound joined entity's `Id` (e.g. `select new { ContactId = c.Id }`) in an inner join, which threw `ArgumentException` during materialization.
  - Projecting it after a left join, which threw `NullReferenceException` for unmatched rows, or — when a `where` clause came between the join and the `select` — silently returned `Guid.Empty` for every row because the primary key column was never requested. Unmatched rows now return `Guid.Empty`, matching other columns of a missing left-join match.

## [1.0.17] - 2026-07-29

- Fixed `NotSupportedException` for `Contains` over an array whose element type does not implement `IEquatable<T>` — an array of option set enums, most commonly. C# binds `array.Contains(value)` to the span-based `MemoryExtensions.Contains`, which takes a trailing `IEqualityComparer<T>` argument in that case; the extra argument is now recognised and still translates to an `in` (or `not-in`) condition. A non-null comparer has no FetchXml equivalent and remains unsupported.
- Fixed `Contains` over a collection of option set enums emitting the enum member name (`<value>Hot</value>`) instead of the underlying option set value (`<value>100000002</value>`), which silently matched nothing. Enum elements of any collection type — `List<T>`, `HashSet<T>`, array — are now serialized as their underlying integer.
- Fixed `CancellationToken` being observed only between pages on async queries. The token is now passed through to the service's cancellable `RetrieveMultipleAsync` overload (used when the service implements `IOrganizationServiceAsync2`, as `ServiceClient` does), so an in-flight request is cancelled rather than running to completion, and it is checked before each page request is sent.

## [1.0.16] - 2026-07-29

- Added support for non-constant attribute names in `GetAttributeValue<T>(name)`. The name argument no longer has to be a string literal: any expression that can be evaluated while the query is translated — a local variable, field, property, method call, indexer, concatenation, or ternary — is resolved to the attribute name in filters, projections, ordering, grouping, and join keys. Names that depend on the row being queried remain unsupported.

## [1.0.14] - 2026-06-25

- Added support for distinct column counts in grouped aggregates: `g.Select(x => x.Lookup).Distinct().Count()` (and the `g.Select(x => x.Lookup.Id).Distinct().Count()` variant) now translate to FetchXml `aggregate="countcolumn" distinct="true"` over the selected attribute, instead of being treated as a plain row count over the primary key.

## [1.0.13] - 2026-06-22

- Fixed 'Entity.Id' not resolving in queries over unbound entities (e.g. `Queryable("logicalname").Where(x => x.Id == id)`). The primary key attribute is now resolved from entity metadata using the query's known logical name when the parameter is the base 'Entity' type, so predicates, ordering, and projections referencing 'x.Id' work for unbound queries as they already do for typed ones.

## [1.0.12] - 2026-05-28

- Added 'OnBeforeMaterialize' and 'OnAfterMaterialize' transform hooks, available both inline per query and globally via 'DataverseQueryDiagnostics.BeforeMaterialize'/'AfterMaterialize'. OnBeforeMaterialize can adjust or replace the raw row before projection; OnAfterMaterialize can enrich or replace the materialized result using the source row. The global hook runs first and the per-query hook runs after it, so the per-query hook takes precedence.
- Fixed grouped aggregates (Count, Max, etc.) being computed on the wrong entity in some joined GroupBy queries: a GroupBy composed over an already-joined query, and a GroupBy across chained joins, now place their aggregate attributes on the grouped element's entity instead of defaulting to the root entity.

## [1.0.11] - 2026-05-27

- Added the 'CaptureFetchXml' extension method to capture the FetchXml of every request as a query executes, including once per page for multi-page queries.
- Added a 'ToFetchXml' overload that returns the FetchXml for aggregate and element operators (Count, Sum, Min, Max, Average, CountColumn, First, Single, etc.) without executing the query.
- Added the global 'DataverseQueryDiagnostics.FetchXmlRequested' hook, raised with the FetchXml of every request for process-wide logging or telemetry.
- Fixed invalid aggregate FetchXml when a GroupBy or aggregate was composed on top of a query that projected a whole linked entity (the link emitted all-attributes instead of the groupby/aggregate attributes).
- Improved query composition support: a re-projecting Select now narrows previously projected link columns, and projecting a whole linked entity now retrieves its columns.
- Fixed projected columns (especially lookups/EntityReferences) coming back null from a WithFirstRow (matchfirstrowusingcrossapply) join, which returns its columns keyed by schema name rather than the alias-prefixed name the materializer expected.
- Fixed an OrderBy on a join's inner source being dropped; it is now emitted as a root-level order qualified by the link alias (the only placement cross-apply accepts), so WithFirstRow keeps the row defined by that ordering.

## [1.0.10] - 2026-05-07

- Fix NotSupportedException when projection uses GetAttributeValue inside a constructor.

## [1.0.9] - 2026-03-27

- Added logic to guard against unsupported expressions.
- Fix for columns not being picked up for the resulting select statement.

## [1.0.8] - 2026-03-24

- Fixed an issue where it was not correctly determining the column logical name.

## [1.0.7] - 2026-03-24

- Fixed an issue where columns were not being selected properly.

## [1.0.6] - 2026-03-24

- Added support for multiple chained left joins.

## [1.0.5] - 2026-03-18

- Added the 'ReturnRecordCount' and 'ReturnRecordCountAsync' extension methods.
- Updates to publish action and readme.

## [1.0.4] - 2026-03-18

- Refactor selection materializer logic to support more scenarios.
- Improved support for nullable properties on the select result when using grouping.
- Updated the release flow to auto generate a bulleted list of items based on the descriptions of the commits.
- Added support for selecting into a class when grouping.
- Added support for aggregate ordering when the column is on a linked entity.
- Added support for ordering by an aggregate when grouping is used.
- Fixed an issue with unbound queryies.
- Fixed an issue where the Queryable<> extensions were not available for .Net Framework 4.6.2

## [1.0.3] - 2026-03-17

### Added the ability to return an EntityReference in the result when grouping



## [1.0.2] - 2026-03-17

### Added support for composite key grouping.



## [1.0.1] - 2026-03-17

### Added a Github action to auto publish and update the CHANGELOG file.



- Initial release.

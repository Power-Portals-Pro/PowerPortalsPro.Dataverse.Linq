# Changelog

All notable changes to PowerPortalsPro.Dataverse.Linq will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

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

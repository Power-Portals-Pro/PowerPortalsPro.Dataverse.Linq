# Changelog

All notable changes to PowerPortalsPro.Dataverse.Linq will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

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

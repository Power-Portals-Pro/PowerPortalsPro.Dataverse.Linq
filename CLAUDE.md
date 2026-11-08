# CLAUDE.md

## Testing

- When adding or modifying a feature to the PowerPortalsPro.Dataverse.Linq query provider, always create unit tests (in the `FetchXml/` folder) for what it changes in translation: the FetchXml it generates, and the exceptions it throws.
- When a change affects the query sent to Dataverse or the results it returns, also create integration tests (in the `Integration/` folder) that run it against Dataverse. That includes each such part of a change, such as a fixed edge case, a new FetchXml shape or a materialized result: only Dataverse shows whether it accepts the query and returns the right rows.
- A change that only throws needs a unit test, not an integration test, whenever a unit test can reach the throw: any exception raised while the query is translated, before a request is sent, or one a stubbed service response triggers. If only a live request can reach it, an integration test is required.

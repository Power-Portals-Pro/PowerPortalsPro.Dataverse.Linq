using FluentAssertions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Tests for the OnBeforeMaterialize / OnAfterMaterialize transform hooks — both the
/// per-query (inline) form and the global <see cref="DataverseQueryDiagnostics"/> form.
/// </summary>
public class MaterializeHookTests : FetchXmlTestBase
{
    private void SetupRows(params Entity[] rows)
    {
        var collection = new EntityCollection { MoreRecords = false };
        collection.Entities.AddRange(rows);
        _service.RetrieveMultiple(Arg.Any<QueryBase>()).Returns(collection);
    }

    private static Entity Contact(string first, string last)
    {
        var e = new Entity("new_customcontact", Guid.NewGuid());
        e["new_firstname"] = first;
        e["new_lastname"] = last;
        return e;
    }

    // -------------------------------------------------------------------------
    // Inline OnBeforeMaterialize — transforms the raw row before projection
    // -------------------------------------------------------------------------

    [Fact]
    public void OnBeforeMaterialize_PatchesRawEntity_BeforeProjection()
    {
        var row = new Entity("new_customaccount", Guid.NewGuid());
        row["new_name"] = "raw";
        SetupRows(row);

        var results = _service.Queryable<CustomAccount>()
            .OnBeforeMaterialize(e => { e["new_name"] = "patched"; return e; })
            .Select(a => new { a.Name })
            .ToList();

        results.Should().ContainSingle();
        results[0].Name.Should().Be("patched");
    }

    // -------------------------------------------------------------------------
    // Inline OnAfterMaterialize — mutates / replaces the materialized result
    // -------------------------------------------------------------------------

    [Fact]
    public void OnAfterMaterialize_MutatesWholeEntityResult()
    {
        SetupRows(Contact("Ada", "Lovelace"));

        var results = _service.Queryable<CustomContact>()
            .OnAfterMaterialize((source, c) => { c.FirstName += "!"; return c; })
            .ToList();

        results.Should().ContainSingle();
        results[0].FirstName.Should().Be("Ada!");
    }

    [Fact]
    public void OnAfterMaterialize_CanReplaceResultUsingSourceEntity()
    {
        SetupRows(Contact("Ada", "Lovelace"));

        var results = _service.Queryable<CustomContact>()
            .Select(c => new { Value = c.FirstName })
            // Replace the projected object, reading from the source entity.
            .OnAfterMaterialize((source, _) => new { Value = source.GetAttributeValue<string>("new_lastname") })
            .ToList();

        results.Should().ContainSingle();
        results[0].Value.Should().Be("Lovelace");
    }

    [Fact]
    public void OnAfterMaterialize_ReturningNull_LeavesResultUnchanged()
    {
        SetupRows(Contact("Ada", "Lovelace"));

        var results = _service.Queryable<CustomContact>()
            .OnAfterMaterialize((source, c) => null)
            .ToList();

        results.Should().ContainSingle();
        results[0].FirstName.Should().Be("Ada");
    }

    // -------------------------------------------------------------------------
    // Global + inline ordering — global runs first, the query-level hook runs last
    // (and therefore takes precedence) in both phases.
    // -------------------------------------------------------------------------

    // The global hooks below are marker-scoped: they only act on rows carrying this test's
    // marker attribute. Global diagnostics are process-wide statics and xUnit runs test
    // classes in parallel, so an unscoped global hook could corrupt (or record from) a query
    // running concurrently in another class.

    [Fact]
    public void QueryHooks_RunAfterGlobalHooks_InBothPhases()
    {
        const string marker = "hook_order_marker";
        var row = Contact("Ada", "Lovelace");
        row[marker] = true;
        SetupRows(row);

        var order = new List<string>();
        DataverseQueryDiagnostics.BeforeMaterialize = e => { if (e.Contains(marker)) order.Add("global-before"); return e; };
        DataverseQueryDiagnostics.AfterMaterialize = (e, o) => { if (e.Contains(marker)) order.Add("global-after"); return o; };
        try
        {
            _ = _service.Queryable<CustomContact>()
                .OnBeforeMaterialize(e => { order.Add("query-before"); return e; })
                .OnAfterMaterialize((s, c) => { order.Add("query-after"); return c; })
                .ToList();
        }
        finally
        {
            DataverseQueryDiagnostics.BeforeMaterialize = null;
            DataverseQueryDiagnostics.AfterMaterialize = null;
        }

        order.Should().Equal("global-before", "query-before", "global-after", "query-after");
    }

    [Fact]
    public void QueryAfterMaterialize_TakesPrecedenceOverGlobal()
    {
        const string marker = "precedence_marker";
        var row = Contact("Ada", "Lovelace");
        row[marker] = true;
        SetupRows(row);

        DataverseQueryDiagnostics.AfterMaterialize = (src, o) =>
            src.Contains(marker) && o is CustomContact c
                ? new CustomContact { FirstName = "global", LastName = c.LastName }
                : o;
        try
        {
            var results = _service.Queryable<CustomContact>()
                .OnAfterMaterialize((_, c) => { c.FirstName = "query-wins"; return c; })
                .ToList();

            // Global runs first (sets "global"), the query hook runs last and overrides it.
            results.Should().ContainSingle();
            results[0].FirstName.Should().Be("query-wins");
        }
        finally
        {
            DataverseQueryDiagnostics.AfterMaterialize = null;
        }
    }

    [Fact]
    public void GlobalAfterMaterialize_CanTransformResult()
    {
        // Marker attribute keeps this global transform scoped to this test's rows, so it
        // can't disturb any query running in parallel.
        const string marker = "materialize_hook_marker";
        var row = Contact("Ada", "Lovelace");
        row[marker] = true;
        SetupRows(row);

        DataverseQueryDiagnostics.AfterMaterialize = (source, result) =>
            source.Contains(marker) && result is CustomContact c
                ? new CustomContact { FirstName = "redacted", LastName = c.LastName }
                : result;
        try
        {
            var results = _service.Queryable<CustomContact>().ToList();

            results.Should().ContainSingle();
            results[0].FirstName.Should().Be("redacted");
            results[0].LastName.Should().Be("Lovelace");
        }
        finally
        {
            DataverseQueryDiagnostics.AfterMaterialize = null;
        }
    }
}

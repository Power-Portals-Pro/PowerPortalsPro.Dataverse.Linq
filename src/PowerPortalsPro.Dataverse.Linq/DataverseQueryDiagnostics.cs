using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Process-wide diagnostic hooks for the Dataverse LINQ provider. Useful for blanket
/// logging or telemetry of the FetchXml the provider sends, without instrumenting each
/// query individually. For per-query capture, prefer <c>CaptureFetchXml(...)</c>.
/// </summary>
public static class DataverseQueryDiagnostics
{
    /// <summary>
    /// Raised with the exact FetchXml of each request immediately before it is sent to
    /// Dataverse, for every query executed by the provider in this process. Fires once
    /// per page for multi-page queries, with the page number and paging-cookie embedded
    /// in the FetchXml.
    /// </summary>
    /// <remarks>
    /// Handlers run inline on the thread issuing the request, so keep them fast and
    /// exception-free. This is a global hook intended for diagnostics; use
    /// <c>CaptureFetchXml(...)</c> when you need to scope capture to a single query.
    /// </remarks>
    public static event Action<string>? FetchXmlRequested;

    internal static void RaiseFetchXmlRequested(string fetchXml) =>
        FetchXmlRequested?.Invoke(fetchXml);

    /// <summary>
    /// Returns <c>true</c> when at least one handler is subscribed to
    /// <see cref="FetchXmlRequested"/>. The provider uses this to skip building the
    /// notification string when nothing is listening.
    /// </summary>
    internal static bool HasFetchXmlSubscribers => FetchXmlRequested is not null;

    /// <summary>
    /// Process-wide transform applied to every raw <see cref="Entity"/> row before it is
    /// materialized, for every query. Returns the entity to materialize (mutated or
    /// replaced), or <c>null</c> to leave the row unchanged.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="FetchXmlRequested"/> (an observe-only multicast event), this is a
    /// transform pipeline stage, so it is a single delegate — assign a composed delegate
    /// if you need to run several. It runs <i>before</i> any per-query
    /// <c>OnBeforeMaterialize</c>, so the per-query hook runs last and takes precedence.
    /// Handlers run inline on the materializing thread; keep them fast and exception-free.
    /// </remarks>
    public static Func<Entity, Entity?>? BeforeMaterialize { get; set; }

    /// <summary>
    /// Process-wide transform applied to every materialized result, for every query,
    /// receiving the source <see cref="Entity"/> and the materialized object. Returns a
    /// replacement object, or <c>null</c> to leave the result unchanged. It runs
    /// <i>before</i> any per-query <c>OnAfterMaterialize</c>, so the per-query hook runs
    /// last and takes precedence. Single delegate; see <see cref="BeforeMaterialize"/>
    /// remarks.
    /// </summary>
    public static Func<Entity, object?, object?>? AfterMaterialize { get; set; }

    internal static Entity? RaiseBeforeMaterialize(Entity entity) =>
        BeforeMaterialize?.Invoke(entity);

    internal static object? RaiseAfterMaterialize(Entity source, object? result) =>
        AfterMaterialize?.Invoke(source, result);
}

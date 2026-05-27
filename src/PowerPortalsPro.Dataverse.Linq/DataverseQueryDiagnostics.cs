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
}

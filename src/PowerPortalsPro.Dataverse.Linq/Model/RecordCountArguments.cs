namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Contains the total record count information returned by Dataverse
/// when <c>returntotalrecordcount="true"</c> is set on the FetchXml query.
/// </summary>
public record RecordCountArguments(int TotalRecordCount, bool TotalRecordCountLimitExceeded);

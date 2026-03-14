namespace Dataverse.Linq.Model;

public enum SqlQueryHint
{
    [FetchXmlValue("ForceOrder")]
    ForceOrder,

    [FetchXmlValue("DisableRowGoal")]
    DisableRowGoal,

    [FetchXmlValue("EnableOptimizerHotfixes")]
    EnableOptimizerHotfixes,

    [FetchXmlValue("LoopJoin")]
    LoopJoin,

    [FetchXmlValue("MergeJoin")]
    MergeJoin,

    [FetchXmlValue("HashJoin")]
    HashJoin,

    [FetchXmlValue("NO_PERFORMANCE_SPOOL")]
    NoPerformanceSpool,

    [FetchXmlValue("ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS")]
    EnableHistAmendmentForAscKeys,
}

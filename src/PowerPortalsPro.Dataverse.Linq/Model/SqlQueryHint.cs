namespace PowerPortalsPro.Dataverse.Linq.Model;

/// <summary>
/// SQL query hints that can be applied to FetchXml queries via the <c>options</c> attribute.
/// </summary>
public enum SqlQueryHint
{
    /// <summary>Forces the query optimizer to use the join order specified in the query.</summary>
    [FetchXmlValue("ForceOrder")]
    ForceOrder,

    /// <summary>Disables the row goal optimization, causing the optimizer to plan for the full result set.</summary>
    [FetchXmlValue("DisableRowGoal")]
    DisableRowGoal,

    /// <summary>Enables query optimizer hotfixes released after the current compatibility level.</summary>
    [FetchXmlValue("EnableOptimizerHotfixes")]
    EnableOptimizerHotfixes,

    /// <summary>Forces the use of nested loop joins.</summary>
    [FetchXmlValue("LoopJoin")]
    LoopJoin,

    /// <summary>Forces the use of merge joins.</summary>
    [FetchXmlValue("MergeJoin")]
    MergeJoin,

    /// <summary>Forces the use of hash joins.</summary>
    [FetchXmlValue("HashJoin")]
    HashJoin,

    /// <summary>Prevents the query optimizer from adding a performance spool operator to the plan.</summary>
    [FetchXmlValue("NO_PERFORMANCE_SPOOL")]
    NoPerformanceSpool,

    /// <summary>Enables histogram amendment for ascending key columns.</summary>
    [FetchXmlValue("ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS")]
    EnableHistAmendmentForAscKeys,
}

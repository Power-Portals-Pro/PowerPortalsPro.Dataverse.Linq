using PowerPortalsPro.Dataverse.Linq.Model;
using System.Xml.Linq;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Pure model-to-XML transformer. Converts a <see cref="FetchXmlQuery"/> into
/// a FetchXml string with no expression logic.
/// </summary>
internal static class FetchXmlBuilder
{
    // -------------------------------------------------------------------------
    // New model-based entry point
    // -------------------------------------------------------------------------

    internal static string Build(FetchXmlQuery query)
    {
        var fetchElement = new XElement("fetch", new XAttribute("mapping", "logical"));

        if (query.Top.HasValue)
            fetchElement.Add(new XAttribute("top", query.Top.Value));
        if (query.PageSize.HasValue)
            fetchElement.Add(new XAttribute("count", query.PageSize.Value));
        if (query.Page.HasValue)
            fetchElement.Add(new XAttribute("page", query.Page.Value));
        if (query.Distinct)
            fetchElement.Add(new XAttribute("distinct", "true"));
        if (query.Aggregate)
            fetchElement.Add(new XAttribute("aggregate", "true"));
        if (query.AggregateLimit.HasValue)
            fetchElement.Add(new XAttribute("aggregatelimit", query.AggregateLimit.Value));
        if (query.Datasource.HasValue)
            fetchElement.Add(new XAttribute("datasource", query.Datasource.Value.ToFetchXmlString()));
        if (query.LateMaterialize)
            fetchElement.Add(new XAttribute("latematerialize", "true"));
        if (query.NoLock)
            fetchElement.Add(new XAttribute("no-lock", "true"));
        if (query.QueryHints is { Count: > 0 })
            fetchElement.Add(new XAttribute("options",
                string.Join(",", query.QueryHints.Select(h => h.ToFetchXmlString()))));
        if (query.UseRawOrderBy)
            fetchElement.Add(new XAttribute("useraworderby", "true"));
        if (query.ReturnTotalRecordCount)
            fetchElement.Add(new XAttribute("returntotalrecordcount", "true"));

        fetchElement.Add(BuildEntity(query));
        return fetchElement.ToString();
    }

    // -------------------------------------------------------------------------
    // Entity
    // -------------------------------------------------------------------------

    private static XElement BuildEntity(FetchXmlQuery query)
    {
        var element = new XElement("entity", new XAttribute("name", query.EntityLogicalName));
        AddAttributes(element, query.Attributes, query.AllAttributes);

        foreach (var order in query.Orders)
            element.Add(BuildOrder(order));

        if (query.Filter is not null && !IsEmptyFilter(query.Filter))
            element.Add(BuildFilter(query.Filter));

        foreach (var link in query.Links)
            element.Add(BuildLinkEntity(link));

        return element;
    }

    // -------------------------------------------------------------------------
    // Link entity
    // -------------------------------------------------------------------------

    private static XElement BuildLinkEntity(FetchLinkEntity link)
    {
        var element = new XElement("link-entity",
            new XAttribute("name", link.Name),
            new XAttribute("from", link.From),
            new XAttribute("to", link.To),
            new XAttribute("alias", link.Alias),
            new XAttribute("link-type", link.LinkType));

        AddAttributes(element, link.Attributes, link.AllAttributes);

        foreach (var order in link.Orders)
            element.Add(BuildOrder(order));

        if (link.Filter is not null)
            element.Add(BuildFilter(link.Filter));

        foreach (var nested in link.Links)
            element.Add(BuildLinkEntity(nested));

        return element;
    }

    // -------------------------------------------------------------------------
    // Filter / Condition
    // -------------------------------------------------------------------------

    private static XElement BuildFilter(FetchFilter filter)
    {
        var element = new XElement("filter",
            new XAttribute("type", filter.Type == FilterType.Or ? "or" : "and"));

        foreach (var link in filter.Links)
            element.Add(BuildLinkEntity(link));

        foreach (var condition in filter.Conditions)
            element.Add(BuildCondition(condition));

        foreach (var nested in filter.Filters)
            element.Add(BuildFilter(nested));

        return element;
    }

    private static XElement BuildCondition(FetchCondition condition)
    {
        var element = new XElement("condition");

        if (condition.EntityAlias is not null)
            element.Add(new XAttribute("entityname", condition.EntityAlias));

        element.Add(new XAttribute("attribute", condition.Attribute));
        element.Add(new XAttribute("operator", condition.Operator.ToFetchXml()));

        if (condition.ValueOf is not null)
            element.Add(new XAttribute("valueof", condition.ValueOf));
        else if (condition.Value is not null)
            element.Add(new XAttribute("value", condition.Value));

        foreach (var val in condition.Values)
            element.Add(new XElement("value", val));

        return element;
    }

    // -------------------------------------------------------------------------
    // Order / Attribute
    // -------------------------------------------------------------------------

    private static XElement BuildOrder(FetchOrder order)
    {
        var element = new XElement("order");

        if (order.Alias is not null)
            element.Add(new XAttribute("alias", order.Alias));
        else
            element.Add(new XAttribute("attribute", order.Attribute));

        element.Add(new XAttribute("descending", order.Descending ? "true" : "false"));

        if (order.EntityAlias is not null)
            element.Add(new XAttribute("entityname", order.EntityAlias));

        return element;
    }

    private static XElement BuildAttribute(FetchAttribute attr)
    {
        var element = new XElement("attribute", new XAttribute("name", attr.Name));

        if (attr.Alias is not null)
            element.Add(new XAttribute("alias", attr.Alias));
        if (attr.Aggregate is not null)
            element.Add(new XAttribute("aggregate", attr.Aggregate));
        if (attr.GroupBy)
            element.Add(new XAttribute("groupby", "true"));
        if (attr.DateGrouping is not null)
            element.Add(new XAttribute("dategrouping", attr.DateGrouping));
        if (attr.RowAggregate is not null)
            element.Add(new XAttribute("rowaggregate", attr.RowAggregate));

        return element;
    }

    private static bool IsEmptyFilter(FetchFilter filter) =>
        filter.Conditions.Count == 0 && filter.Filters.Count == 0 && filter.Links.Count == 0;

    private static void AddAttributes(XElement element, List<FetchAttribute> attributes, bool allAttributes)
    {
        if (allAttributes)
            element.Add(new XElement("all-attributes"));
        else
            foreach (var attr in attributes)
                element.Add(BuildAttribute(attr));
    }
}

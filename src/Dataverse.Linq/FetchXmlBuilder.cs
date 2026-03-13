using Dataverse.Linq.Expressions;
using System.Xml.Linq;

namespace Dataverse.Linq;

internal static class FetchXmlBuilder
{
    internal static string Build(string entityName, IReadOnlyList<string>? columns = null) =>
        BuildFetchXml(entityName, columns, link: null, nullFilter: null);

    internal static string BuildJoin(JoinInfo join) =>
        BuildFetchXml(
            join.OuterEntityLogicalName,
            join.OuterColumns,
            new LinkSpec(join.InnerEntityLogicalName, join.InnerKeyAttribute, join.OuterKeyAttribute, join.InnerAlias,
                join.IsOuterJoin ? "outer" : "inner",
                join.InnerColumns),
            join.FilterWhereInnerIsNull
                ? new NullFilterSpec(join.InnerAlias, join.InnerEntityLogicalName + "id")
                : null);

    // -------------------------------------------------------------------------
    // Core builder
    // -------------------------------------------------------------------------

    private static string BuildFetchXml(
        string entityName,
        IReadOnlyList<string>? columns,
        LinkSpec? link,
        NullFilterSpec? nullFilter)
    {
        var entity = CreateEntityElement(entityName, columns);

        if (link is not null)
            entity.Add(CreateLinkEntityElement(link));

        if (nullFilter is not null)
            entity.Add(CreateNullFilterElement(nullFilter));

        return new XElement("fetch", new XAttribute("mapping", "logical"), entity).ToString();
    }

    // -------------------------------------------------------------------------
    // Element builders
    // -------------------------------------------------------------------------

    private static XElement CreateEntityElement(string name, IReadOnlyList<string>? columns)
    {
        var element = new XElement("entity", new XAttribute("name", name));
        AddColumns(element, columns);
        return element;
    }

    private static XElement CreateLinkEntityElement(LinkSpec link)
    {
        var element = new XElement("link-entity",
            new XAttribute("name", link.Name),
            new XAttribute("from", link.From),
            new XAttribute("to", link.To),
            new XAttribute("alias", link.Alias),
            new XAttribute("link-type", link.LinkType));
        AddColumns(element, link.Columns);
        return element;
    }

    private static XElement CreateNullFilterElement(NullFilterSpec filter) =>
        new XElement("filter",
            new XElement("condition",
                new XAttribute("entityname", filter.Alias),
                new XAttribute("attribute", filter.Attribute),
                new XAttribute("operator", "null")));

    private static void AddColumns(XElement element, IReadOnlyList<string>? columns)
    {
        if (columns is { Count: > 0 })
            foreach (var col in columns)
                element.Add(new XElement("attribute", new XAttribute("name", col)));
        else
            element.Add(new XElement("all-attributes"));
    }
}

internal sealed record LinkSpec(
    string Name,
    string From,
    string To,
    string Alias,
    string LinkType,
    IReadOnlyList<string>? Columns = null);

internal sealed record NullFilterSpec(string Alias, string Attribute);

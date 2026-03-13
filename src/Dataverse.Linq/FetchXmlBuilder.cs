using Dataverse.Linq.Expressions;
using System.Xml.Linq;

namespace Dataverse.Linq;

internal static class FetchXmlBuilder
{
    internal static string Build(string entityLogicalName, IReadOnlyList<string>? columns = null)
    {
        var entityElement = new XElement("entity", new XAttribute("name", entityLogicalName));

        if (columns is { Count: > 0 })
            foreach (var column in columns)
                entityElement.Add(new XElement("attribute", new XAttribute("name", column)));
        else
            entityElement.Add(new XElement("all-attributes"));

        return new XElement("fetch", new XAttribute("mapping", "logical"), entityElement).ToString();
    }

    internal static string BuildJoin(JoinInfo join)
    {
        var entityElement = new XElement("entity", new XAttribute("name", join.OuterEntityLogicalName));

        if (join.OuterColumns is { Count: > 0 })
            foreach (var col in join.OuterColumns)
                entityElement.Add(new XElement("attribute", new XAttribute("name", col)));
        else
            entityElement.Add(new XElement("all-attributes"));

        var linkElement = new XElement("link-entity",
            new XAttribute("name", join.InnerEntityLogicalName),
            new XAttribute("from", join.InnerKeyAttribute),
            new XAttribute("to", join.OuterKeyAttribute),
            new XAttribute("alias", join.InnerAlias),
            new XAttribute("link-type", "inner"));

        if (join.InnerColumns is { Count: > 0 })
            foreach (var col in join.InnerColumns)
                linkElement.Add(new XElement("attribute", new XAttribute("name", col)));
        else
            linkElement.Add(new XElement("all-attributes"));

        entityElement.Add(linkElement);

        return new XElement("fetch", new XAttribute("mapping", "logical"), entityElement).ToString();
    }

    internal static string BuildLeftJoin(LeftJoinInfo join)
    {
        var entityElement = new XElement("entity", new XAttribute("name", join.OuterEntityLogicalName));

        if (join.OuterColumns is { Count: > 0 })
            foreach (var col in join.OuterColumns)
                entityElement.Add(new XElement("attribute", new XAttribute("name", col)));
        else
            entityElement.Add(new XElement("all-attributes"));

        var linkElement = new XElement("link-entity",
            new XAttribute("name", join.InnerEntityLogicalName),
            new XAttribute("from", join.InnerKeyAttribute),
            new XAttribute("to", join.OuterKeyAttribute),
            new XAttribute("alias", join.InnerAlias),
            new XAttribute("link-type", "outer"));

        entityElement.Add(linkElement);

        if (join.FilterWhereInnerIsNull)
        {
            entityElement.Add(new XElement("filter",
                new XElement("condition",
                    new XAttribute("entityname", join.InnerAlias),
                    new XAttribute("attribute", join.InnerEntityLogicalName + "id"),
                    new XAttribute("operator", "null"))));
        }

        return new XElement("fetch", new XAttribute("mapping", "logical"), entityElement).ToString();
    }
}

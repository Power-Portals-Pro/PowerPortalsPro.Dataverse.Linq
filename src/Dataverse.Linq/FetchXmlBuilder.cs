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
}

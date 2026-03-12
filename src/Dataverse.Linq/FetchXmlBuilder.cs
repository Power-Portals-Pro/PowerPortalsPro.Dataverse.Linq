using System.Xml.Linq;

namespace Dataverse.Linq;

internal static class FetchXmlBuilder
{
    internal static string Build(string entityLogicalName)
    {
        return new XElement("fetch",
            new XAttribute("mapping", "logical"),
            new XElement("entity",
                new XAttribute("name", entityLogicalName),
                new XElement("all-attributes")))
            .ToString();
    }
}

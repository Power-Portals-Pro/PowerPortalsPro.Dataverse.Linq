using System.Reflection;

namespace Dataverse.Linq.Model;

[AttributeUsage(AttributeTargets.Field)]
public sealed class FetchXmlValueAttribute(string value) : Attribute
{
    public string Value { get; } = value;
}

public enum FetchDatasource
{
    [FetchXmlValue("retained")]
    Retained,
}

internal static class FetchDatasourceExtensions
{
    internal static string ToFetchXmlString(this FetchDatasource datasource)
    {
        var member = typeof(FetchDatasource).GetField(datasource.ToString())!;
        var attr = member.GetCustomAttribute<FetchXmlValueAttribute>();
        return attr?.Value ?? datasource.ToString().ToLowerInvariant();
    }
}

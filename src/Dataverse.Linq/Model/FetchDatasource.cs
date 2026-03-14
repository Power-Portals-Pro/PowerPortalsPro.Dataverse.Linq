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

internal static class FetchXmlValueExtensions
{
    internal static string ToFetchXmlString<TEnum>(this TEnum value) where TEnum : struct, Enum
    {
        var member = typeof(TEnum).GetField(value.ToString()!)!;
        var attr = member.GetCustomAttribute<FetchXmlValueAttribute>();
        return attr?.Value ?? value.ToString()!.ToLowerInvariant();
    }
}

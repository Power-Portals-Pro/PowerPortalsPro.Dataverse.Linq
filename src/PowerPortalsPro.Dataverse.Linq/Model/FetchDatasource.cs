using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Model;

/// <summary>
/// Marks an enum field with its corresponding FetchXml string representation.
/// </summary>
[AttributeUsage(AttributeTargets.Field)]
public sealed class FetchXmlValueAttribute(string value) : Attribute
{
    /// <summary>
    /// The FetchXml string value for the decorated enum field.
    /// </summary>
    public string Value { get; } = value;
}

/// <summary>
/// Specifies the FetchXml <c>datasource</c> attribute value.
/// </summary>
public enum FetchDatasource
{
    /// <summary>
    /// Queries long-term retained data.
    /// </summary>
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

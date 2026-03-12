using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace Dataverse.Linq.Tests.ProxyClasses;

[EntityLogicalName("account")]
public class Account : Entity
{
    public new const string LogicalName = "account";
    public const string PrimaryIdAttribute = "accountid";

    public Account() : base(LogicalName) { }

    public Account(Entity entity) : base(LogicalName)
    {
        Id = entity.Id;
        Attributes.AddRange(entity.Attributes);
    }

    [AttributeLogicalName("name")]
    public string? Name
    {
        get => GetAttributeValue<string>("name");
        set => SetAttributeValue("name", value);
    }

    [AttributeLogicalName("websiteurl")]
    public string? Website
    {
        get => GetAttributeValue<string>("websiteurl");
        set => SetAttributeValue("websiteurl", value);
    }

    [AttributeLogicalName("primarycontactid")]
    public EntityReference? PrimaryContact
    {
        get => GetAttributeValue<EntityReference>("primarycontactid");
        set => SetAttributeValue("primarycontactid", value);
    }
}

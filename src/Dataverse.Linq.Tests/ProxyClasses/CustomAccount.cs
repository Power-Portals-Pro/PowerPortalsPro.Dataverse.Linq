using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace Dataverse.Linq.Tests.ProxyClasses;

[EntityLogicalName("new_customaccount")]
public class CustomAccount : Entity
{
    public new const string LogicalName = "new_customaccount";
    public const string PrimaryIdAttribute = "new_customaccountid";

    public CustomAccount() : base(LogicalName) { }

    public CustomAccount(Entity entity) : base(LogicalName)
    {
        Id = entity.Id;
        Attributes.AddRange(entity.Attributes);
    }

    [AttributeLogicalName("new_name")]
    public string? Name
    {
        get => GetAttributeValue<string>("new_name");
        set => SetAttributeValue("new_name", value);
    }

    [AttributeLogicalName("new_website")]
    public string? Website
    {
        get => GetAttributeValue<string>("new_website");
        set => SetAttributeValue("new_website", value);
    }

    [AttributeLogicalName("new_primarycontact")]
    public EntityReference? PrimaryContact
    {
        get => GetAttributeValue<EntityReference>("new_primarycontact");
        set => SetAttributeValue("new_primarycontact", value);
    }
}

using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace Dataverse.Linq.Tests.ProxyClasses;

[EntityLogicalName("new_customcontact")]
public class CustomContact : Entity
{
    public new const string LogicalName = "new_customcontact";
    public const string PrimaryIdAttribute = "new_customcontactid";

    public CustomContact() : base(LogicalName) { }

    public CustomContact(Entity entity) : base(LogicalName)
    {
        Id = entity.Id;
        Attributes.AddRange(entity.Attributes);
    }

    [AttributeLogicalName("new_firstname")]
    public string? FirstName
    {
        get => GetAttributeValue<string>("new_firstname");
        set => SetAttributeValue("new_firstname", value);
    }

    [AttributeLogicalName("new_lastname")]
    public string? LastName
    {
        get => GetAttributeValue<string>("new_lastname");
        set => SetAttributeValue("new_lastname", value);
    }

    [AttributeLogicalName("new_parentaccount")]
    public EntityReference? ParentAccount
    {
        get => GetAttributeValue<EntityReference>("new_parentaccount");
        set => SetAttributeValue("new_parentaccount", value);
    }

    [AttributeLogicalName("new_parentaccount")]
    public Guid? ParentAccountId
    {
        get => GetAttributeValue<EntityReference>("new_parentaccount")?.Id;
        set => SetAttributeValue("new_parentaccount",
            value.HasValue ? new EntityReference(CustomAccount.LogicalName, value.Value) : null);
    }
}

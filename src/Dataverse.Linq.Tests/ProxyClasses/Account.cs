using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace Dataverse.Linq.Tests.ProxyClasses;

[EntityLogicalName("account")]
public class Account : Entity
{
    public new const string LogicalName = "account";

    public Account() : base(LogicalName) { }

    public Account(Entity entity) : base(LogicalName)
    {
        Id = entity.Id;
        Attributes.AddRange(entity.Attributes);
    }

    public string? Name
    {
        get => GetAttributeValue<string>("name");
        set => SetAttributeValue("name", value);
    }
}

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class FilterIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    // -------------------------------------------------------------------------
    // Compare to constant with Or condition
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ConstantOrCondition_WithNullVariable_ThrowsNotSupported()
    {
        var date = (DateTime?)null;

        var act = () => (from a in Service.Queryable<CustomAccount>()
                         where (date == null || a.CreatedOn > date)
                         select a).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Where_ConstantAndCondition_WithNullVariable_ThrowsNotSupported()
    {
        var date = (DateTime?)null;

        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.PrimaryContact.Id equals c.CustomContactId into contacts
                         from c in contacts.DefaultIfEmpty()
                         where (a.CreatedOn > c.CreatedOn && date == null)
                         select a).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Contains with zero elements
    // -------------------------------------------------------------------------

    [Fact]
    public void Where_ContainsWithEmptyGuidList_ThrowsServerError()
    {
        var accountIds = new List<Guid>();

        var act = () => Service.Queryable<CustomAccount>()
            .Where(a => accountIds.Contains(a.CustomAccountId))
            .ToList();

        act.Should().Throw<Exception>();
    }
}

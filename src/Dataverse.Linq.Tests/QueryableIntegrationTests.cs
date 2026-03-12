using FluentAssertions;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using System;

namespace Dataverse.Linq.Tests;

public class QueryableIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task TestConnectionAsync()
    {
        var sut = this.ServiceProvider.GetRequiredService<ServiceClient>();
        var whoAmIResponse = (WhoAmIResponse)await sut.ExecuteAsync(new WhoAmIRequest());
        whoAmIResponse.UserId.Should().NotBe(Guid.Empty);
    }
}

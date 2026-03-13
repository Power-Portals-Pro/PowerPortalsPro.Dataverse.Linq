using Dataverse.Linq.Tests.Proxies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Dataverse.Linq.Tests;

public class DataManagementTests : IntegrationTestBase
{
    private ServiceClient Service => ServiceProvider.GetRequiredService<ServiceClient>();

    //[Fact]
    [Fact(Skip = "Run on demand only")]
    public async Task SeedData()
    {
        await DeleteAllAsync(CustomAccount.LogicalName);
        await DeleteAllAsync(CustomContact.LogicalName);

        var accountIds = await SeedAccountsAsync(100);
        var contactIds = await SeedContactsAsync(accountIds);
        await LinkContactsToAccountsAsync(accountIds, contactIds);
        await SeedAccountsWithoutContactsAsync(50);
    }

    //[Fact]
    [Fact(Skip = "Run on demand only")]
    public async Task DeleteAllData()
    {
        await DeleteAllAsync(CustomAccount.LogicalName);
        await DeleteAllAsync(CustomContact.LogicalName);
    }

    // -------------------------------------------------------------------------
    // Seeding
    // -------------------------------------------------------------------------

    private async Task SeedAccountsWithoutContactsAsync(int count)
    {
        var requests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = false },
            Requests = new OrganizationRequestCollection()
        };

        for (var i = 1; i <= count; i++)
        {
            var account = new Entity(CustomAccount.LogicalName)
            {
                ["new_name"] = $"Empty Account {i:D3}",
                ["new_website"] = $"https://empty{i:D3}.example.com"
            };
            requests.Requests.Add(new CreateRequest { Target = account });
        }

        await Service.ExecuteAsync(requests);
    }

    private async Task<List<Guid>> SeedAccountsAsync(int count)
    {
        var requests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = true },
            Requests = new OrganizationRequestCollection()
        };

        for (var i = 1; i <= count; i++)
        {
            var account = new Entity(CustomAccount.LogicalName)
            {
                ["new_name"] = $"Custom Account {i:D3}",
                ["new_website"] = $"https://account{i:D3}.example.com"
            };
            requests.Requests.Add(new CreateRequest { Target = account });
        }

        var response = (ExecuteMultipleResponse)await Service.ExecuteAsync(requests);

        return response.Responses
            .Select(r => ((CreateResponse)r.Response).id)
            .ToList();
    }

    private async Task<List<Guid>> SeedContactsAsync(List<Guid> accountIds)
    {
        const int contactsPerAccount = 5;

        var requests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = true },
            Requests = new OrganizationRequestCollection()
        };

        for (var a = 0; a < accountIds.Count; a++)
        {
            for (var c = 1; c <= contactsPerAccount; c++)
            {
                var contact = new Entity(CustomContact.LogicalName)
                {
                    ["new_firstname"] = $"First{a + 1:D3}",
                    ["new_lastname"] = $"Last{c}",
                    ["new_parentaccount"] = new EntityReference(CustomAccount.LogicalName, accountIds[a])
                };
                requests.Requests.Add(new CreateRequest { Target = contact });
            }
        }

        var response = (ExecuteMultipleResponse)await Service.ExecuteAsync(requests);

        return response.Responses
            .Select(r => ((CreateResponse)r.Response).id)
            .ToList();
    }

    private async Task LinkContactsToAccountsAsync(List<Guid> accountIds, List<Guid> contactIds)
    {
        const int contactsPerAccount = 5;

        var requests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = false },
            Requests = new OrganizationRequestCollection()
        };

        for (var i = 0; i < accountIds.Count; i++)
        {
            var account = new Entity(CustomAccount.LogicalName, accountIds[i])
            {
                ["new_primarycontact"] = new EntityReference(CustomContact.LogicalName, contactIds[i * contactsPerAccount])
            };
            requests.Requests.Add(new UpdateRequest { Target = account });
        }

        await Service.ExecuteAsync(requests);
    }

    // -------------------------------------------------------------------------
    // Deletion
    // -------------------------------------------------------------------------

    private async Task DeleteAllAsync(string entityLogicalName)
    {
        var ids = await RetrieveAllIdsAsync(entityLogicalName);
        if (ids.Count == 0) return;

        const int batchSize = 100;
        for (var offset = 0; offset < ids.Count; offset += batchSize)
        {
            var batch = ids.Skip(offset).Take(batchSize);
            var requests = new ExecuteMultipleRequest
            {
                Settings = new ExecuteMultipleSettings { ContinueOnError = true, ReturnResponses = false },
                Requests = new OrganizationRequestCollection()
            };

            foreach (var id in batch)
                requests.Requests.Add(new DeleteRequest { Target = new EntityReference(entityLogicalName, id) });

            await Service.ExecuteAsync(requests);
        }
    }

    private async Task<List<Guid>> RetrieveAllIdsAsync(string entityLogicalName)
    {
        var ids = new List<Guid>();
        var query = new QueryExpression(entityLogicalName)
        {
            ColumnSet = new ColumnSet(false),
            PageInfo = new PagingInfo { Count = 5000, PageNumber = 1 }
        };

        while (true)
        {
            var response = await Service.RetrieveMultipleAsync(query);
            ids.AddRange(response.Entities.Select(e => e.Id));

            if (!response.MoreRecords) break;
            query.PageInfo.PageNumber++;
            query.PageInfo.PagingCookie = response.PagingCookie;
        }

        return ids;
    }
}

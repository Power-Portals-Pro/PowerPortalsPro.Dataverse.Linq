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
        await DeleteAllAsync(CustomOpportunity.LogicalName);

        var accountIds = await SeedAccountsAsync(100);
        await SetParentAccountsAsync(accountIds);
        var contactIds = await SeedContactsAsync(accountIds);
        await LinkContactsToAccountsAsync(accountIds, contactIds);
        await SeedAccountsWithoutContactsAsync(50);
        await SeedOpportunitiesAsync(contactIds);
    }

    //[Fact]
    [Fact(Skip = "Run on demand only")]
    public async Task DeleteAllData()
    {
        await DeleteAllAsync(CustomAccount.LogicalName);
        await DeleteAllAsync(CustomContact.LogicalName);
        await DeleteAllAsync(CustomOpportunity.LogicalName);
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

    private static readonly CustomAccount.AccountRating_Enum[] Ratings =
        [CustomAccount.AccountRating_Enum.Cold, CustomAccount.AccountRating_Enum.Warm, CustomAccount.AccountRating_Enum.Hot];

    private static readonly CustomContact.Color[][] ColorSets =
    [
        [CustomContact.Color.Red],
        [CustomContact.Color.Green, CustomContact.Color.Yellow, CustomContact.Color.Red],
        [CustomContact.Color.Blue],
        [CustomContact.Color.Purple, CustomContact.Color.Pink, CustomContact.Color.Orange],
        [CustomContact.Color.Blue, CustomContact.Color.Green],
        [CustomContact.Color.Black, CustomContact.Color.White],
    ];

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

            // Each column uses a different modulus so nulls are spread across
            // different records, giving varied test coverage.
            if (i % 5 != 0)
                account["new_accountrating"] = new OptionSetValue((int)Ratings[i % 3]);
            if (i % 7 != 0)
                account["new_creditlimit"] = new Money(i * 1000m + 0.50m);
            if (i % 4 != 0)
                account["new_datecompanywasorganized"] = new DateTime(2000 + (i % 25), (i % 12) + 1, (i % 28) + 1);
            if (i % 6 != 0)
                account["new_description"] = $"Description for account {i:D3}";
            if (i % 5 != 1)
                account["new_ispreferredaccount"] = i % 2 == 0;
            if (i % 3 != 0)
                account["new_latitude"] = 30.0 + (i % 60);
            if (i % 3 != 0)
                account["new_longitude"] = -120.0 + (i % 120);
            if (i % 8 != 0)
                account["new_numberofemployees"] = i * 10;
            if (i % 4 != 1)
                account["new_percentcomplete"] = (decimal)(i % 101);
            if (i % 6 != 1)
                account["new_primaryemail"] = $"account{i:D3}@example.com";

            requests.Requests.Add(new CreateRequest { Target = account });
        }

        var response = (ExecuteMultipleResponse)await Service.ExecuteAsync(requests);

        return response.Responses
            .Select(r => ((CreateResponse)r.Response).id)
            .ToList();
    }

    /// <summary>
    /// Sets parent accounts to create a hierarchy for CountChildren testing.
    /// Accounts 1-10 become parents; accounts 11-50 are assigned as children
    /// (4 children per parent).
    /// </summary>
    private async Task SetParentAccountsAsync(List<Guid> accountIds)
    {
        const int parentCount = 10;
        const int childrenPerParent = 4;

        var requests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = false },
            Requests = new OrganizationRequestCollection()
        };

        for (var p = 0; p < parentCount; p++)
        {
            for (var c = 0; c < childrenPerParent; c++)
            {
                var childIndex = parentCount + (p * childrenPerParent) + c;
                if (childIndex >= accountIds.Count) break;

                var update = new Entity(CustomAccount.LogicalName, accountIds[childIndex])
                {
                    ["new_parentaccount"] = new EntityReference(CustomAccount.LogicalName, accountIds[p])
                };
                requests.Requests.Add(new UpdateRequest { Target = update });
            }
        }

        await Service.ExecuteAsync(requests);
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
                var firstName = $"First{a + 1:D3}";
                var lastName = $"Last{c}";
                var contact = new Entity(CustomContact.LogicalName)
                {
                    ["new_firstname"] = firstName,
                    ["new_lastname"] = lastName,
                    ["new_name"] = $"{firstName} {lastName}",
                    ["new_parentaccount"] = new EntityReference(CustomAccount.LogicalName, accountIds[a])
                };

                // Assign contact rating — cycle through Ratings; skip every 4th contact
                var contactIndex = a * contactsPerAccount + c;
                if (contactIndex % 4 != 0)
                    contact["new_contactrating"] = new OptionSetValue((int)Ratings[contactIndex % 3]);

                // Assign favorite colors — cycle through ColorSets; skip every 6th contact
                if (contactIndex % 6 != 0)
                {
                    var colors = ColorSets[(contactIndex - 1) % ColorSets.Length];
                    contact["new_favoritecolors"] = new OptionSetValueCollection(
                        colors.Select(clr => new OptionSetValue((int)clr)).ToList());
                }

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

    private static readonly CustomOpportunity.CustomOpportunity_StatusReason[] OpportunityStatuses =
        [CustomOpportunity.CustomOpportunity_StatusReason.Active,
         CustomOpportunity.CustomOpportunity_StatusReason.Won,
         CustomOpportunity.CustomOpportunity_StatusReason.Lost];

    private async Task SeedOpportunitiesAsync(List<Guid> contactIds, int count = 100)
    {
        var total = Math.Min(count, contactIds.Count);

        // Step 1: Create all opportunities as Active
        var createRequests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = true },
            Requests = new OrganizationRequestCollection()
        };

        var targetStatuses = new List<CustomOpportunity.CustomOpportunity_StatusReason>();

        for (var i = 0; i < total; i++)
        {
            var statusReason = OpportunityStatuses[i % 3];
            targetStatuses.Add(statusReason);

            var opportunity = new CustomOpportunity
            {
                Name = $"Opportunity {i + 1:D3}",
                Contact = new EntityReference(CustomContact.LogicalName, contactIds[i]),
                ActualCloseDate = new DateTime(2020 + (i % 5), (i % 12) + 1, (i % 28) + 1),
                ActualRevenueMoney = new Money((i + 1) * 1000m + 500m),
                EstimatedRevenueMoney = new Money((i + 1) * 1500m + 250m),
            };

            createRequests.Requests.Add(new CreateRequest { Target = opportunity });
        }

        var createResults = (ExecuteMultipleResponse)await Service.ExecuteAsync(createRequests);
        if (createResults.Responses.Any(r => r.Fault != null))
        {
            var errors = createResults.Responses.Where(r => r.Fault != null)
                .Select(r => $"Request {r.RequestIndex}: {r.Fault.Message}");
            throw new Exception($"Errors occurred during opportunity creation:\n{(string.Join("\n", errors))}");
        }

        // Step 2: Update non-Active records to their target state
        var updateRequests = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = false },
            Requests = new OrganizationRequestCollection()
        };

        for (var i = 0; i < total; i++)
        {
            var statusReason = targetStatuses[i];
            if (statusReason == CustomOpportunity.CustomOpportunity_StatusReason.Active)
                continue;

            var id = ((CreateResponse)createResults.Responses[i].Response).id;
            var update = new CustomOpportunity
            {
                Id = id,
                Status = CustomOpportunity.CustomOpportunity_Status.Inactive,
                StatusReason = statusReason,
            };

            updateRequests.Requests.Add(new UpdateRequest { Target = update });
        }

        if (updateRequests.Requests.Count > 0)
        {
            var updateResults = (ExecuteMultipleResponse)await Service.ExecuteAsync(updateRequests);
            if (updateResults.Responses.Any(r => r.Fault != null))
            {
                var errors = updateResults.Responses.Where(r => r.Fault != null)
                    .Select(r => $"Request {r.RequestIndex}: {r.Fault.Message}");
                throw new Exception($"Errors occurred during opportunity state update:\n{(string.Join("\n", errors))}");
            }
        }
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

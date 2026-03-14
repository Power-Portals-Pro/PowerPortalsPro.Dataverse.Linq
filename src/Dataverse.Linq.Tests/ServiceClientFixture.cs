using Microsoft.Extensions.Configuration;
using Microsoft.PowerPlatform.Dataverse.Client;

namespace Dataverse.Linq.Tests;

public class ServiceClientFixture : IDisposable
{
    private readonly Lazy<ServiceClient> _primaryClient;

    public ServiceClientFixture()
    {
        var configurationBuilder = new ConfigurationBuilder();
        var appSettingsFileLocation = $"{Path.GetDirectoryName(typeof(ServiceClientFixture).Assembly.Location)}\\appsettings.json";
        configurationBuilder.AddJsonFile(appSettingsFileLocation);
        configurationBuilder.AddUserSecrets(typeof(ServiceClientFixture).Assembly);
        Configuration = configurationBuilder.Build();

        _primaryClient = new Lazy<ServiceClient>(CreatePrimaryClient);
    }

    public IConfiguration Configuration { get; }

    public ServiceClient CreateClient()
    {
        if (_primaryClient.IsValueCreated)
            return _primaryClient.Value.Clone();

        // First call — force creation and return a clone so the primary stays pristine.
        var primary = _primaryClient.Value;
        return primary.Clone();
    }

    private ServiceClient CreatePrimaryClient()
    {
        var orgUrl = Configuration.GetValue<string>("D365:Url");
        if (string.IsNullOrWhiteSpace(orgUrl))
            throw new Exception("Please ensure all configurations are supplied for creating an Organization Service.");

        var clientId = Configuration.GetValue<string>("D365:ClientId");
        var secret = Configuration.GetValue<string>("D365:Secret");
        var username = Configuration.GetValue<string>("D365:Username");
        var password = Configuration.GetValue<string>("D365:Password");

        var orgUri = new Uri(orgUrl);
        var rootOrgUrl = $"{orgUri.Scheme}://{orgUri.Authority}";
        var connectionString = !string.IsNullOrWhiteSpace(username)
            ? $"AuthType='OAuth';ServiceUri='{rootOrgUrl}';Username='{username}';Password='{password}'"
            : $"AuthType='ClientSecret';ServiceUri='{rootOrgUrl}';ClientId='{clientId}';ClientSecret='{secret}'";

        var client = new ServiceClient(connectionString);
        var impersonatingUser = Configuration.GetValue<Guid?>("D365:ImpersonatedUserId");
        if (impersonatingUser.HasValue)
            client.CallerId = impersonatingUser.Value;

        client.EnableAffinityCookie = false;
        return client;
    }

    public void Dispose()
    {
        if (_primaryClient.IsValueCreated)
            _primaryClient.Value.Dispose();
    }
}

[CollectionDefinition("Dataverse")]
public class DataverseCollection : ICollectionFixture<ServiceClientFixture>;

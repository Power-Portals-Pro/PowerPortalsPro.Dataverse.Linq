using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using System;
using System.Net.Http.Headers;
using IConfiguration = Microsoft.Extensions.Configuration.IConfiguration;

namespace Dataverse.Linq.Tests;

public class IntegrationTestBase : IDisposable
{
    private readonly IConfiguration _configuration;
    private readonly IServiceScope _serviceScope;
    private readonly IServiceProvider _serviceProvider;

    public IntegrationTestBase()
    {
        var configurationBuilder = new ConfigurationBuilder();
        var appSettingsFileLocation = $"{Path.GetDirectoryName(typeof(IntegrationTestBase).Assembly.Location)}\\appsettings.json";
        configurationBuilder.AddJsonFile(appSettingsFileLocation);
        this.BuildConfiguration(configurationBuilder);
        configurationBuilder.AddUserSecrets(typeof(IntegrationTestBase).Assembly);
        _configuration = configurationBuilder.Build();

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<IConfiguration>(x => _configuration);
        serviceCollection.AddSingleton<IOrganizationService>(x => this.CreateOrganizationService());
        serviceCollection.AddSingleton<ServiceClient>(x => this.CreateOrganizationService());
        serviceCollection.AddSingleton<ITracingService>(x => new ConsoleTracer());

        serviceCollection.AddLogging(builder => builder.AddConsole());

        // Allow inheritors to register any services they need for execution.
        this.RegisterServices(serviceCollection);

        var serviceProvider = serviceCollection.BuildServiceProvider();
        _serviceScope = serviceProvider.CreateScope();
        _serviceProvider = _serviceScope.ServiceProvider;
    }

    protected IServiceProvider ServiceProvider => _serviceProvider;

    protected IConfiguration Configuration => _configuration;

    protected virtual void RegisterServices(IServiceCollection services)
    {
        // Nothing
    }

    protected virtual void BuildConfiguration(IConfigurationBuilder configurationBuilder)
    {
        // Nothing
    }

    public void Dispose()
    {
        _serviceScope?.Dispose();
    }

    protected ServiceClient CreateOrganizationService(Guid? impersonatingUser = null)
    {
        var orgUrl = _configuration.GetValue<string>("D365:Url");
        if (string.IsNullOrWhiteSpace(orgUrl))
            throw new Exception("Please ensure all configurations are supplied for creating an Organization Service.");

        return this.CreateOrganizationService(orgUrl, impersonatingUser);
    }

    protected ServiceClient CreateOrganizationService(string orgUrl, Guid? impersonatingUser = null)
    {
        var clientId = _configuration.GetValue<string>("D365:ClientId");
        var secret = _configuration.GetValue<string>("D365:Secret");
        var username = _configuration.GetValue<string>("D365:Username");
        var password = _configuration.GetValue<string>("D365:Password");

        if (string.IsNullOrWhiteSpace(orgUrl))
            throw new Exception("Please ensure all configurations are supplied for creating an Organization Service.");

        var orgUri = new Uri(orgUrl);
        var rootOrgUrl = $"{orgUri.Scheme}://{orgUri.Authority}";
        var connectionString = !string.IsNullOrWhiteSpace(username)
            ? $"AuthType='OAuth';ServiceUri='{rootOrgUrl}';Username='{username}';Password='{password}'"
            : $"AuthType='ClientSecret';ServiceUri='{rootOrgUrl}';ClientId='{clientId}';ClientSecret='{secret}'";

        var client = new ServiceClient(connectionString);
        var impersonatingUserFromConfig = _configuration.GetValue<Guid?>("D365:ImpersonatedUserId");
        if (impersonatingUser.HasValue || impersonatingUserFromConfig.HasValue)
            client.CallerId = impersonatingUser ?? impersonatingUserFromConfig ?? Guid.Empty;

        client.EnableAffinityCookie = false;
        return client;
    }

    protected async Task<HttpClient> CreateWebApiHttpClientAsync()
    {
        var tenantId = _configuration.GetValue<string>("Azure:TenantId");
        var orgUrl = _configuration.GetValue<string>("D365:Url");
        var clientId = _configuration.GetValue<string>("D365:ClientId");
        var secret = _configuration.GetValue<string>("D365:Secret");

        if (string.IsNullOrWhiteSpace(orgUrl))
            throw new Exception("Please ensure all configurations are supplied for creating an Organization Service.");

        var orgUri = new Uri(orgUrl);
        var rootOrgUrl = $"{orgUri.Scheme}://{orgUri.Authority}";
        var scope = new[] { $"{rootOrgUrl}/.default" };
        var authority = $"https://login.microsoftonline.com/{tenantId}";

        var clientApp = ConfidentialClientApplicationBuilder.Create(clientId)
             .WithClientSecret(secret)
             .WithAuthority(new Uri(authority))
             .Build();

        var authResult = await clientApp.AcquireTokenForClient(scope).ExecuteAsync();

        var httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{orgUrl}api/data/v9.2/"),
            Timeout = TimeSpan.FromMinutes(10),
        };

        httpClient.DefaultRequestHeaders.Add("OData-MaxVersion", "4.0");
        httpClient.DefaultRequestHeaders.Add("OData-Version", "4.0");
        httpClient.DefaultRequestHeaders.Add("Prefer", "odata.include-annotations=*");
        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);

        return httpClient;
    }

    protected class ConsoleTracer : ITracingService
    {
        public void Trace(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }
}

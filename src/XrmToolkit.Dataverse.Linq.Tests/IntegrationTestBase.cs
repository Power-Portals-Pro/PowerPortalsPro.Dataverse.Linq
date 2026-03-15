using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
#if !NETFRAMEWORK
using Microsoft.Identity.Client;
using System.Net.Http.Headers;
#endif

namespace XrmToolkit.Dataverse.Linq.Tests;

[Collection("Dataverse")]
public class IntegrationTestBase : IDisposable
{
    private readonly IServiceScope _serviceScope;
    private readonly IServiceProvider _serviceProvider;

    public IntegrationTestBase(ServiceClientFixture fixture)
    {
        Fixture = fixture;

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<IConfiguration>(_ => fixture.Configuration);
        serviceCollection.AddSingleton<IOrganizationService>(_ => fixture.CreateClient());
        serviceCollection.AddSingleton<ServiceClient>(_ => fixture.CreateClient());
        serviceCollection.AddSingleton<ITracingService>(_ => new ConsoleTracer());

        serviceCollection.AddLogging(builder => builder.AddConsole());

        this.RegisterServices(serviceCollection);

        var serviceProvider = serviceCollection.BuildServiceProvider();
        _serviceScope = serviceProvider.CreateScope();
        _serviceProvider = _serviceScope.ServiceProvider;
    }

    protected ServiceClientFixture Fixture { get; }

    protected IServiceProvider ServiceProvider => _serviceProvider;

    protected IConfiguration Configuration => Fixture.Configuration;

    protected virtual void RegisterServices(IServiceCollection services)
    {
        // Nothing
    }

    public void Dispose()
    {
        _serviceScope?.Dispose();
    }

#if !NETFRAMEWORK
    protected async Task<HttpClient> CreateWebApiHttpClientAsync()
    {
        var config = Fixture.Configuration;
        var tenantId = config.GetValue<string>("Azure:TenantId");
        var orgUrl = config.GetValue<string>("D365:Url");
        var clientId = config.GetValue<string>("D365:ClientId");
        var secret = config.GetValue<string>("D365:Secret");

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
#endif

    protected class ConsoleTracer : ITracingService
    {
        public void Trace(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }
}

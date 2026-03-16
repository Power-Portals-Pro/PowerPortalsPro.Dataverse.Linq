using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.ServiceModel;

namespace PowerPortalsPro.Dataverse.Linq.Tests;

/// <summary>
/// Wraps a <see cref="ServiceClient"/> and retries transient Dataverse errors
/// (e.g. server throttling, metadata timestamp failures) with exponential backoff.
/// </summary>
internal class RetryOrganizationService(ServiceClient inner, int maxRetries = 3)
#if !NETFRAMEWORK
    : IOrganizationServiceAsync
#else
    : IOrganizationService
#endif
{
    private static bool IsTransient(FaultException<OrganizationServiceFault> ex) =>
        ex.Detail?.ErrorCode == -2147204784 // Server busy / throttled
        || ex.Message.Contains("MetadataTimestamp", StringComparison.OrdinalIgnoreCase)
        || ex.Message.Contains("exceeded", StringComparison.OrdinalIgnoreCase);

    private T ExecuteWithRetry<T>(Func<T> action)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return action();
            }
            catch (FaultException<OrganizationServiceFault> ex) when (attempt < maxRetries && IsTransient(ex))
            {
                Thread.Sleep(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
            }
        }
    }

    private void ExecuteWithRetry(Action action)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                action();
                return;
            }
            catch (FaultException<OrganizationServiceFault> ex) when (attempt < maxRetries && IsTransient(ex))
            {
                Thread.Sleep(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
            }
        }
    }

#if !NETFRAMEWORK
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> action)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await action();
            }
            catch (FaultException<OrganizationServiceFault> ex) when (attempt < maxRetries && IsTransient(ex))
            {
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
            }
        }
    }

    private async Task ExecuteWithRetryAsync(Func<Task> action)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch (FaultException<OrganizationServiceFault> ex) when (attempt < maxRetries && IsTransient(ex))
            {
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
            }
        }
    }
#endif

    // IOrganizationService
    public Guid Create(Entity entity) => ExecuteWithRetry(() => inner.Create(entity));
    public void Update(Entity entity) => ExecuteWithRetry(() => inner.Update(entity));
    public void Delete(string entityName, Guid id) => ExecuteWithRetry(() => inner.Delete(entityName, id));
    public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) =>
        ExecuteWithRetry(() => inner.Retrieve(entityName, id, columnSet));
    public EntityCollection RetrieveMultiple(QueryBase query) =>
        ExecuteWithRetry(() => inner.RetrieveMultiple(query));
    public OrganizationResponse Execute(OrganizationRequest request) =>
        ExecuteWithRetry(() => inner.Execute(request));
    public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) =>
        ExecuteWithRetry(() => inner.Associate(entityName, entityId, relationship, relatedEntities));
    public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) =>
        ExecuteWithRetry(() => inner.Disassociate(entityName, entityId, relationship, relatedEntities));

#if !NETFRAMEWORK
    // IOrganizationServiceAsync
    public Task<Guid> CreateAsync(Entity entity) =>
        ExecuteWithRetryAsync(() => inner.CreateAsync(entity));
    public Task UpdateAsync(Entity entity) =>
        ExecuteWithRetryAsync(() => inner.UpdateAsync(entity));
    public Task DeleteAsync(string entityName, Guid id) =>
        ExecuteWithRetryAsync(() => inner.DeleteAsync(entityName, id));
    public Task<Entity> RetrieveAsync(string entityName, Guid id, ColumnSet columnSet) =>
        ExecuteWithRetryAsync(() => inner.RetrieveAsync(entityName, id, columnSet));
    public Task<EntityCollection> RetrieveMultipleAsync(QueryBase query) =>
        ExecuteWithRetryAsync(() => inner.RetrieveMultipleAsync(query));
    public Task<OrganizationResponse> ExecuteAsync(OrganizationRequest request) =>
        ExecuteWithRetryAsync(() => inner.ExecuteAsync(request));
    public Task AssociateAsync(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) =>
        ExecuteWithRetryAsync(() => inner.AssociateAsync(entityName, entityId, relationship, relatedEntities));
    public Task DisassociateAsync(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) =>
        ExecuteWithRetryAsync(() => inner.DisassociateAsync(entityName, entityId, relationship, relatedEntities));
#endif
}

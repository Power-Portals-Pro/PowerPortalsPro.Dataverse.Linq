using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using NSubstitute;
using System.Reflection;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public abstract class FetchXmlTestBase
{
#if !NETFRAMEWORK
    protected readonly IOrganizationServiceAsync _service;
#else
    protected readonly IOrganizationService _service;
#endif

    protected FetchXmlTestBase()
    {
#if !NETFRAMEWORK
        _service = Substitute.For<IOrganizationServiceAsync>();
#else
        _service = Substitute.For<IOrganizationService>();
#endif

        // Set up metadata responses for Entity.Id resolution
        _service.Execute(Arg.Any<OrganizationRequest>()).Returns(callInfo =>
        {
            var request = callInfo.Arg<OrganizationRequest>();
            if (request is RetrieveEntityRequest entityRequest)
            {
                var metadata = new EntityMetadata { LogicalName = entityRequest.LogicalName };
                // Set PrimaryIdAttribute via reflection (it has no public setter)
                typeof(EntityMetadata)
                    .GetProperty(nameof(EntityMetadata.PrimaryIdAttribute))!
                    .SetValue(metadata, $"{entityRequest.LogicalName}id");
                var response = new RetrieveEntityResponse();
                response.Results["EntityMetadata"] = metadata;
                return response;
            }
            throw new NotSupportedException($"Unexpected request type: {request.GetType().Name}");
        });

        // Clear the metadata cache so tests don't interfere with each other
        EntityMetadataCache.Clear();
    }

    protected static void AssertFetchXml(string actual, string expected) =>
        NormalizeLineEndings(actual).Should().Be(NormalizeLineEndings(expected));

    private static string NormalizeLineEndings(string value) =>
        value.Replace("\r\n", "\n").Replace("\r", "\n");

    protected static string TranslateToFetchXml<TEntity>(
        System.Linq.Expressions.Expression expression) where TEntity : Entity
    {
        var entityLogicalName = typeof(TEntity).GetCustomAttribute<EntityLogicalNameAttribute>()!.LogicalName;
        var query = Expressions.FetchXmlQueryTranslator.Translate<TEntity>(
            expression, null, entityLogicalName);
        return FetchXmlBuilder.Build(query);
    }
}

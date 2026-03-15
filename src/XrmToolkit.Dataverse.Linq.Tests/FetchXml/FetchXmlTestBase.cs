using XrmToolkit.Dataverse.Linq.Tests.Proxies;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using NSubstitute;
using System.Reflection;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

public abstract class FetchXmlTestBase
{
    protected readonly IOrganizationServiceAsync _service;

    protected FetchXmlTestBase()
    {
        _service = Substitute.For<IOrganizationServiceAsync>();

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
        actual.ReplaceLineEndings("\n").Should().Be(expected.ReplaceLineEndings("\n"));

    protected static string TranslateToFetchXml<TEntity>(
        System.Linq.Expressions.Expression expression) where TEntity : Entity
    {
        var entityLogicalName = typeof(TEntity).GetCustomAttribute<EntityLogicalNameAttribute>()!.LogicalName;
        var query = Expressions.FetchXmlQueryTranslator.Translate<TEntity>(
            expression, null, entityLogicalName);
        return FetchXmlBuilder.Build(query);
    }
}

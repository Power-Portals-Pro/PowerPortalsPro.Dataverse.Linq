using Dataverse.Linq.Tests.ProxyClasses;
using FluentAssertions;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using System.Xml.Linq;

namespace Dataverse.Linq.Tests;

public class QueryableTests
{
    private static EntityCollection SinglePage(params Entity[] entities) =>
        new(entities) { MoreRecords = false };

    // -------------------------------------------------------------------------
    // FetchXml shape
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_GeneratesCorrectFetchXml()
    {
        FetchExpression? captured = null;
        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Do<QueryBase>(q => captured = q as FetchExpression))
               .Returns(SinglePage());

        await service.Queryable<Account>().ToListAsync();

        captured.Should().NotBeNull();
        var xml = XDocument.Parse(captured!.Query);
        xml.Root!.Name.LocalName.Should().Be("fetch");
        xml.Root.Attribute("mapping")!.Value.Should().Be("logical");
        var entity = xml.Root.Element("entity");
        entity!.Attribute("name")!.Value.Should().Be("account");
        entity.Element("all-attributes").Should().NotBeNull();
    }

    // -------------------------------------------------------------------------
    // Result conversion
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_ReturnsEmptyList_WhenNoRecords()
    {
        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>()).Returns(SinglePage());

        var results = await service.Queryable<Account>().ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToListAsync_ConvertsEntitiesToT()
    {
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        var e1 = new Entity("account", id1) { ["name"] = "Contoso" };
        var e2 = new Entity("account", id2) { ["name"] = "Fabrikam" };

        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>()).Returns(SinglePage(e1, e2));

        var results = await service.Queryable<Account>().ToListAsync();

        results.Should().HaveCount(2);
        results[0].Id.Should().Be(id1);
        results[0].Name.Should().Be("Contoso");
        results[1].Id.Should().Be(id2);
        results[1].Name.Should().Be("Fabrikam");
    }

    // -------------------------------------------------------------------------
    // Paging
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_PagesThrough_WhenMoreRecordsIsTrue()
    {
        var page1 = new EntityCollection([new Entity("account", Guid.NewGuid())])
        {
            MoreRecords = true,
            PagingCookie = "<cookie page=\"1\" />",
        };
        var page2 = new EntityCollection([new Entity("account", Guid.NewGuid())])
        {
            MoreRecords = false,
        };

        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Any<QueryBase>()).Returns(page1, page2);

        var results = await service.Queryable<Account>().ToListAsync();

        results.Should().HaveCount(2);
        await service.Received(2).RetrieveMultipleAsync(Arg.Any<QueryBase>());
    }

    [Fact]
    public async Task ToListAsync_SecondPageRequest_IncludesPagingCookieAndPageNumber()
    {
        const string cookie = "<cookie page=\"1\" />";
        var capturedQueries = new List<string>();

        var page1 = new EntityCollection([new Entity("account", Guid.NewGuid())])
        {
            MoreRecords = true,
            PagingCookie = cookie,
        };
        var page2 = new EntityCollection([new Entity("account", Guid.NewGuid())])
        {
            MoreRecords = false,
        };

        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Do<QueryBase>(q => capturedQueries.Add(((FetchExpression)q).Query)))
               .Returns(page1, page2);

        await service.Queryable<Account>().ToListAsync();

        capturedQueries.Should().HaveCount(2);
        var secondPage = XDocument.Parse(capturedQueries[1]);
        secondPage.Root!.Attribute("paging-cookie")!.Value.Should().Be(cookie);
        secondPage.Root!.Attribute("page")!.Value.Should().Be("2");
    }

    // -------------------------------------------------------------------------
    // Column selection
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithColumns_EmitsAttributeElementsInsteadOfAllAttributes()
    {
        FetchExpression? captured = null;
        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Do<QueryBase>(q => captured = q as FetchExpression))
               .Returns(SinglePage());

        await service.Queryable<Account>("name", "telephone1").ToListAsync();

        var entity = XDocument.Parse(captured!.Query).Root!.Element("entity")!;
        entity.Element("all-attributes").Should().BeNull();
        var attributes = entity.Elements("attribute").Select(e => e.Attribute("name")!.Value).ToList();
        attributes.Should().Equal("name", "telephone1");
    }

    [Fact]
    public async Task ToListAsync_WithNoColumns_EmitsAllAttributes()
    {
        FetchExpression? captured = null;
        var service = Substitute.For<IOrganizationServiceAsync>();
        service.RetrieveMultipleAsync(Arg.Do<QueryBase>(q => captured = q as FetchExpression))
               .Returns(SinglePage());

        await service.Queryable<Account>().ToListAsync();

        var entity = XDocument.Parse(captured!.Query).Root!.Element("entity")!;
        entity.Element("all-attributes").Should().NotBeNull();
        entity.Elements("attribute").Should().BeEmpty();
    }

    // -------------------------------------------------------------------------
    // Guard clauses
    // -------------------------------------------------------------------------

    [Fact]
    public void Queryable_ThrowsInvalidOperationException_WhenTypeHasNoEntityLogicalNameAttribute()
    {
        var service = Substitute.For<IOrganizationServiceAsync>();

        var act = () => service.Queryable<EntityWithNoAttribute>();

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*EntityLogicalNameAttribute*");
    }

    // Helper: a type that intentionally lacks the attribute
    private class EntityWithNoAttribute : Entity { }
}

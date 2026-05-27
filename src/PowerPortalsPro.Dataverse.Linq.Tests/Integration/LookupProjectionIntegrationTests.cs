using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Integration coverage for projecting columns (especially lookups / EntityReferences)
/// from a joined entity, including through a <c>WithFirstRow</c> (cross-apply) join, and
/// for the inner <c>OrderBy</c> that defines which "first row" cross-apply keeps.
/// </summary>
public partial class LookupProjectionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    [Fact]
    public void ProjectLookupFromJoin_NotNull()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new
                       {
                           a.Name,
                           ParticipantId = c.ParentAccount,
                           LastInteractionOn = c.CreatedOn,
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ParticipantId.Should().NotBeNull();
            r.ParticipantId!.Id.Should().NotBe(Guid.Empty);
        });
    }

    [Fact]
    public void ProjectLookupFromWithFirstRowJoin_NotNull()
    {
        // Regression: a lookup projected from a WithFirstRow (cross-apply) link used to
        // come back null because cross-apply returns columns keyed by schema name.
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                                      .OrderByDescending(p => p.CreatedOn)
                                      .WithFirstRow()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new
                       {
                           a.Name,
                           ParticipantId = c.ParentAccount,
                           LastInteractionOn = c.CreatedOn,
                       }).ToList();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.ParticipantId.Should().NotBeNull();
            r.ParticipantId!.Id.Should().NotBe(Guid.Empty);
            r.LastInteractionOn.Should().NotBeNull();
        });
    }

    [Fact]
    public void WithFirstRow_WithInnerOrderBy_ExecutesAndReturnsOneRowPerParent()
    {
        // Before the fix the inner OrderBy was either dropped (silently) or, when emitted
        // inside the link, rejected by Dataverse ("MatchFirstRowUsingCrossApply doesn't
        // support order clause inside linkentity expression"). Emitted as a root-level
        // order qualified by the link alias, the ordered cross-apply executes and returns
        // exactly one contact per account. The order placement itself is pinned by
        // JoinFetchXmlTests.ToFetchXml_WithFirstRowAndOrderByDescending_*.
        var inner = (from a in Service.Queryable<CustomAccount>()
                     join c in Service.Queryable<CustomContact>()
                         on a.CustomAccountId equals c.ParentAccount.Id
                     select a.CustomAccountId).ToList();
        var distinctParents = inner.Distinct().Count();

        var firstRows = (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                                        .OrderByDescending(p => p.CreatedOn)
                                        .WithFirstRow()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select new { AccountId = a.CustomAccountId, c.CreatedOn }).ToList();

        firstRows.Should().HaveCount(distinctParents);
        firstRows.Select(r => r.AccountId).Should().OnlyHaveUniqueItems();
        firstRows.Should().AllSatisfy(r => r.CreatedOn.Should().NotBeNull());
    }
}

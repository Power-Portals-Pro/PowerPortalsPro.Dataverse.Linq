using FluentAssertions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// Materialization tests for <c>WithFirstRow</c> (matchfirstrowusingcrossapply) joins.
/// Cross-apply returns its linked columns merged into the root row as
/// <see cref="AliasedValue"/>s keyed by the column's <i>schema</i> name (e.g.
/// <c>new_ParentAccount</c>) rather than <c>{alias}.{logicalname}</c>. These tests pin the
/// provider's normalization so projected linked columns (especially lookups) are not null.
/// </summary>
public class CrossApplyMaterializationTests : FetchXmlTestBase
{
    [Fact]
    public void WithFirstRow_ProjectLookup_MaterializesEntityReference()
    {
        var participantRef = new EntityReference("new_customcontact", Guid.NewGuid());
        var when = new DateTime(2024, 5, 1, 0, 0, 0, DateTimeKind.Utc);

        // Mimic the cross-apply response: linked columns at the root, keyed by SCHEMA
        // name (no alias prefix), carrying logical-name metadata on the AliasedValue.
        var row = new Entity("new_customaccount", Guid.NewGuid());
        row["new_ParentAccount"] = new AliasedValue("new_customcontact", "new_parentaccount", participantRef);
        row["CreatedOn"] = new AliasedValue("new_customcontact", "createdon", when);

        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { Entities = { row }, MoreRecords = false });

        var results = (from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                                      .OrderByDescending(p => p.CreatedOn)
                                      .WithFirstRow()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new
                       {
                           ParticipantId = c.ParentAccount,
                           LastInteractionOn = c.CreatedOn,
                       }).ToList();

        results.Should().ContainSingle();
        results[0].ParticipantId.Should().NotBeNull();
        results[0].ParticipantId!.Id.Should().Be(participantRef.Id);
        results[0].LastInteractionOn.Should().Be(when);
    }

    [Fact]
    public void WithFirstRow_ProjectWholeInnerEntity_MaterializesFromSchemaNamedAliases()
    {
        var participantRef = new EntityReference("new_customaccount", Guid.NewGuid());

        var row = new Entity("new_customaccount", Guid.NewGuid());
        row["new_ParentAccount"] = new AliasedValue("new_customcontact", "new_parentaccount", participantRef);
        row["new_FirstName"] = new AliasedValue("new_customcontact", "new_firstname", "Ada");

        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { Entities = { row }, MoreRecords = false });

        var results = (from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>().WithFirstRow()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new { Contact = c }).ToList();

        results.Should().ContainSingle();
        results[0].Contact.Should().NotBeNull();
        results[0].Contact.ParentAccount!.Id.Should().Be(participantRef.Id);
        results[0].Contact.FirstName.Should().Be("Ada");
    }

    [Fact]
    public void NormalInnerJoin_ProjectLookup_StillMaterializesFromAliasPrefixedValues()
    {
        // A normal inner join returns linked columns keyed "{alias}.{logicalname}".
        // The cross-apply normalization must not disturb this path.
        var ownerRef = new EntityReference("systemuser", Guid.NewGuid());

        var row = new Entity("new_customaccount", Guid.NewGuid());
        row["c.ownerid"] = new AliasedValue("new_customcontact", "ownerid", ownerRef);

        _service.RetrieveMultiple(Arg.Any<QueryBase>())
            .Returns(new EntityCollection { Entities = { row }, MoreRecords = false });

        var results = (from a in _service.Queryable<CustomAccount>()
                       join c in _service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new { Owner = c.Owner }).ToList();

        results.Should().ContainSingle();
        results[0].Owner.Should().NotBeNull();
        results[0].Owner!.Id.Should().Be(ownerRef.Id);
    }
}

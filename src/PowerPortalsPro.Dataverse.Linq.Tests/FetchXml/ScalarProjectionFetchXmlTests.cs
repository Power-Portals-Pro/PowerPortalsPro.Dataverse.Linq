using FluentAssertions;
using Microsoft.Xrm.Sdk;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// A select reads only the attributes its projection uses, whatever the projection's shape:
/// a single value as the whole body (<c>GetAttributeValue</c>, the indexer, <c>Id</c>), or
/// values nested in a method call, operator or conditional. Distinct then de-duplicates
/// those attributes on the server.
/// </summary>
public class ScalarProjectionFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Late-bound — a single value as the whole projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValue_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.GetAttributeValue<string>("name"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValueDistinct_GeneratesDistinctOnSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Where(a => a.GetAttributeValue<string>("name") != null)
            .Select(a => a.GetAttributeValue<string>("name"))
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="name" />
                <filter type="and">
                  <condition attribute="name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectIndexer_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a["name"])
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectIndexerInAnonymousType_GeneratesAttribute()
    {
        var column = "name";
        var fetchXml = _service.Queryable("account")
            .Select(a => new { Name = (string)a[column], Parent = a["parentaccountid"] })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="name" />
                <attribute name="parentaccountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectNullableId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => (Guid?)a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectActivityId_GeneratesPrimaryKeyFromMetadata()
    {
        var fetchXml = _service.Queryable("task")
            .Select(t => t.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="task">
                <attribute name="activityid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectId_MetadataUnavailable_FallsBackToLogicalNameId()
    {
        // A service that can't answer the metadata request (a stub, or a caller without the
        // privilege) still gets a query, keyed on {logicalname}id.
        var service = Substitute.For<IOrganizationService>();
        service.Execute(Arg.Any<OrganizationRequest>()).Returns(_ => throw new InvalidOperationException("No metadata."));

        var fetchXml = service.Queryable("new_widget")
            .Select(w => w.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_widget">
                <attribute name="new_widgetid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectLookupId_GeneratesLookupAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => a.GetAttributeValue<EntityReference>("parentaccountid").Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="parentaccountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectAnonymousWithId_GeneratesEachAttributeOnce()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => new
            {
                a.Id,
                Name = a.GetAttributeValue<string>("name"),
                Upper = a.GetAttributeValue<string>("name").ToUpper(),
            })
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="account">
                <attribute name="accountid" />
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Early-bound
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectGetAttributeValue_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.GetAttributeValue<string>("new_name"))
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectProperty_Distinct_GeneratesSingleAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectId_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Values the projection computes from attributes
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectMethodOnProperty_GeneratesItsAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Website.ToUpper())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectComputedMembers_GeneratesEveryAttributeRead()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new
            {
                a.Name,
                Website = a.Website.ToUpper(),
                Employees = a.NumberOfEmployees + 1,
                ParentName = a.ParentAccount.Name,
            })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <attribute name="new_numberofemployees" />
                <attribute name="new_parentaccount" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectLogicalName_NeedsNoAttribute()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, LogicalName = ((Entity)a).LogicalName })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Projections that need the whole entity
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_TypedSelectWholeEntityBesideProperty_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Account = a })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectAttributesCollection_GeneratesAllAttributes()
    {
        var fetchXml = _service.Queryable("account")
            .Select(a => new { Name = a.GetAttributeValue<string>("name"), a.Attributes.Count })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Terminal operators over a single-value projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_UnboundSelectNullableIdFirstOrDefault_GeneratesPrimaryKeyAttribute()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(r => (Guid?)r.Id).FirstOrDefault());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="1">
              <entity name="account">
                <attribute name="accountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectGetAttributeValueMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(a => a.GetAttributeValue<int?>("numberofemployees")).Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="account">
                <attribute name="numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_TypedSelectComputedValueMax_Throws()
    {
        // Max of the column would drop the + 1; the provider can't aggregate a computed value.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.NumberOfEmployees + 1).Max());

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // A select directly on another: folded into one
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectThenSelect_GeneratesTheAttributesTheFirstExposes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Select(x => x.Name.ToUpper())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenIdentitySelectMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(a => a.GetAttributeValue<int?>("numberofemployees")).Select(n => n).Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="account">
                <attribute name="numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenComputedSelectSum_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.NumberOfEmployees).Select(n => n + 1).Sum());

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // A select composed on an earlier projection
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectAfterWhereOnProjection_GeneratesEachAttributeOnce()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name, Website = a.Website })
            .Where(a => a.Name != null)
            .Select(a => new { a.Name, a.Website })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectOverValuesAcrossTake_KeepsTheEarlierProjectionsAttributes()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Take(5)
            .Select(n => n.ToUpper())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Selects not folded: the second runs on the first one's results
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectThenSelectOfRowAggregate_KeepsTheRowAggregate()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .Select(x => x.Children)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" alias="children" rowaggregate="CountChildren" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectUsingAComputedValueTwice_GeneratesTheFirstsAttributes()
    {
        // Folding would call Guid.NewGuid() once per use of x.Key.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Key = Guid.NewGuid() })
            .Select(x => new { A = x.Key, B = x.Key })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectUsingAComputedValueInANestedLambda_GeneratesTheFirstsAttributes()
    {
        // Folding would call Guid.NewGuid() each time the nested lambda runs.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Key = Guid.NewGuid() })
            .Select(x => Enumerable.Range(0, 3).Select(i => x.Key).ToList())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectNotFoldedOverAnEntityProjection_GeneratesTheFirstsAttributes()
    {
        // Two uses of a constructed entity can't be folded; the second select runs on the first's
        // results rather than re-reading the row.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .Select(x => x.Name + x.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_EntitySelectAfterASelectOnResults_RunsOnTheResults()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { Upper = a.Name.ToUpper() })
            .Take(5)
            .Select(x => new CustomAccount { Name = x.Upper })
            .Take(3)
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="3">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectUsingEachComputedMemberOnce_FoldsToTheMembersRead()
    {
        // Each computed member is used once after the members are reduced, so the selects fold
        // and the description, which nothing reads, isn't requested.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Website = a.Website.ToUpper(), a.Description })
            .Select(x => x.Name + x.Website)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectUsingTheFirstsObjectTwice_GeneratesTheFirstsAttributes()
    {
        // Folding would make two objects where the select made one.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Select(x => new { First = x, Second = x })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OrderByAfterASelectOnResults_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Website)
            .Take(5)
            .Select(w => new CustomAccount { Name = w })
            .OrderBy(x => x.Name)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereAfterASelectOnResults_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Website)
            .Take(5)
            .Select(w => new CustomAccount { Name = w })
            .Where(x => x.Name != null)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctAfterASelectOnResults_Throws()
    {
        // The server would de-duplicate name and website, not the names the last select keeps.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Take(5)
            .Select(x => x.Name)
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_OnAfterMaterializeBeforeASelectOnResults_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .OnAfterMaterialize((e, n) => n.Trim())
            .Take(5)
            .Select(n => n.Length)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectThenIdentitySelectOfRowAggregate_Folds()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .Select(x => x)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" alias="children" rowaggregate="CountChildren" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectReachingNestedWorkTwice_DoesNotFold()
    {
        // Folding would call Guid.NewGuid() for x.Inner and again for x.Inner.Key; the selects
        // read no column either way.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { Inner = new { Key = Guid.NewGuid() } })
            .Select(x => new { x.Inner, x.Inner.Key })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount" />
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_AggregateOfASelectComposedAcrossTake_Throws()
    {
        // The last select re-reads the row, so its value isn't the first projection's + 1.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q
                .Select(a => new CustomAccount { NumberOfEmployees = a.NumberOfEmployees + 1 })
                .Take(5)
                .Select(x => x.NumberOfEmployees)
                .Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectOnGroupedSelect_RunsOnTheGroups()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        group a by a.AccountRating_OptionSetValue.Value into g
                        select new { Rating = g.Key, Count = g.Count() })
                       .Select(x => x.Count)
                       .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_accountrating" alias="rating" groupby="true" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OnAfterMaterializeBeforeALaterSelect_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name })
            .OnAfterMaterialize((e, x) => x)
            .Where(x => x.Name != null)
            .Select(x => x.Name)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_JoinAfterASelectOnResults_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Take(5)
            .Select(n => n.ToUpper())
            .Join(_service.Queryable<CustomContact>(), n => n, c => c.LastName, (n, c) => c.FirstName)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectReadingTwoMembersOfAnEntity_FoldsAndKeepsComposing()
    {
        // The object is never used whole, only its members, so making it twice is harmless; the
        // selects fold and the Where after them is translated.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name, Website = a.Website })
            .Select(x => new CustomAccount { Name = x.Name, Website = x.Website })
            .Where(x => x.Name != null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_website" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectOfAMemberComputedInALambda_Folds()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Website, Digits = a.Name.Count(ch => char.IsDigit(ch)) })
            .Select(x => x.Digits)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OnAfterMaterializeBeforeASelectOfTheSameType_Translates()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .OnAfterMaterialize((e, a) => a)
            .Select(a => new CustomAccount { Name = a.Name })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_AggregateOfASelectComposedOnAnIdentitySelect_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q
                .Select(a => a)
                .Where(a => a.Name != null)
                .Select(a => a.NumberOfEmployees)
                .Sum());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="sum" aggregate="sum" />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectAsCastSum_Throws()
    {
        // A Money column "as decimal?" is null, not the column's amount.
        var act = () => _service.Queryable("account")
            .ToFetchXml(q => q.Select(a => a["revenue"] as decimal?).Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectThenSelectReadingTwoMembersOfAClass_DoesNotFold()
    {
        // A class's constructor may do work of its own (Stamp takes a new Guid), so it isn't run
        // once per member read; the second select runs on the first's results.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new Stamp(a.Name))
            .Select(s => new { s.Id, Again = s.Id })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    /// <summary>A value whose constructor does work of its own.</summary>
    public sealed class Stamp(string name)
    {
        public string Name { get; } = name;

        public Guid Id { get; } = Guid.NewGuid();
    }

    [Fact]
    public void ToFetchXml_SelectCastToFloatMax_Throws()
    {
        // A decimal beyond float's range would be infinity, not the column's value.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (float?)a.CreditLimitMoney.Value).Max());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectDoubleToDecimalSum_Throws()
    {
        // A double to decimal can throw for a value decimal can't hold.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (decimal?)a.Latitude).Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectWideningCastSum_Throws()
    {
        // Summing the int column on the server could overflow where a sum of longs wouldn't.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (long?)a.NumberOfEmployees).Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectDistinctSum_Throws()
    {
        // The server would sum every row, not each distinct value once.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.NumberOfEmployees).Distinct().Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectDistinctMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.NumberOfEmployees).Distinct().Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectAsAnotherEntityTypeAcrossTake_RunsOnTheResults()
    {
        // A contact made from an account can't be read off the account row.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new CustomContact { LastName = a.Name })
            .Take(5)
            .Select(c => c.LastName)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_CountChildrenInASelectOnResults_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => new { Account = a })
            .Take(5)
            .Select(x => x.Account.CountChildren())
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectAfterATransformingSelectAcrossTake_RunsOnTheResults()
    {
        // The server returns the top 5 names; the first select upper-cases them and the second
        // reads them off its results, so nothing new is requested.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .Take(5)
            .Select(x => x.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" top="5">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_OnAfterMaterializeBetweenSelectsOfTheSameType_Throws()
    {
        // The hook would run after the later select, not between them where it was placed.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .OnAfterMaterialize((e, n) => n + "!")
            .Take(5)
            .Select(n => n.Substring(0, 1))
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_CountAfterASelectOnResults_Throws()
    {
        // The server would count every row, not the five the Take kept.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.Name).Take(5).Select(n => n.Length).Count());

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Distinct over a select's values
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_DistinctOverAComputedValue_Throws()
    {
        // Dataverse would de-duplicate the names, not their initials.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Name.Substring(0, 1))
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctOverAComputedValueBesideItsColumn_Translates()
    {
        // The name is in the result too, so distinct names are distinct results.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Initial = a.Name.Substring(0, 1) })
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_DistinctOverANarrowingCast_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => (int)a.CreditLimitMoney.Value)
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctOverAWholeEntity_Translates()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => new { Account = a })
            .Distinct()
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <all-attributes />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectDistinctCount_CountsTheDistinctRows()
    {
        // Counted from the distinct rows returned, not as a count of every row.
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.Name).Distinct().Count());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_DistinctThenSelect_KeepsEachEntitysRow()
    {
        // Every entity is a distinct row; its key keeps two accounts with one name apart.
        var fetchXml = _service.Queryable<CustomAccount>()
            .Distinct()
            .Select(a => a.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <attribute name="new_customaccountid" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_DistinctThenSelectCount_CountsTheEntities()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Distinct().Select(a => a.Name).Count());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" distinct="true" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_DistinctOverAConstant_Throws()
    {
        // Dataverse would return one row per account, not the one value.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => 1)
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctOverAConstructedClass_Throws()
    {
        // A constructor may change what it's given, so its object isn't known to hold the column.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => new Stamp(a.Name))
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctAfterTake_Throws()
    {
        // Dataverse removes duplicates before it takes the first rows.
        var act = () => _service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Take(10)
            .Distinct()
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_DistinctThenJoin_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Distinct()
            .Join(_service.Queryable<CustomContact>(), a => a.CustomAccountId, c => c.ParentAccount.Id,
                (a, c) => new { a.Name, c.LastName })
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectDistinctCountWithPredicate_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => a.Name).Distinct().Count(n => n != null));

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_IndexedSelect_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Select((a, i) => a.Name + i)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_ThreeSelects_FoldIntoOne()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Select(a => a)
            .Select(a => new { a.Name, a.Website })
            .Select(x => x.Name)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectToWiderTypeThenSelect_Translates()
    {
        var fetchXml = _service.Queryable("account")
            .Select<Entity, object>(a => a.GetAttributeValue<string>("name"))
            .Select(o => o)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <attribute name="name" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Aggregates of a select whose value isn't a root column's
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectNumericCastSum_Throws()
    {
        // Summing the decimal column would skip each value's truncation to int.
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (int)a.CreditLimitMoney.Value).Sum());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_SelectUserDefinedConversionMax_Throws()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (Headcount)a.NumberOfEmployees).Max());

        act.Should().Throw<NotSupportedException>();
    }

    /// <summary>A value made from a column by a conversion of its own.</summary>
    public readonly record struct Headcount(int? Count)
    {
        public static explicit operator Headcount(int? count) => new(count);
    }

    [Fact]
    public void ToFetchXml_UnboundSelectUnboxedIndexerMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable("account")
            .ToFetchXml(q => q.Select(a => (int?)a["numberofemployees"]).Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="account">
                <attribute name="numberofemployees" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_SelectNullableLiftMax_GeneratesAggregate()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .ToFetchXml(q => q.Select(a => (decimal?)a.CreditLimitMoney.Value).Max());

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_creditlimit" alias="max" aggregate="max" />
              </entity>
            </fetch>
            """);
    }
}

using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Selects of a single value (<c>GetAttributeValue</c>, the indexer, <c>Id</c>) and of values
/// computed from attributes. The seeded data has 500 contacts whose last names are
/// <c>Last1</c> to <c>Last5</c>, and 150 accounts that each have a website.
/// </summary>
public partial class ScalarProjectionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private static readonly string[] LastNames = ["Last1", "Last2", "Last3", "Last4", "Last5"];

    [Fact]
    public void UnboundSelectGetAttributeValue_Distinct_ReturnsEachValueOnce()
    {
        var results = Service.Queryable("new_customcontact")
            .Select(c => c.GetAttributeValue<string>("new_lastname"))
            .Distinct()
            .ToList();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public void UnboundSelectId_ReturnsEveryRecordId()
    {
        var expected = Service.Queryable("new_customaccount", "new_name").ToList().Select(a => a.Id);

        var results = Service.Queryable("new_customaccount")
            .Select(a => a.Id)
            .ToList();

        results.Should().HaveCount(150);
        results.Should().NotContain(Guid.Empty);
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void TypedSelectComputedMembers_ReturnsValuesFromEveryAttributeRead()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website, Upper = a.Website.ToUpper() })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r =>
        {
            r.Website.Should().NotBeNullOrEmpty();
            r.Upper.Should().Be(r.Website.ToUpper());
        });
    }

    [Fact]
    public void SelectThenSelect_ReturnsTheSecondProjection()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList().Select(n => n.ToUpper());

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Select(x => x.Name.ToUpper())
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectThenIdentitySelect_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList().Max(a => a.NumberOfEmployees);

        var result = Service.Queryable("new_customaccount")
            .Select(a => a.GetAttributeValue<int?>("new_numberofemployees"))
            .Select(n => n)
            .Max();

        result.Should().Be(expected);
    }

    [Fact]
    public void SelectThenComputedSelect_Sum_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .Select(n => n + 1)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectAfterWhereOnProjection_ReturnsTheProjectedValues()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => new { a.Name, a.Website }).ToList();

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name, Website = a.Website })
            .Where(a => a.Name != null)
            .Select(a => new { a.Name, a.Website })
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectOverValuesAcrossTake_RunsOnTheEarlierProjection()
    {
        var results = Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom Account"))
            .OrderBy(a => a.Name)
            .Select(a => a.Name)
            .Take(5)
            .Select(n => n.ToUpper())
            .ToList();

        results.Should().Equal(
            "CUSTOM ACCOUNT 001", "CUSTOM ACCOUNT 002", "CUSTOM ACCOUNT 003",
            "CUSTOM ACCOUNT 004", "CUSTOM ACCOUNT 005");
    }

    [Fact]
    public void SelectThenSelectOfRowAggregate_ReturnsTheCounts()
    {
        var expected = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .ToList()
            .Select(x => x.Children);

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .Select(x => x.Children)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectThenSelectUsingAComputedValueTwice_ComputesItOnce()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Key = Guid.NewGuid() })
            .Select(x => new { A = x.Key, B = x.Key })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.A.Should().Be(r.B));
    }

    [Fact]
    public void SelectThenSelectUsingAComputedValueInANestedLambda_ComputesItOnce()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Key = Guid.NewGuid() })
            .Select(x => Enumerable.Range(0, 3).Select(i => x.Key).ToList())
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(keys => keys.Distinct().Should().ContainSingle());
    }

    [Fact]
    public void SelectThenSelectNotFoldedOverAnEntityProjection_RunsOnTheFirstsResults()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList()
            .Select(n => n.ToUpper() + n.ToUpper());

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .Select(x => x.Name + x.Name)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void EntitySelectAfterASelectOnResults_RunsOnTheResults()
    {
        var results = Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom Account"))
            .OrderBy(a => a.Name)
            .Select(a => new { Upper = a.Name.ToUpper() })
            .Take(5)
            .Select(x => new CustomAccount { Name = x.Upper })
            .Take(3)
            .Select(a => a.Name)
            .ToList();

        results.Should().Equal("CUSTOM ACCOUNT 001", "CUSTOM ACCOUNT 002", "CUSTOM ACCOUNT 003");
    }

    [Fact]
    public void SelectThenSelectUsingEachComputedMemberOnce_ReturnsTheValues()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => new { a.Name, a.Website }).ToList()
            .Select(a => a.Name + a.Website.ToUpper());

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Website = a.Website.ToUpper(), a.Description })
            .Select(x => x.Name + x.Website)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectThenSelectUsingTheFirstsObjectTwice_KeepsOneObject()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Select(x => new { First = x, Second = x })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => ReferenceEquals(r.First, r.Second).Should().BeTrue());
    }

    [Fact]
    public void OrderByAfterASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.Website)
            .Take(5)
            .Select(w => new CustomAccount { Name = w })
            .OrderBy(x => x.Name)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectUserDefinedConversion_Max_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => (FetchXml.ScalarProjectionFetchXmlTests.Headcount)a.NumberOfEmployees)
            .Max();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void DistinctAfterASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, a.Website })
            .Take(5)
            .Select(x => x.Name)
            .Distinct()
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void OnAfterMaterializeBeforeASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .OnAfterMaterialize((e, n) => n.Trim())
            .Take(5)
            .Select(n => n.Length)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectThenIdentitySelectOfRowAggregate_ReturnsTheCounts()
    {
        var expected = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .ToList();

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Name, Children = a.CountChildren() })
            .Select(x => x)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectThenSelectReachingNestedWorkTwice_ComputesItOnce()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { Inner = new { Key = Guid.NewGuid() } })
            .Select(x => new { x.Inner, x.Inner.Key })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Inner.Key.Should().Be(r.Key));
    }

    [Fact]
    public void AggregateOfASelectComposedAcrossTake_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { NumberOfEmployees = a.NumberOfEmployees + 1 })
            .Take(5)
            .Select(x => x.NumberOfEmployees)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectOnGroupedSelect_RunsOnTheGroups()
    {
        var expected = (from a in Service.Queryable<CustomAccount>()
                        group a by a.AccountRating_OptionSetValue.Value into g
                        select new { Rating = g.Key, Count = g.Count() }).ToList()
                       .Select(x => x.Count);

        var results = (from a in Service.Queryable<CustomAccount>()
                       group a by a.AccountRating_OptionSetValue.Value into g
                       select new { Rating = g.Key, Count = g.Count() })
                      .Select(x => x.Count)
                      .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void OnAfterMaterializeBeforeALaterSelect_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name })
            .OnAfterMaterialize((e, x) => x)
            .Where(x => x.Name != null)
            .Select(x => x.Name)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void JoinAfterASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Take(5)
            .Select(n => n.ToUpper())
            .Join(Service.Queryable<CustomContact>(), n => n, c => c.LastName, (n, c) => c.FirstName)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectThenSelectReadingTwoMembersOfAnEntity_FoldsAndKeepsComposing()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => new { a.Name, a.Website }).ToList();

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name, Website = a.Website })
            .Select(x => new CustomAccount { Name = x.Name, Website = x.Website })
            .Where(x => x.Name != null)
            .ToList();

        results.Select(r => new { r.Name, r.Website }).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectThenSelectOfAMemberComputedInALambda_ReturnsTheValues()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList()
            .Select(n => n.Count(ch => char.IsDigit(ch)));

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new { a.Website, Digits = a.Name.Count(ch => char.IsDigit(ch)) })
            .Select(x => x.Digits)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void OnAfterMaterializeBeforeASelectOfTheSameType_RunsOnTheSelectsValues()
    {
        var results = Service.Queryable<CustomAccount>()
            .OnAfterMaterialize((e, a) =>
            {
                a.Name += "!";
                return a;
            })
            .Select(a => new CustomAccount { Name = a.Name })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(a => a.Name.Should().EndWith("!"));
    }

    [Fact]
    public void AggregateOfASelectComposedOnAnIdentitySelect_ReturnsColumnSum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList()
            .Where(a => a.Name != null)
            .Sum(a => a.NumberOfEmployees);

        var result = Service.Queryable<CustomAccount>()
            .Select(a => a)
            .Where(a => a.Name != null)
            .Select(a => a.NumberOfEmployees)
            .Sum();

        result.Should().Be(expected);
    }

    [Fact]
    public void SelectAsCast_Sum_Throws()
    {
        var act = () => Service.Queryable("new_customaccount")
            .Select(a => a["new_creditlimit"] as decimal?)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectThenSelectReadingTwoMembersOfAClass_MakesTheObjectOnce()
    {
        var results = Service.Queryable<CustomAccount>()
            .Select(a => new FetchXml.ScalarProjectionFetchXmlTests.Stamp(a.Name))
            .Select(s => new { s.Id, Again = s.Id })
            .ToList();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Again.Should().Be(r.Id));
    }

    [Fact]
    public void SelectCastToFloat_Max_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => (float?)a.CreditLimitMoney.Value)
            .Max();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectDoubleToDecimal_Sum_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => (decimal?)a.Latitude)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void UnboundSelectIndexer_OfAColumnSomeRowsLack_ReturnsNullForThem()
    {
        var expected = Service.Queryable("new_customaccount")
            .Select(a => a.GetAttributeValue<string>("new_description"))
            .ToList();

        var results = Service.Queryable("new_customaccount")
            .Select(a => (string)a["new_description"])
            .ToList();

        expected.Should().Contain((string?)null, "some accounts have no description");
        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectWideningCast_Sum_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => (long?)a.NumberOfEmployees)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectDistinct_Sum_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .Distinct()
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectDistinct_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList().Max(a => a.NumberOfEmployees);

        var result = Service.Queryable<CustomAccount>()
            .Select(a => a.NumberOfEmployees)
            .Distinct()
            .Max();

        result.Should().Be(expected);
    }

    [Fact]
    public void SelectAsAnotherEntityTypeAcrossTake_RunsOnTheResults()
    {
        var results = Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom Account"))
            .OrderBy(a => a.Name)
            .Select(a => new CustomContact { LastName = a.Name })
            .Take(3)
            .Select(c => c.LastName)
            .ToList();

        results.Should().Equal("Custom Account 001", "Custom Account 002", "Custom Account 003");
    }

    [Fact]
    public void CountChildrenInASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => new { Account = a })
            .Take(5)
            .Select(x => x.Account.CountChildren())
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectAfterATransformingSelectAcrossTake_KeepsTheFirstsValues()
    {
        var results = Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom Account"))
            .OrderBy(a => a.Name)
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .Take(3)
            .Select(x => x.Name)
            .ToList();

        results.Should().Equal("CUSTOM ACCOUNT 001", "CUSTOM ACCOUNT 002", "CUSTOM ACCOUNT 003");
    }

    [Fact]
    public void SelectAfterATransformingSelectAcrossWhere_KeepsTheFirstsValues()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList()
            .Select(n => n.ToUpper());

        var results = Service.Queryable<CustomAccount>()
            .Select(a => new CustomAccount { Name = a.Name.ToUpper() })
            .Where(x => x.Name != null)
            .Select(x => x.Name)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void OnAfterMaterializeBetweenSelectsOfTheSameType_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .OnAfterMaterialize((e, n) => n + "!")
            .Take(5)
            .Select(n => n.Substring(0, 1))
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void CountAfterASelectOnResults_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => a.Name)
            .Take(5)
            .Select(n => n.Length)
            .Count();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void IndexedSelect_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select((a, i) => a.Name + i)
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void UnboundSelectUnboxedIndexer_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList().Max(a => a.NumberOfEmployees);

        var result = Service.Queryable("new_customaccount")
            .Select(a => (int?)a["new_numberofemployees"])
            .Max();

        result.Should().Be(expected);
    }

    [Fact]
    public void ThreeSelects_ReturnTheLastProjection()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList();

        var results = Service.Queryable<CustomAccount>()
            .Select(a => a)
            .Select(a => new { a.Name, a.Website })
            .Select(x => x.Name)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectToWiderTypeThenSelect_ReturnsTheValues()
    {
        var expected = Service.Queryable<CustomAccount>().Select(a => a.Name).ToList();

        var results = Service.Queryable("new_customaccount")
            .Select<Entity, object>(a => a.GetAttributeValue<string>("new_name"))
            .Select(o => o)
            .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void SelectNumericCast_Sum_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Select(a => (int)a.CreditLimitMoney.Value)
            .Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectNullableLift_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList()
            .Max(a => (decimal?)a.CreditLimitMoney?.Value);

        var result = Service.Queryable<CustomAccount>()
            .Select(a => (decimal?)a.CreditLimitMoney.Value)
            .Max();

        result.Should().Be(expected);
    }

    [Fact]
    public void UnboundSelectGetAttributeValue_Max_ReturnsColumnMaximum()
    {
        var expected = Service.Queryable<CustomAccount>().ToList().Max(a => a.NumberOfEmployees);

        var result = Service.Queryable("new_customaccount")
            .Select(a => a.GetAttributeValue<int?>("new_numberofemployees"))
            .Max();

        result.Should().Be(expected);
    }
}

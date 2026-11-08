using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// Selects after a join that compute values from columns, use an entity whole beside one of its
/// columns, or read a single value. The seeded data has 100 accounts with 5 contacts each (last
/// names <c>Last1</c> to <c>Last5</c>) and 50 accounts with none; every account has a website.
/// </summary>
public partial class JoinProjectionIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private static readonly string[] LastNames = ["Last1", "Last2", "Last3", "Last4", "Last5"];

    [Fact]
    public void JoinAfterDistinct_Throws()
    {
        // The join's columns would replace the ones Distinct() de-duplicated.
        var act = () => Service.Queryable<CustomAccount>()
            .Distinct()
            .Join(Service.Queryable<CustomContact>(), a => a.CustomAccountId, c => c.ParentAccount.Id,
                (a, c) => new { a.Name, c.LastName })
            .ToList();

        act.Should().Throw<NotSupportedException>().WithMessage("*'Join' after Distinct()*");
    }

    [Fact]
    public void SelectAfterDistinctOnAJoin_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Join(Service.Queryable<CustomContact>(), a => a.CustomAccountId, c => c.ParentAccount.Id,
                (a, c) => new { a, c })
            .Distinct()
            .Select(x => x.a.Name)
            .ToList();

        act.Should().Throw<NotSupportedException>().WithMessage("*Select after Distinct() on a join*");
    }

    [Fact]
    public void JoinSelectLinkColumn_Distinct_ReturnsEachValueOnce()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select c.LastName).Distinct().ToList();

        results.Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public void JoinSelectComputedRootMember_ReturnsComputedValues()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new { a.Website, WebsiteLength = a.Website.Length }).ToList();

        results.Should().HaveCount(500);
        results.Should().AllSatisfy(r => r.WebsiteLength.Should().Be(r.Website.Length));
    }

    [Fact]
    public void JoinSelectRootColumn_Sum_SumsOverJoinedRows()
    {
        var expected = (from a in Service.Queryable<CustomAccount>()
                        join c in Service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.NumberOfEmployees }).ToList().Sum(r => r.NumberOfEmployees);

        var result = (from a in Service.Queryable<CustomAccount>()
                      join c in Service.Queryable<CustomContact>()
                          on a.CustomAccountId equals c.ParentAccount.Id
                      select a.NumberOfEmployees).Sum();

        result.Should().Be(expected);
    }

    [Fact]
    public void JoinSelectComputedValue_Sum_Throws()
    {
        // Summing the column would drop the + 1.
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select a.NumberOfEmployees + 1).Sum();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void JoinSelectThenSelect_ReturnsTheSecondProjection()
    {
        var expected = (from a in Service.Queryable<CustomAccount>()
                        join c in Service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        select new { a.Name }).ToList()
                       .Select(x => x.Name.ToUpper());

        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name != null
                       select new { a.Name, c.LastName })
                      .Select(x => x.Name.ToUpper())
                      .ToList();

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void JoinSelectLinkColumn_DistinctCount_CountsTheValues()
    {
        var count = (from a in Service.Queryable<CustomAccount>()
                     join c in Service.Queryable<CustomContact>()
                         on a.CustomAccountId equals c.ParentAccount.Id
                     select c.LastName).Distinct().Count();

        count.Should().Be(5);
    }

    [Fact]
    public void LeftJoinDistinctOverANullTest_Throws()
    {
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id into contacts
                         from c in contacts.DefaultIfEmpty()
                         select new { a.Name, HasContact = c != null }).Distinct().ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void JoinDistinctOverAComputedValue_Throws()
    {
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select c.FirstName + " " + c.LastName).Distinct().ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void SelectOverJoinProjectionAcrossTake_RunsOnTheResults()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id
                       select new { a.Name, c.LastName })
                      .Take(5)
                      .Select(x => x.LastName)
                      .ToList();

        results.Should().HaveCount(5);
        results.Should().OnlyContain(lastName => LastNames.Contains(lastName));
    }

    [Fact]
    public void JoinSelectLinkColumn_Max_Throws()
    {
        var act = () => (from a in Service.Queryable<CustomAccount>()
                         join c in Service.Queryable<CustomContact>()
                             on a.CustomAccountId equals c.ParentAccount.Id
                         select c.LastName).Max();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void UnboundLeftJoinSelectIndexer_Distinct_DeduplicatesTheValuesRead()
    {
        // Each account's 5 contacts share its first name, so each account gives one row; the
        // 50 accounts without contacts give one row each, with a null name.
        var results = (from a in Service.Queryable("new_customaccount")
                       join c in Service.Queryable("new_customcontact")
                           on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { AccountId = a.Id, FirstName = (string)c["new_firstname"] }).Distinct().ToList();

        results.Should().HaveCount(150).And.OnlyHaveUniqueItems();
        results.Count(r => r.FirstName is null).Should().Be(50);
    }

    [Fact]
    public void UnboundLeftJoinSelectIndexer_ReturnsMatchedValues()
    {
        var results = (from a in Service.Queryable("new_customaccount")
                       join c in Service.Queryable("new_customcontact")
                           on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { AccountId = a.Id, LastName = c == null ? null : (string)c["new_lastname"] }).ToList();

        results.Should().HaveCount(550);
        results.Count(r => r.LastName is null).Should().Be(50);
        results.Where(r => r.LastName is not null).Select(r => r.LastName).Distinct()
            .Should().BeEquivalentTo(LastNames);
    }

    [Fact]
    public void LeftJoinSelectWholeRootBesideColumn_ReturnsCompleteEntity()
    {
        var results = (from a in Service.Queryable<CustomAccount>()
                       join c in Service.Queryable<CustomContact>()
                           on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { a.Name, Account = a }).ToList();

        results.Should().HaveCount(550);
        results.Should().AllSatisfy(r =>
        {
            r.Account.Name.Should().Be(r.Name);
            r.Account.Website.Should().NotBeNullOrEmpty();
        });
    }
}

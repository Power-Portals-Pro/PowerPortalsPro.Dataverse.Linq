using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

/// <summary>
/// An operator after a select reads its lambda through the select: a member copied from a
/// column filters, orders or aggregates that column, and a computed member throws. The seeded
/// data has 100 "Custom Account NNN" rows, with a description, a credit limit of NNN * 1000 + 0.50
/// and NNN * 10 employees (each missing on some rows), and 50 "Empty Account NNN" rows.
/// </summary>
public partial class OperatorAfterSelectIntegrationTests(ServiceClientFixture fixture) : IntegrationTestBase(fixture)
{
#if !NETFRAMEWORK
    private IOrganizationServiceAsync Service => ServiceProvider.GetRequiredService<IOrganizationServiceAsync>();
#else
    private IOrganizationService Service => ServiceProvider.GetRequiredService<IOrganizationService>();
#endif

    private IQueryable<CustomAccount> Accounts => Service.Queryable<CustomAccount>();

    private IQueryable<CustomContact> Contacts => Service.Queryable<CustomContact>();

    [Fact]
    public void WhereOnAnonymousMember_ReturnsTheMatch()
    {
        var results = Accounts
            .Select(a => new { a.Name, a.Website })
            .Where(x => x.Name == "Custom Account 001")
            .ToList();

        results.Should().ContainSingle()
            .Which.Website.Should().Be("https://account001.example.com");
    }

    [Fact]
    public void WhereOnSelectedValue_ReturnsTheMatches()
    {
        var results = Accounts
            .Select(a => a.Name)
            .Where(name => name.StartsWith("Empty Account"))
            .ToList();

        results.Should().HaveCount(50);
        results.Should().AllSatisfy(name => name.Should().StartWith("Empty Account"));
    }

    [Fact]
    public void WhereOnEntityMemberCopiedFromAnotherColumn_FiltersTheSourceColumn()
    {
        var results = Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => x.Name == "Description for account 001")
            .ToList();

        results.Should().ContainSingle()
            .Which.Name.Should().Be("Description for account 001");
    }

    [Fact]
    public void WhereOnIdOfEntityWhoseKeyTheSelectSets_ReturnsTheRecord()
    {
        var id = Accounts.Where(a => a.Name == "Custom Account 002").Select(a => a.CustomAccountId).First();

        var results = Accounts
            .Select(a => new CustomAccount { CustomAccountId = a.CustomAccountId, Name = a.Name })
            .Where(x => x.Id == id)
            .ToList();

        results.Should().ContainSingle()
            .Which.Name.Should().Be("Custom Account 002");
    }

    [Fact]
    public void WhereOnColumnReadByNameOffTheSelectedEntity_FiltersTheSourceColumn()
    {
        var results = Accounts
            .Select(a => new CustomAccount { Name = a.Description })
            .Where(x => (string)x["new_name"] == "Description for account 003")
            .ToList();

        results.Should().ContainSingle()
            .Which.Name.Should().Be("Description for account 003");
    }

    [Fact]
    public void WhereThroughLet_ReturnsTheMatch()
    {
        var results = (from a in Accounts
                       let name = a.Name
                       where name == "Custom Account 001"
                       select a).ToList();

        results.Should().ContainSingle()
            .Which.Website.Should().Be("https://account001.example.com");
    }

    [Fact]
    public void WhereOnComputedEntityMember_Throws()
    {
        // LINQ would match Custom Account 001; a condition on new_name would match nothing.
        var act = () => Accounts
            .Select(a => new CustomAccount { Name = "x" + a.Name })
            .Where(x => x.Name == "xCustom Account 001")
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void WhereTruncatingCast_Throws()
    {
        // (int?)1000.50 is 1000, which a condition on the credit limit wouldn't match.
        var act = () => Accounts.Where(a => (int?)a.CreditLimit == 1000).ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void WhereWideningCast_MatchesTheUncastCondition()
    {
        var expected = Accounts.Where(a => a.NumberOfEmployees > 500).Count();

        var count = Accounts.Where(a => (long?)a.NumberOfEmployees > 500L).Count();

        count.Should().Be(expected).And.BeGreaterThan(0);
    }

    [Fact]
    public void OrderByAnonymousMember_OrdersByItsColumn()
    {
        var expected = Accounts.OrderByDescending(a => a.Name).Select(a => a.Name).ToList();

        var results = Accounts
            .Select(a => new { a.Name })
            .OrderByDescending(x => x.Name)
            .ToList();

        results.Select(r => r.Name).Should().Equal(expected);
    }

    [Fact]
    public void FirstOrDefaultWithPredicateOnAnonymousMember_ReturnsTheMatch()
    {
        var result = Accounts
            .Select(a => new { a.Name, a.Website })
            .FirstOrDefault(x => x.Name == "Empty Account 001");

        result.Should().NotBeNull();
        result!.Website.Should().Be("https://empty001.example.com");
    }

    [Fact]
    public void CountWithPredicateOnAnonymousMember_CountsTheMatches()
    {
        var count = Accounts
            .Select(a => new { a.Name })
            .Count(x => x.Name.StartsWith("Custom Account"));

        count.Should().Be(100);
    }

    [Fact]
    public void MaxOfAnonymousMember_MatchesMaxOfTheColumn()
    {
        var expected = Accounts.Max(a => a.NumberOfEmployees);

        var result = Accounts
            .Select(a => new { a.NumberOfEmployees })
            .Max(x => x.NumberOfEmployees);

        result.Should().Be(expected).And.NotBeNull();
    }

    [Fact]
    public void WhereOnJoinSelectMember_FiltersTheJoinedColumn()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                       select new { a.Name, c.LastName })
            .Where(x => x.LastName == "Last1")
            .ToList();

        results.Should().HaveCount(100);
        results.Should().AllSatisfy(r => r.LastName.Should().Be("Last1"));
    }

    [Fact]
    public void OrderByJoinSelectMember_OrdersByTheJoinedColumn()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                       where a.Name == "Custom Account 001"
                       select new { a.Name, Surname = c.LastName })
            .OrderByDescending(x => x.Surname)
            .ToList();

        results.Select(r => r.Surname).Should().Equal("Last5", "Last4", "Last3", "Last2", "Last1");
    }

    [Fact]
    public void WhereOnLeftJoinSelectedEntities_FiltersEachEntity()
    {
        var results = (from a in Accounts
                       join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                       from c in contacts.DefaultIfEmpty()
                       select new { a, c })
            .Where(x => x.a.Name == "Custom Account 001" && x.c.LastName != "Last1")
            .ToList();

        results.Should().HaveCount(4);
        results.Should().AllSatisfy(r =>
        {
            r.a.Name.Should().Be("Custom Account 001");
            r.c.LastName.Should().NotBe("Last1");
        });
    }

    [Fact]
    public void WhereOnComputedJoinSelectMember_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         select new { FullName = c.FirstName + " " + c.LastName })
            .Where(x => x.FullName == "First1 Last1")
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void OrderByAfterGroupedSelect_Throws()
    {
        var act = () => Accounts
            .GroupBy(a => a.AccountRating)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key)
            .ToList();

        act.Should().Throw<NotSupportedException>().WithMessage("*can't follow a Select on groups*");
    }

    [Fact]
    public void WhereAfterGroupedSelect_Throws()
    {
        var act = () => Accounts
            .GroupBy(a => a.AccountRating)
            .Select(g => new { g.Key, Count = g.Count() })
            .Where(x => x.Count > 1)
            .ToList();

        act.Should().Throw<NotSupportedException>().WithMessage("*can't follow a Select on groups*");
    }

    [Fact]
    public void JoinAfterSelectMakingAnEntity_Throws()
    {
        // The join would key and read the stored name, not the upper-cased one.
        var act = () => Accounts
            .Select(a => new CustomAccount { CustomAccountId = a.CustomAccountId, Name = a.Name.ToUpper() })
            .Join(Contacts, x => x.CustomAccountId, c => c.ParentAccount.Id, (x, c) => new { x.Name, c.LastName })
            .ToList();

        act.Should().Throw<NotSupportedException>().WithMessage("*'Join' can't follow a Select that makes new values*");
    }

    // A cast that may change the value, or a value the row computes, isn't a column to compare.
    public static TheoryData<string, Func<IQueryable<CustomAccount>, object?>> ComparisonsThatChangeTheValue => new()
    {
        // (float)16777217 is 16777216, so the condition couldn't keep 16777217 apart from it; the
        // cast is rejected whatever the value. A decimal's double is only near the decimal.
        { "int to float", q => q.Where(a => (float?)a.NumberOfEmployees == 16777217f).ToList() },
        { "decimal to double", q => q.Where(a => (double?)a.CreditLimit > 1000.5).ToList() },
        { "a column to a truncated column", q => q.Where(a => (int?)a.CreditLimit == a.NumberOfEmployees).ToList() },
        { "a member to a computed member", q => q.Select(a => new { a.Name, Upper = a.Name.ToUpper() }).Where(x => x.Name == x.Upper).ToList() },
    };

    [Theory]
    [MemberData(nameof(ComparisonsThatChangeTheValue))]
    public void WhereComparingAValueTheRowChanges_Throws(string comparison, Func<IQueryable<CustomAccount>, object?> query)
    {
        var act = () => query(Accounts);

        act.Should().Throw<NotSupportedException>(comparison).WithMessage("*comparison*");
    }

    [Fact]
    public void GroupByAfterSelectMakingAnEntity_Throws()
    {
        var act = () => Accounts
            .Select(a => new CustomAccount { Name = a.Name.Substring(0, 5) })
            .GroupBy(x => x.Name)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void WhereAfterJoinSelectMakingAnEntity_Throws()
    {
        var act = () => (from a in Accounts
                         join c in Contacts on a.CustomAccountId equals c.ParentAccount.Id
                         select new CustomContact { LastName = c.LastName.ToUpper() })
            .Where(x => x.LastName == "LAST1")
            .ToList();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void MaxOfJoinedColumn_Throws()
    {
        var act = () => Accounts
            .Join(Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
            .Max(x => x.c.LastName);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void MaxOfRootColumnAfterJoin_MatchesTheJoinedAccounts()
    {
        var expected = Accounts
            .Where(a => a.Name.StartsWith("Custom Account"))
            .Max(a => a.NumberOfEmployees);

        var result = Accounts
            .Join(Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
            .Max(x => x.a.NumberOfEmployees);

        result.Should().Be(expected).And.NotBeNull();
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.Integration;

public partial class FilterIntegrationTests
{
    // -------------------------------------------------------------------------
    // Where — typed proxy
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereNameEquals_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001")
            .ToListAsync();

        results.Should().ContainSingle();
        results[0].Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task ToListAsync_WhereNameNotNull_ExcludesNulls()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        results.Should().HaveCount(150);
        results.Should().AllSatisfy(r => r.Name.Should().NotBeNull());
    }

    [Fact]
    public async Task ToListAsync_WhereDescriptionIsNull_ReturnsRecordsWithNullDescription()
    {
        var all = await Service.Queryable<CustomAccount>().ToListAsync();
        var nullDescriptions = all.Count(a => a.Description == null);

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Description == null)
            .ToListAsync();

        results.Should().HaveCount(nullDescriptions);
    }

    // -------------------------------------------------------------------------
    // Where — NotEqual (non-null)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereNotEqualToValue_ExcludesMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != "Custom Account 001")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => r.Name == "Custom Account 001");
    }

    // -------------------------------------------------------------------------
    // Where — comparison operators (lt, le, gt, ge)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereLessThan_ReturnsMatchingRecords()
    {
        // Seed: NumberOfEmployees = i * 10 for i where i % 8 != 0
        // i=1 → 10, i=2 → 20, i=3 → 30, i=4 → 40 all have < 50
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees < 50)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees < 50);
    }

    [Fact]
    public async Task ToListAsync_WhereLessEqual_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees <= 50)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees <= 50);
        // Should include i=5 (50) which < 50 would not
        results.Count.Should().BeGreaterThan(
            (await Service.Queryable<CustomAccount>()
                .Where(a => a.NumberOfEmployees < 50)
                .ToListAsync()).Count);
    }

    [Fact]
    public async Task ToListAsync_WhereGreaterThan_ReturnsMatchingRecords()
    {
        // Seed: i=97 → 970, i=98 → 980, i=99 → 990 (i=100 → 1000 but 100 % 8 == 4 so not null... check: 100%8=4 !=0, so 1000 is set)
        // > 950 should return records with 960, 970, 980, 990, 1000
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees > 950)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees > 950);
    }

    [Fact]
    public async Task ToListAsync_WhereGreaterEqual_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees >= 950)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.NumberOfEmployees >= 950);
        // Should include i=95 (950) which > 950 would not
        results.Count.Should().BeGreaterThan(
            (await Service.Queryable<CustomAccount>()
                .Where(a => a.NumberOfEmployees > 950)
                .ToListAsync()).Count);
    }

    // -------------------------------------------------------------------------
    // Where — In / NotIn
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereIn_ReturnsMatchingRecords()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002", "Custom Account 003" };
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => names.Contains(a.Name))
            .ToListAsync();

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo(names);
    }

    [Fact]
    public async Task ToListAsync_WhereNotIn_ExcludesMatchingRecords()
    {
        var excluded = new[] { "Custom Account 001", "Custom Account 002" };
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => !excluded.Contains(a.Name))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => excluded.Contains(r.Name));
    }

    [Fact]
    public async Task ToListAsync_WhereInWithGuids_ReturnsMatchingRecords()
    {
        // First get some known account IDs
        var allAccounts = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Custom Account 002" || a.Name == "Custom Account 003")
            .ToListAsync();
        var ids = allAccounts.Select(a => a.CustomAccountId).ToArray();
        ids.Should().HaveCount(3);

        // Now query by those IDs
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => ids.Contains(a.CustomAccountId))
            .ToListAsync();

        results.Should().HaveCount(3);
        results.Select(r => r.CustomAccountId).Should().BeEquivalentTo(ids);
    }

    [Fact]
    public async Task ToListAsync_WhereNotInWithGuids_ExcludesMatchingRecords()
    {
        // First get some known account IDs
        var excluded = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Custom Account 001" || a.Name == "Custom Account 002")
            .ToListAsync();
        var excludedIds = excluded.Select(a => a.CustomAccountId).ToArray();
        excludedIds.Should().HaveCount(2);

        // Now query excluding those IDs
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => !excludedIds.Contains(a.CustomAccountId))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(r => excludedIds.Contains(r.CustomAccountId));
    }

    // -------------------------------------------------------------------------
    // String methods
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WithContainsFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Account"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().Contain("Account"));
    }

    [Fact]
    public async Task ToListAsync_WithStartsWithFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Name.Should().StartWith("Custom"));
    }

    [Fact]
    public async Task ToListAsync_WithEndsWithFilter_ReturnsMatchingRecords()
    {
        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.EndsWith("001"))
            .ToListAsync();

        results.Should().HaveCount(2);
        results.Should().OnlyContain(r => r.Name.EndsWith("001"));
    }

    // -------------------------------------------------------------------------
    // String.Length
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_StringLengthEqual_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length == 50).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length == 50)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Should().HaveLength(50));
    }

    [Fact]
    public async Task Where_StringLengthNotEqual_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and non-matching strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length != 50).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length != 50)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length != 50).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthGreaterThan_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length > 75).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length > 75)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Length.Should().BeGreaterThan(75));
    }

    [Fact]
    public async Task Where_StringLengthGreaterThanOrEqual_MatchesLinq()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length >= 75).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length >= 75)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c => c.VariableLengthString.Length.Should().BeGreaterThanOrEqualTo(75));
    }

    [Fact]
    public async Task Where_StringLengthLessThan_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and short strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length < 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length < 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length < 25).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthLessThanOrEqual_IncludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        // not-like includes nulls, so expected count includes both null and short strings
        var expected = all.Where(c => c.VariableLengthString == null || c.VariableLengthString.Length <= 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString.Length <= 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
            (c.VariableLengthString == null || c.VariableLengthString.Length <= 25).Should().BeTrue());
    }

    [Fact]
    public async Task Where_StringLengthLessThanAndNotNull_ExcludesNullValues()
    {
        var all = await Service.Queryable<CustomContact>().ToListAsync();
        var expected = all.Where(c => c.VariableLengthString != null && c.VariableLengthString.Length < 25).ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.VariableLengthString != null && c.VariableLengthString.Length < 25)
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Should().AllSatisfy(c =>
        {
            c.VariableLengthString.Should().NotBeNull();
            c.VariableLengthString.Length.Should().BeLessThan(25);
        });
    }

    // -------------------------------------------------------------------------
    // Negated filter expressions
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_NegatedContains_ExcludesMatchingRecords()
    {
        var withMatch = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Account"))
            .ToListAsync();

        var withoutMatch = await Service.Queryable<CustomAccount>()
            .Where(a => !a.Name.Contains("Account"))
            .ToListAsync();

        var total = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToListAsync();

        (withMatch.Count + withoutMatch.Count).Should().Be(total.Count);
    }

    [Fact]
    public async Task Where_NotEqual_ReturnsOtherRecords()
    {
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();
        var knownName = allAccounts.First(a => a.Name != null).Name;

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => a.Name != knownName)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().NotContain(a => a.Name == knownName);
        results.Count.Should().BeLessThan(allAccounts.Count);
    }

    // -------------------------------------------------------------------------
    // Column-to-column comparison (valueof)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereSameTableColumnsEqual_ReturnsOnlyMatchingRecords()
    {
        // Seeded contacts have different FirstName and LastName values,
        // so no records should match FirstName == LastName.
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FirstName == c.LastName)
            .ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereSameTableColumnsNotEqual_ReturnsNonMatchingRecords()
    {
        // Seeded contacts have different FirstName and LastName values,
        // so all records should match FirstName != LastName.
        var allContacts = await Service.Queryable<CustomContact>().ToListAsync();
        var results = await Service.Queryable<CustomContact>()
            .Where(c => c.FirstName != c.LastName)
            .ToListAsync();

        results.Should().HaveCount(allContacts.Count);
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableColumnEqual_ReturnsOnlyMatchingRecords()
    {
        // Seeded data: Account.Name = "Custom Account 001" etc,
        // Contact.FirstName = "First001" etc — never equal.
        var results = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             where a.Name == c.FirstName
                             select new { a.Name, c.FirstName }).ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableRatingEqual_ReturnsMatchingRecords()
    {
        // Accounts and contacts both have ratings assigned from the same enum
        // with different modulus patterns, so some match and some don't.
        var matches = await (from a in Service.Queryable<CustomAccount>()
                             join c in Service.Queryable<CustomContact>()
                                 on a.CustomAccountId equals c.ParentAccount.Id
                             where a.AccountRating_OptionSetValue == c.ContactRating_OptionSetValue
                             select new { a.Name, a.AccountRating, c.FirstName, c.ContactRating }).ToListAsync();

        matches.Should().NotBeEmpty();
        matches.Should().AllSatisfy(r =>
            ((int?)r.AccountRating).Should().Be((int?)r.ContactRating));
    }

    [Fact]
    public async Task ToListAsync_WhereCrossTableRatingNotEqual_ReturnsNonMatchingRecords()
    {
        var nonMatches = await (from a in Service.Queryable<CustomAccount>()
                                join c in Service.Queryable<CustomContact>()
                                    on a.CustomAccountId equals c.ParentAccount.Id
                                where a.AccountRating_OptionSetValue != c.ContactRating_OptionSetValue
                                select new { a.Name, a.AccountRating, c.FirstName, c.ContactRating }).ToListAsync();

        nonMatches.Should().NotBeEmpty();
        nonMatches.Should().AllSatisfy(r =>
        {
            // Either side may be null (null != value is true in FetchXml)
            if (r.AccountRating is not null && r.ContactRating is not null)
                ((int)r.AccountRating).Should().NotBe((int)r.ContactRating);
        });
    }

    // -------------------------------------------------------------------------
    // Any() — link-type="any" / "not any"
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereAny_ReturnsContactsThatArePrimaryContactOfMatchingAccount()
    {
        // Find contacts that are the primary contact of an account named "Custom Account 001".
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task ToListAsync_WhereNotAny_ReturnsContactsThatAreNotPrimaryContactOfMatchingAccount()
    {
        // Find contacts that are NOT the primary contact of any account named "Custom Account 001".
        var allContacts = await Service.Queryable<CustomContact>().ToListAsync();
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => !Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        // All contacts except the one primary contact of "Custom Account 001"
        results.Should().HaveCount(allContacts.Count - 1);
    }

    [Fact]
    public async Task ToListAsync_WhereAnyWithoutFilter_ReturnsContactsThatArePrimaryContactOfAnyAccount()
    {
        // Find contacts that are the primary contact of any account.
        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId))
            .ToListAsync();

        // Each of the 100 accounts has a primary contact (the first contact per account group).
        results.Should().HaveCount(100);
    }

    // -------------------------------------------------------------------------
    // All() — link-type="all" / "not all"
    //
    // FetchXml link-type="all" only considers parent rows that have at least
    // one related child. Parents with no children are excluded (no vacuous truth).
    // All() and !All() are complementary within the set of parents that HAVE children.
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereAll_ReturnsContactsWhereAllLinkedAccountsSatisfyCondition()
    {
        // Find contacts where ALL accounts referencing them as PrimaryContact
        // have Name == "Custom Account 001". Cross-validate: find the one account
        // with that name, then find its primary contact.
        var targetAccount = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        // Only the primary contact of that one account should match
        results.Should().HaveCount(1);
        results[0].CustomContactId.Should().Be(targetAccount.PrimaryContact.Id);
    }

    [Fact]
    public async Task ToListAsync_WhereNotAll_ReturnsContactsWhereAtLeastOneLinkedAccountFails()
    {
        // !All(Name == "Custom Account 001") means: at least one linked account
        // has Name != "Custom Account 001". Cross-validate: all primary contacts
        // except the one linked to "Custom Account 001".
        var primaryContacts = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId))
            .ToListAsync();

        var allMatch = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        var results = await Service.Queryable<CustomContact>()
            .Where(contact => !Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        results.Should().HaveCount(primaryContacts.Count - allMatch.Count);
    }

    [Fact]
    public async Task ToListAsync_WhereAll_PlusNotAll_CoversContactsWithLinkedAccounts()
    {
        // All(..) and !All(..) are complementary within the set of contacts that
        // have linked accounts. Use Any() to establish the baseline.
        var primaryContacts = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId))
            .ToListAsync();

        var allMatch = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        var notAllMatch = await Service.Queryable<CustomContact>()
            .Where(contact => !Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Custom Account 001"))
            .ToListAsync();

        (allMatch.Count + notAllMatch.Count).Should().Be(primaryContacts.Count);
        allMatch.Select(c => c.CustomContactId).Should().NotIntersectWith(
            notAllMatch.Select(c => c.CustomContactId));
    }

    [Fact]
    public async Task ToListAsync_WhereAll_WithNotNullCondition_ReturnsExpectedResults()
    {
        // Find contacts where ALL accounts referencing them as PrimaryContact
        // have a non-null AccountRating. Cross-validate: primary contacts of
        // accounts that DO have a rating.
        var expectedContactIds = (await (from a in Service.Queryable<CustomAccount>()
                                         where a.PrimaryContact != null
                                               && a.AccountRating_OptionSetValue != null
                                         select a.PrimaryContact.Id).ToListAsync())
            .Distinct().ToList();

        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.AccountRating_OptionSetValue != null))
            .ToListAsync();

        results.Should().HaveCount(expectedContactIds.Count);
        results.Select(c => c.CustomContactId).Should().BeEquivalentTo(expectedContactIds);
    }

    [Fact]
    public async Task ToListAsync_WhereAll_WithOrCondition_DeMorganApplied()
    {
        // All(join && (Name == "Custom Account 001" || Rating != null))
        // DeMorgan negation: Name != "Custom Account 001" AND Rating == null
        // Returns contacts where NO linked account fails BOTH conditions.
        // Cross-validate: primary contacts minus those whose account fails.
        var primaryContacts = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId))
            .ToListAsync();

        var failingAccountContactIds = (await (from a in Service.Queryable<CustomAccount>()
                                                where a.PrimaryContact != null
                                                      && a.Name != "Custom Account 001"
                                                      && a.AccountRating_OptionSetValue == null
                                                select a.PrimaryContact.Id).ToListAsync())
            .Distinct().ToHashSet();

        var results = await Service.Queryable<CustomContact>()
            .Where(contact => Service.Queryable<CustomAccount>().All(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && (a.Name == "Custom Account 001" || a.AccountRating_OptionSetValue != null)))
            .ToListAsync();

        results.Should().HaveCount(primaryContacts.Count - failingAccountContactIds.Count);
        results.Select(c => c.CustomContactId).Should().NotContain(
            id => failingAccountContactIds.Contains(id));
    }

    // -------------------------------------------------------------------------
    // Exists() — link-type="exists" / "not exists"
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ToListAsync_WhereExists_ReturnsAccountsWithMatchingContacts()
    {
        // Find accounts where there EXISTS a contact with a non-null rating.
        // Cross-validate with a join: distinct account IDs from contacts that have ratings.
        var expectedIds = (await (from a in Service.Queryable<CustomAccount>()
                                  join c in Service.Queryable<CustomContact>()
                                      on a.CustomAccountId equals c.ParentAccount.Id
                                  where c.ContactRating_OptionSetValue != null
                                  select a.CustomAccountId).ToListAsync())
            .Distinct().ToList();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => Service.Queryable<CustomContact>().Exists(
                c => c.ParentAccount.Id == a.CustomAccountId
                     && c.ContactRating_OptionSetValue != null))
            .ToListAsync();

        results.Should().HaveCount(expectedIds.Count);
        results.Select(a => a.CustomAccountId).Should().BeEquivalentTo(expectedIds);
    }

    [Fact]
    public async Task ToListAsync_WhereNotExists_ReturnsAccountsWithNoMatchingContacts()
    {
        // !Exists(rated contact) should return accounts that have NO contacts with ratings.
        // Cross-validate: all accounts minus those that have at least one rated contact.
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();

        var accountsWithRatedContact = (await (from a in Service.Queryable<CustomAccount>()
                                               join c in Service.Queryable<CustomContact>()
                                                   on a.CustomAccountId equals c.ParentAccount.Id
                                               where c.ContactRating_OptionSetValue != null
                                               select a.CustomAccountId).ToListAsync())
            .Distinct().ToHashSet();

        var expected = allAccounts
            .Where(a => !accountsWithRatedContact.Contains(a.CustomAccountId))
            .ToList();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => !Service.Queryable<CustomContact>().Exists(
                c => c.ParentAccount.Id == a.CustomAccountId
                     && c.ContactRating_OptionSetValue != null))
            .ToListAsync();

        results.Should().HaveCount(expected.Count);
        results.Select(a => a.CustomAccountId).Should().BeEquivalentTo(
            expected.Select(a => a.CustomAccountId));
    }

    [Fact]
    public async Task ToListAsync_WhereExists_PlusNotExists_CoversAllAccounts()
    {
        // Exists + !Exists with the same predicate should cover all accounts.
        var allAccounts = await Service.Queryable<CustomAccount>().ToListAsync();

        var exists = await Service.Queryable<CustomAccount>()
            .Where(a => Service.Queryable<CustomContact>().Exists(
                c => c.ParentAccount.Id == a.CustomAccountId))
            .ToListAsync();

        var notExists = await Service.Queryable<CustomAccount>()
            .Where(a => !Service.Queryable<CustomContact>().Exists(
                c => c.ParentAccount.Id == a.CustomAccountId))
            .ToListAsync();

        (exists.Count + notExists.Count).Should().Be(allAccounts.Count);
        exists.Select(a => a.CustomAccountId).Should().NotIntersectWith(
            notExists.Select(a => a.CustomAccountId));
    }

    [Fact]
    public async Task ToListAsync_WhereExists_WithNameFilter_MatchesJoinResults()
    {
        // Accounts where there EXISTS a contact whose FirstName starts with "First001".
        // Cross-validate with a join query.
        var expectedIds = (await (from a in Service.Queryable<CustomAccount>()
                                  join c in Service.Queryable<CustomContact>()
                                      on a.CustomAccountId equals c.ParentAccount.Id
                                  where c.FirstName.StartsWith("First001")
                                  select a.CustomAccountId).ToListAsync())
            .Distinct().ToList();

        var results = await Service.Queryable<CustomAccount>()
            .Where(a => Service.Queryable<CustomContact>().Exists(
                c => c.ParentAccount.Id == a.CustomAccountId
                     && c.FirstName.StartsWith("First001")))
            .ToListAsync();

        results.Should().HaveCount(expectedIds.Count);
        results.Select(a => a.CustomAccountId).Should().BeEquivalentTo(expectedIds);
    }

    // -------------------------------------------------------------------------
    // Entity.Id — where clause using base Entity.Id property
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Where_EntityId_ReturnsMatchingRecord()
    {
        // First get a known account ID
        var knownAccount = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        // Query using Entity.Id instead of the typed CustomAccountId property
        var result = await Service.Queryable<CustomAccount>()
            .FirstOrDefaultAsync(a => a.Id == knownAccount.Id);

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
        result.CustomAccountId.Should().Be(knownAccount.CustomAccountId);
    }

    [Fact]
    public async Task Where_EntityId_WithSelect_ReturnsProjectedResult()
    {
        var knownAccount = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        var result = await Service.Queryable<CustomAccount>()
            .Where(a => a.Id == knownAccount.Id)
            .Select(a => new { a.Name, a.Website })
            .FirstOrDefaultAsync();

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task Where_EntityId_Contact_ReturnsMatchingRecord()
    {
        var knownContact = await Service.Queryable<CustomContact>()
            .FirstAsync();

        var result = await Service.Queryable<CustomContact>()
            .FirstOrDefaultAsync(c => c.Id == knownContact.Id);

        result.Should().NotBeNull();
        result!.CustomContactId.Should().Be(knownContact.CustomContactId);
    }

    // -------------------------------------------------------------------------
    // Column comparison across single entity
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Join_CompareEntityRefToGuid_WithSameEntity_ReturnsResults()
    {
        // Column comparison within a single entity (not cross-entity in a join)
        var all = await Service.Queryable<CustomOpportunity>().ToListAsync();
        var expected = all.Count(o =>
            o.ActualRevenue.HasValue && o.EstimatedRevenue.HasValue
            && o.ActualRevenue > o.EstimatedRevenue);

        var results = await Service.Queryable<CustomOpportunity>()
            .Where(o => o.ActualRevenue > o.EstimatedRevenue)
            .ToListAsync();

        results.Should().HaveCount(expected);
    }
}

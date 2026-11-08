using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// <c>Equals</c> in a filter translates as <c>==</c> does: <c>x.Name.Equals("X")</c>,
/// <c>Equals(x.Name, "X")</c>, <c>string.Equals(x.Name, "X", comparison)</c>, on a property, the
/// indexer or <c>GetAttributeValue</c>.
/// </summary>
public class EqualsFetchXmlTests : FetchXmlTestBase
{
    private IQueryable<CustomAccount> Accounts => _service.Queryable<CustomAccount>();

    private IQueryable<Entity> UnboundAccounts => _service.Queryable("account");

    private static void ShouldTranslateAlike(string equals, string equalityOperator) =>
        equals.Should().Be(equalityOperator);

    [Fact]
    public void ToFetchXml_WhereStringEquals_GeneratesEqualCondition()
    {
        var fetchXml = Accounts.Where(a => a.Name.Equals("Contoso")).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEquals_TranslatesAsNotEqual() =>
        ShouldTranslateAlike(
            Accounts.Where(a => !a.Name.Equals("Contoso")).ToFetchXml(),
            Accounts.Where(a => a.Name != "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereStaticStringEquals_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => string.Equals(a.Name, "Contoso")).ToFetchXml(),
            Accounts.Where(a => a.Name == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereStaticObjectEquals_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => Equals(a.Name, "Contoso")).ToFetchXml(),
            Accounts.Where(a => a.Name == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereEqualsWithComparison_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a.Name.Equals("contoso", StringComparison.OrdinalIgnoreCase)).ToFetchXml(),
            Accounts.Where(a => a.Name == "contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereStaticEqualsWithComparison_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => string.Equals(a.Name, "contoso", StringComparison.CurrentCultureIgnoreCase)).ToFetchXml(),
            Accounts.Where(a => a.Name == "contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereValueTypeEquals_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a.NumberOfEmployees.Equals(50)).ToFetchXml(),
            Accounts.Where(a => a.NumberOfEmployees == 50).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereEqualsNull_TranslatesAsNullCondition() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a.Name.Equals(null)).ToFetchXml(),
            Accounts.Where(a => a.Name == null).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereEqualsAnotherColumn_TranslatesAsColumnComparison() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a.Name.Equals(a.Description)).ToFetchXml(),
            Accounts.Where(a => a.Name == a.Description).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerEquals_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            UnboundAccounts.Where(a => a["name"].Equals("Contoso")).ToFetchXml(),
            UnboundAccounts.Where(a => a.GetAttributeValue<string>("name") == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereCastIndexerEquals_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            UnboundAccounts.Where(a => ((string)a["name"]).Equals("Contoso")).ToFetchXml(),
            UnboundAccounts.Where(a => a.GetAttributeValue<string>("name") == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueEquals_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            UnboundAccounts.Where(a => a.GetAttributeValue<string>("name").Equals("Contoso")).ToFetchXml(),
            UnboundAccounts.Where(a => a.GetAttributeValue<string>("name") == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_EqualsInCountPredicate_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.ToFetchXml(q => q.Count(a => a.Name.Equals("Contoso"))),
            Accounts.ToFetchXml(q => q.Count(a => a.Name == "Contoso")));

    [Theory]
    [InlineData(StringComparison.Ordinal)]
    [InlineData(StringComparison.InvariantCulture)]
    [InlineData(StringComparison.CurrentCulture)]
    public void ToFetchXml_WhereEqualsWithCaseSensitiveComparison_Throws(StringComparison comparison)
    {
        // Dataverse would match "Contoso" for "contoso".
        var act = () => Accounts.Where(a => a.Name.Equals("contoso", comparison)).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereEqualsOfAnotherNumberType_Throws()
    {
        // A boxed long never equals an int?, where == would convert it.
        var act = () => Accounts.Where(a => a.NumberOfEmployees.Equals(50L)).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereEnumEqualsItsNumber_Throws()
    {
        var act = () => Accounts.Where(a => a.AccountRating.Equals(1)).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereEnumEqualsItsMember_TranslatesAsEqualityOperator() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a.AccountRating.Equals(CustomAccount.AccountRating_Enum.Hot)).ToFetchXml(),
            Accounts.Where(a => a.AccountRating == CustomAccount.AccountRating_Enum.Hot).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereEqualsOfAnotherType_Throws()
    {
        // An int? never equals a string; there's no condition to give.
        var act = () => Accounts.Where(a => a.NumberOfEmployees.Equals("50")).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }
}

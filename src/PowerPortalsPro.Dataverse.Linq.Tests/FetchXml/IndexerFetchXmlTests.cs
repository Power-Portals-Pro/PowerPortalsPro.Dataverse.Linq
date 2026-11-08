using FluentAssertions;
using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// The indexer, <c>entity["name"]</c>, names a column wherever <c>GetAttributeValue</c> does: in a
/// filter, an order, a join key, an aggregate's selector. Each query translates as its
/// <c>GetAttributeValue</c> twin does.
/// </summary>
public class IndexerFetchXmlTests : FetchXmlTestBase
{
    private static readonly Guid ParentId = new("11111111-2222-3333-4444-555555555555");

    private IQueryable<Entity> Accounts => _service.Queryable("account");

    private IQueryable<Entity> Contacts => _service.Queryable("contact");

    private static void ShouldTranslateAlike(string indexer, string getAttributeValue) =>
        indexer.Should().Be(getAttributeValue);

    // -------------------------------------------------------------------------
    // Where
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereIndexerEquals_GeneratesEqualCondition()
    {
        var fetchXml = Accounts.Where(a => (string)a["name"] == "Contoso").ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <all-attributes />
                <filter type="and">
                  <condition attribute="name" operator="eq" value="Contoso" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereIndexerIsNull_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a["description"] == null).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<string>("description") == null).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerIsNotNull_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => a["description"] != null).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<string>("description") != null).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerStartsWith_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => ((string)a["name"]).StartsWith("Con")).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<string>("name").StartsWith("Con")).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerIsNullOrEmpty_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => string.IsNullOrEmpty((string)a["name"])).ToFetchXml(),
            Accounts.Where(a => string.IsNullOrEmpty(a.GetAttributeValue<string>("name"))).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerGreaterThan_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => (int?)a["numberofemployees"] > 10).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<int?>("numberofemployees") > 10).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerLookupId_GeneratesConditionOnTheLookup()
    {
        var fetchXml = Accounts
            .Where(a => ((EntityReference)a["parentaccountid"]).Id == ParentId)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <all-attributes />
                <filter type="and">
                  <condition attribute="parentaccountid" operator="eq" value="11111111-2222-3333-4444-555555555555" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereIndexerAsString_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => (a["name"] as string) == "Contoso").ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<string>("name") == "Contoso").ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereIndexerAsLookupId_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => (a["parentaccountid"] as EntityReference)!.Id == ParentId).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<EntityReference>("parentaccountid").Id == ParentId).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereNarrowedColumnValue_Throws()
    {
        // (int?) truncates 3.7 to 3, which a condition on the decimal column wouldn't match.
        var act = () => Accounts
            .Where(a => ((int?)a.GetAttributeValue<decimal?>("revenue")).Value == 3)
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_WhereIndexerOptionSetValue_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.Where(a => ((OptionSetValue)a["statecode"]).Value == 0).ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<OptionSetValue>("statecode").Value == 0).ToFetchXml());

    [Fact]
    public void ToFetchXml_WhereListContainsIndexer_TranslatesAsGetAttributeValue()
    {
        var names = new[] { "Contoso", "Fabrikam" };

        ShouldTranslateAlike(
            Accounts.Where(a => names.Contains((string)a["name"])).ToFetchXml(),
            Accounts.Where(a => names.Contains(a.GetAttributeValue<string>("name"))).ToFetchXml());
    }

    [Fact]
    public void ToFetchXml_WhereIndexerWithCapturedName_TranslatesAsGetAttributeValue()
    {
        var column = "name";

        ShouldTranslateAlike(
            Accounts.Where(a => (string)a[column] == "Contoso").ToFetchXml(),
            Accounts.Where(a => a.GetAttributeValue<string>(column) == "Contoso").ToFetchXml());
    }

    // -------------------------------------------------------------------------
    // OrderBy, terminal operators and aggregates
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_OrderByIndexer_GeneratesOrders()
    {
        var fetchXml = Accounts
            .OrderBy(a => a["name"])
            .ThenByDescending(a => a["createdon"])
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="account">
                <all-attributes />
                <order attribute="name" descending="false" />
                <order attribute="createdon" descending="true" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_FirstOrDefaultWithIndexerPredicate_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.ToFetchXml(q => q.FirstOrDefault(a => (string)a["name"] == "Contoso")),
            Accounts.ToFetchXml(q => q.FirstOrDefault(a => a.GetAttributeValue<string>("name") == "Contoso")));

    [Fact]
    public void ToFetchXml_CountWithIndexerPredicate_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.ToFetchXml(q => q.Count(a => (string)a["name"] == "Contoso")),
            Accounts.ToFetchXml(q => q.Count(a => a.GetAttributeValue<string>("name") == "Contoso")));

    [Fact]
    public void ToFetchXml_MaxOfIndexer_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            Accounts.ToFetchXml(q => q.Max(a => (int?)a["numberofemployees"])),
            Accounts.ToFetchXml(q => q.Max(a => a.GetAttributeValue<int?>("numberofemployees"))));

    // -------------------------------------------------------------------------
    // Joins
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_JoinOnIndexerKey_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            (from a in Accounts
             join c in Contacts on a.Id equals ((EntityReference)c["parentcustomerid"]).Id
             select new { Name = (string)a["name"], FullName = (string)c["fullname"] }).ToFetchXml(),
            (from a in Accounts
             join c in Contacts on a.Id equals c.GetAttributeValue<EntityReference>("parentcustomerid").Id
             select new { Name = a.GetAttributeValue<string>("name"), FullName = c.GetAttributeValue<string>("fullname") }).ToFetchXml());

    [Fact]
    public void ToFetchXml_LeftJoinWhereOnInnerIndexer_TranslatesAsGetAttributeValue() =>
        ShouldTranslateAlike(
            (from a in Accounts
             join c in Contacts on a.Id equals ((EntityReference)c["parentcustomerid"]).Id into contacts
             from c in contacts.DefaultIfEmpty()
             where (string)c["fullname"] == "Ann Smith"
             select new { Name = (string)a["name"] }).ToFetchXml(),
            (from a in Accounts
             join c in Contacts on a.Id equals c.GetAttributeValue<EntityReference>("parentcustomerid").Id into contacts
             from c in contacts.DefaultIfEmpty()
             where c.GetAttributeValue<string>("fullname") == "Ann Smith"
             select new { Name = a.GetAttributeValue<string>("name") }).ToFetchXml());
}

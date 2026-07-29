using FluentAssertions;
using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// <c>GetAttributeValue&lt;T&gt;(name)</c> where the attribute name is not a compile-time
/// constant. Any argument that can be evaluated while the query is translated — a local,
/// a field or property, a method call, an indexer, a concatenation, a ternary — resolves
/// to the attribute name.
/// </summary>
public class DynamicAttributeNameFetchXmlTests : FetchXmlTestBase
{
    private const string NameAttributeConst = "new_name";
    private static readonly string _nameAttributeStatic = "new_name";

    private sealed class AttributeNames
    {
        public string Name { get; } = "new_name";
        public string WebsiteField = "new_website";
        public string GetName() => "new_name";
        public static string GetWebsite() => "new_website";
    }

    // -------------------------------------------------------------------------
    // Where — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithLocalVariable_ResolvesAttribute()
    {
        var nameAttribute = "new_name";

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(nameAttribute) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithLocalVariable_TypedProxy_ResolvesAttribute()
    {
        var nameAttribute = "new_name";

        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.GetAttributeValue<string>(nameAttribute) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithConstField_ResolvesAttribute()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(NameAttributeConst) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithStaticField_ResolvesAttribute()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(_nameAttributeStatic) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithProperty_ResolvesAttribute()
    {
        var names = new AttributeNames();

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(names.Name) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithInstanceMethodCall_ResolvesAttribute()
    {
        var names = new AttributeNames();

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(names.GetName()) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithStaticMethodCall_ResolvesAttribute()
    {
        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(AttributeNames.GetWebsite()) != null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_website" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithConcatenatedName_ResolvesAttribute()
    {
        var prefix = "new_";

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(prefix + "name") == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithInterpolatedName_ResolvesAttribute()
    {
        var prefix = "new";

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>($"{prefix}_name") == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithTernaryName_ResolvesAttribute()
    {
        var useWebsite = true;

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(useWebsite ? "new_website" : "new_name") != null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_website" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithDictionaryLookup_ResolvesAttribute()
    {
        var attributes = new Dictionary<string, string> { ["name"] = "new_name" };

        var fetchXml = _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(attributes["name"]) == "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueWithLoopVariable_ResolvesEachAttribute()
    {
        var expected = new[] { "new_name", "new_website" };

        foreach (var attribute in expected)
        {
            var fetchXml = _service.Queryable("new_customaccount")
                .Where(x => !string.IsNullOrEmpty(x.GetAttributeValue<string>(attribute)))
                .ToFetchXml();

            AssertFetchXml(fetchXml,
                $"""
                <fetch mapping="logical">
                  <entity name="new_customaccount">
                    <all-attributes />
                    <filter type="and">
                      <condition attribute="{attribute}" operator="not-null" />
                      <condition attribute="{attribute}" operator="ne" value="" />
                    </filter>
                  </entity>
                </fetch>
                """);
        }
    }

    [Fact]
    public void ToFetchXml_WhereGetAttributeValueEntityReferenceIdWithVariable_ResolvesAttribute()
    {
        var lookupAttribute = "new_parentaccount";
        var id = Guid.NewGuid();

        var fetchXml = _service.Queryable("new_customcontact")
            .Where(x => x.GetAttributeValue<EntityReference>(lookupAttribute).Id == id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            $"""
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_parentaccount" operator="eq" value="{id}" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Select / OrderBy — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_SelectGetAttributeValueWithVariables_GeneratesAttributes()
    {
        var nameAttribute = "new_name";
        var names = new AttributeNames();

        var fetchXml = _service.Queryable("new_customaccount")
            .Select(x => new
            {
                Name = x.GetAttributeValue<string>(nameAttribute),
                Website = x.GetAttributeValue<string>(names.WebsiteField),
            })
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
    public void ToFetchXml_OrderByGetAttributeValueWithVariable_ResolvesAttribute()
    {
        var nameAttribute = "new_name";

        var fetchXml = _service.Queryable("new_customaccount")
            .OrderBy(x => x.GetAttributeValue<string>(nameAttribute))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <order attribute="new_name" descending="false" />
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_GroupByGetAttributeValueWithVariable_ResolvesAttribute()
    {
        var nameAttribute = "new_name";

        var fetchXml = _service.Queryable<CustomAccount>()
            .GroupBy(a => a.GetAttributeValue<string>(nameAttribute))
            .Select(g => new { Name = g.Key, Count = g.Count() })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical" aggregate="true">
              <entity name="new_customaccount">
                <attribute name="new_name" alias="name" groupby="true" />
                <attribute name="new_customaccountid" alias="count" aggregate="count" />
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Joins — attribute name from a variable
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_JoinOnGetAttributeValueWithVariable_GeneratesJoin()
    {
        var lookupAttribute = "pluginassemblyid";
        var nameAttribute = "name";

        var fetchXml = (from pt in _service.Queryable("plugintype")
                        join pa in _service.Queryable("pluginassembly")
                            on pt.GetAttributeValue<EntityReference>(lookupAttribute).Id equals pa.Id
                        where pa.GetAttributeValue<string>(nameAttribute) == "TestAssembly"
                        select new
                        {
                            pa,
                            pt,
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="plugintype">
                <all-attributes />
                <filter type="and">
                  <condition entityname="pa" attribute="name" operator="eq" value="TestAssembly" />
                </filter>
                <link-entity name="pluginassembly" from="pluginassemblyid" to="pluginassemblyid" alias="pa" link-type="inner">
                  <all-attributes />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_LeftJoinSelectGetAttributeValueWithVariable_IncludesInnerColumns()
    {
        var firstNameAttribute = "new_firstname";
        var lastNameAttribute = "new_lastname";

        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id into contacts
                        from c in contacts.DefaultIfEmpty()
                        select new
                        {
                            a.Name,
                            FirstName = c.GetAttributeValue<string>(firstNameAttribute),
                            LastName = c.GetAttributeValue<string>(lastNameAttribute),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="outer">
                  <attribute name="new_firstname" />
                  <attribute name="new_lastname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_InnerJoinSelectGetAttributeValueWithVariable_IncludesBothColumns()
    {
        var accountNameAttribute = "new_name";
        var firstNameAttribute = "new_firstname";

        var fetchXml = (from a in _service.Queryable("new_customaccount")
                        join c in _service.Queryable("new_customcontact")
                            on a.Id equals c.GetAttributeValue<EntityReference>("new_parentaccount").Id
                        select new
                        {
                            AccountName = a.GetAttributeValue<string>(accountNameAttribute),
                            ContactName = c.GetAttributeValue<string>(firstNameAttribute),
                        }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Unresolvable names
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_GetAttributeValueWithRowDependentName_Throws()
    {
        var act = () => _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(x.GetAttributeValue<string>("new_description")) == "Test")
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void ToFetchXml_GetAttributeValueWithNullName_ThrowsWithExplanation()
    {
        string nameAttribute = null!;

        var act = () => _service.Queryable("new_customaccount")
            .Where(x => x.GetAttributeValue<string>(nameAttribute) == "Test")
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>()
            .WithMessage("*evaluated to null*");
    }
}

using FluentAssertions;
using XrmToolkit.Dataverse.Linq.Tests.Proxies;

namespace XrmToolkit.Dataverse.Linq.Tests.FetchXml;

public class FilterFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Where — typed proxy
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereEqualToValue_GeneratesEqFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == "Test")
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
    public void ToFetchXml_WhereNotEqualToNull_GeneratesNotNullFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualToNull_GeneratesNullFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name == null)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="null" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — NotEqual (non-null)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereNotEqualToValue_GeneratesNeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name != "Test")
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="ne" value="Test" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — comparison operators (lt, le, gt, ge)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereLessThan_GeneratesLtFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees < 50)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="lt" value="50" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereLessEqual_GeneratesLeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees <= 50)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="le" value="50" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGreaterThan_GeneratesGtFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees > 950)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="gt" value="950" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereGreaterEqual_GeneratesGeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.NumberOfEmployees >= 950)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_numberofemployees" operator="ge" value="950" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — In / NotIn
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereIn_GeneratesInFilter()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002", "Custom Account 003" };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => names.Contains(a.Name))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="in">
                    <value>Custom Account 001</value>
                    <value>Custom Account 002</value>
                    <value>Custom Account 003</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotIn_GeneratesNotInFilter()
    {
        var names = new[] { "Custom Account 001", "Custom Account 002" };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !names.Contains(a.Name))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-in">
                    <value>Custom Account 001</value>
                    <value>Custom Account 002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereInWithGuids_GeneratesInFilterOnPrimaryKey()
    {
        var ids = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222") };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => ids.Contains(a.CustomAccountId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="in">
                    <value>11111111-1111-1111-1111-111111111111</value>
                    <value>22222222-2222-2222-2222-222222222222</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotInWithGuids_GeneratesNotInFilterOnPrimaryKey()
    {
        var ids = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111") };
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !ids.Contains(a.CustomAccountId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="not-in">
                    <value>11111111-1111-1111-1111-111111111111</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // String methods (Contains, StartsWith, EndsWith)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_JoinWithContainsFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Contains("Test"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%Test%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithStartsWithFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.StartsWith("Custom"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="Custom%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WithEndsWithFilter_GeneratesLikeCondition()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.EndsWith("001"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="%001" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // String.Length
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_StringLengthEqual_GeneratesLikeWithUnderscores()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length == 5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="_____" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthNotEqual_GeneratesNotLikeWithUnderscores()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length != 3)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-like" value="___" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthGreaterThan_GeneratesLikePattern()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length > 5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="______%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthGreaterThanOrEqual_GeneratesLikePattern()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length >= 5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="_____%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthLessThan_GeneratesNotLikePattern()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length < 5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-like" value="_____%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthLessThanOrEqual_GeneratesNotLikePattern()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Name.Length <= 5)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-like" value="______%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_StringLengthReversed_FlipsOperator()
    {
        // 10 <= x.Name.Length is equivalent to x.Name.Length >= 10
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => 10 <= a.Name.Length)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="like" value="__________%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Negated filter
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_NegatedContains_GeneratesNotLikeFilter()
    {
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => !a.Name.Contains("Test"))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_name" operator="not-like" value="%Test%" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — Column-to-column comparison (valueof)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereSameTableColumnEqual_GeneratesValueOfCondition()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FirstName == c.LastName)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_firstname" operator="eq" valueof="new_lastname" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereSameTableColumnNotEqual_GeneratesValueOfCondition()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FirstName != c.LastName)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_firstname" operator="ne" valueof="new_lastname" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereCrossTableColumnEqual_GeneratesValueOfWithAlias()
    {
        var fetchXml = (from a in _service.Queryable<CustomAccount>()
                        join c in _service.Queryable<CustomContact>()
                            on a.CustomAccountId equals c.ParentAccount.Id
                        where a.Name == c.FirstName
                        select new { a.Name, c.FirstName }).ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_name" operator="eq" valueof="c.new_firstname" />
                </filter>
                <link-entity name="new_customcontact" from="new_parentaccount" to="new_customaccountid" alias="c" link-type="inner">
                  <attribute name="new_firstname" />
                </link-entity>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereColumnComparisonTypeMismatch_ThrowsNotSupportedException()
    {
        var act = () => _service.Queryable<CustomAccount>()
            .Where(a => a.GetAttributeValue<int?>("new_numberofemployees") == a.GetAttributeValue<decimal?>("new_percentcomplete"))
            .ToFetchXml();

        act.Should().Throw<NotSupportedException>()
            .WithMessage("*same type*");
    }

    // -------------------------------------------------------------------------
    // Where — Any() (link-type="any" / "not any")
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereAny_GeneratesLinkTypeAny()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(contact => _service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Contoso"))
            .Select(contact => new { contact.Name })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <attribute name="new_name" />
                <filter type="and">
                  <link-entity name="new_customaccount" from="new_primarycontact" to="new_customcontactid" alias="a" link-type="any">
                    <filter type="and">
                      <condition attribute="new_name" operator="eq" value="Contoso" />
                    </filter>
                  </link-entity>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotAny_GeneratesLinkTypeNotAny()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(contact => !_service.Queryable<CustomAccount>().Any(
                a => a.PrimaryContact.Id == contact.CustomContactId
                     && a.Name == "Contoso"))
            .Select(contact => new { contact.Name })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <attribute name="new_name" />
                <filter type="and">
                  <link-entity name="new_customaccount" from="new_primarycontact" to="new_customcontactid" alias="a" link-type="not any">
                    <filter type="and">
                      <condition attribute="new_name" operator="eq" value="Contoso" />
                    </filter>
                  </link-entity>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereAnyInOrFilter_GeneratesCorrectStructure()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(contact => _service.Queryable<CustomAccount>().Any(
                                  a => a.PrimaryContact.Id == contact.CustomContactId
                                       && a.Name == "Contoso")
                              || contact.Status == CustomContact.CustomContact_Status.Inactive)
            .Select(contact => new { contact.Name })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <attribute name="new_name" />
                <filter type="or">
                  <link-entity name="new_customaccount" from="new_primarycontact" to="new_customcontactid" alias="a" link-type="any">
                    <filter type="and">
                      <condition attribute="new_name" operator="eq" value="Contoso" />
                    </filter>
                  </link-entity>
                  <condition attribute="statecode" operator="eq" value="1" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Entity.Id resolution
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereEntityId_ResolvesToPrimaryKeyAttribute()
    {
        var id = Guid.NewGuid();
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Id == id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            $"""
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq" value="{id}" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEntityIdWithSelect_ResolvesToPrimaryKeyAttribute()
    {
        var id = Guid.NewGuid();
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.Id == id)
            .Select(a => new { a.Name })
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            $"""
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <attribute name="new_name" />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq" value="{id}" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEntityId_ContactEntity_ResolvesToCorrectPrimaryKey()
    {
        var id = Guid.NewGuid();
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.Id == id)
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            $"""
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customcontactid" operator="eq" value="{id}" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Or condition with constant value
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ConstantOrCondition_WithNullValue_ThrowsNotSupported()
    {
        var date = (DateTime?)null;

        var act = () => (from a in _service.Queryable<CustomAccount>()
                         where (date == null || a.CreatedOn > date)
                         select a).ToFetchXml();

        act.Should().Throw<NotSupportedException>();
    }

    // -------------------------------------------------------------------------
    // Contains with zero elements
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_ContainsWithEmptyList_GeneratesInWithNoValues()
    {
        var accountIds = new List<Guid>();
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => accountIds.Contains(a.CustomAccountId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="in" />
                </filter>
              </entity>
            </fetch>
            """);
    }
}

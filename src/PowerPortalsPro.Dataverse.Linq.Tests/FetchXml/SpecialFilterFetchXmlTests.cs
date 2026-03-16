using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Extensions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

public class SpecialFilterFetchXmlTests : FetchXmlTestBase
{
    // -------------------------------------------------------------------------
    // Where — Hierarchy operators
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereAbove_GeneratesAboveFilter()
    {
        var parentId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Above(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="above" value="11111111-1111-1111-1111-111111111111" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereAboveOrEqual_GeneratesEqOrAboveFilter()
    {
        var parentId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.AboveOrEqual(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq-or-above" value="11111111-1111-1111-1111-111111111111" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereUnder_GeneratesUnderFilter()
    {
        var parentId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.Under(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="under" value="22222222-2222-2222-2222-222222222222" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereUnderOrEqual_GeneratesEqOrUnderFilter()
    {
        var parentId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.UnderOrEqual(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="eq-or-under" value="22222222-2222-2222-2222-222222222222" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotUnder_GeneratesNotUnderFilter()
    {
        var parentId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var fetchXml = _service.Queryable<CustomAccount>()
            .Where(a => a.CustomAccountId.NotUnder(parentId))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customaccount">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customaccountid" operator="not-under" value="33333333-3333-3333-3333-333333333333" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualUserOrUserHierarchy_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.CustomContactId.EqualUserOrUserHierarchy())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customcontactid" operator="eq-useroruserhierarchy" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualUserOrUserHierarchyAndTeams_GeneratesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.CustomContactId.EqualUserOrUserHierarchyAndTeams())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_customcontactid" operator="eq-useroruserhierarchyandteams" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — User / Business unit operators
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereEqualUserId_GeneratesEqUserIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.Owner.Id.EqualUserId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="ownerid" operator="eq-userid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEqualUserId_GeneratesNeUserIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.Owner.Id.NotEqualUserId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="ownerid" operator="ne-userid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereEqualBusinessId_GeneratesEqBusinessIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.OwningBusinessUnit.Id.EqualBusinessId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="owningbusinessunit" operator="eq-businessid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNotEqualBusinessId_GeneratesNeBusinessIdFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.OwningBusinessUnit.Id.NotEqualBusinessId())
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="owningbusinessunit" operator="ne-businessid" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — ContainsValues (multi-select option set)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereContainsValues_GeneratesContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.ContainsValues(
                CustomContact.Color.Red, CustomContact.Color.Orange, CustomContact.Color.Blue))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="contain-values">
                    <value>100000000</value>
                    <value>100000001</value>
                    <value>100000002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedContainsValues_GeneratesNotContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.ContainsValues(
                CustomContact.Color.Red, CustomContact.Color.Blue))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="not-contain-values">
                    <value>100000000</value>
                    <value>100000002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereContainsValuesSingleValue_GeneratesContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.ContainsValues(CustomContact.Color.Green))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="contain-values">
                    <value>100000008</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereListContainsEnum_GeneratesContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Contains(CustomContact.Color.Red))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="contain-values">
                    <value>100000000</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedListContainsEnum_GeneratesDoesNotContainValuesFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Contains(CustomContact.Color.Blue))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="not-contain-values">
                    <value>100000002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    // -------------------------------------------------------------------------
    // Where — MultiSelect Equals (eq / in / not-in)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToFetchXml_WhereMultiSelectEqualsSingleValue_GeneratesEqFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Equals(CustomContact.Color.Red))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="eq" value="100000000" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedMultiSelectEqualsSingleValue_GeneratesNeFilter()
    {
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Equals(CustomContact.Color.Red))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="ne" value="100000000" />
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereMultiSelectEqualsMultipleValues_GeneratesInFilter()
    {
        var colors = new[] { CustomContact.Color.Red, CustomContact.Color.Blue };
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => c.FavoriteColors.Equals(colors))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="in">
                    <value>100000000</value>
                    <value>100000002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }

    [Fact]
    public void ToFetchXml_WhereNegatedMultiSelectEqualsMultipleValues_GeneratesNotInFilter()
    {
        var colors = new[] { CustomContact.Color.Red, CustomContact.Color.Blue };
        var fetchXml = _service.Queryable<CustomContact>()
            .Where(c => !c.FavoriteColors.Equals(colors))
            .ToFetchXml();

        AssertFetchXml(fetchXml,
            """
            <fetch mapping="logical">
              <entity name="new_customcontact">
                <all-attributes />
                <filter type="and">
                  <condition attribute="new_favoritecolors" operator="not-in">
                    <value>100000000</value>
                    <value>100000002</value>
                  </condition>
                </filter>
              </entity>
            </fetch>
            """);
    }
}

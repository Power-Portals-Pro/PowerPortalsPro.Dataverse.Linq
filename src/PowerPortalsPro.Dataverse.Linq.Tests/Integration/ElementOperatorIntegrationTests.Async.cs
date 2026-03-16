using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using FluentAssertions;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.Integration;

public partial class ElementOperatorIntegrationTests
{
    // -------------------------------------------------------------------------
    // Terminal operators — First / FirstOrDefault / Single / SingleOrDefault
    // -------------------------------------------------------------------------

    [Fact]
    public async Task FirstAsync_ReturnsFirstRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .OrderBy(a => a.Name)
            .FirstAsync();

        result.Should().NotBeNull();
        result.Name.Should().NotBeNull();
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_ReturnsMatchingRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithNoMatch_ReturnsNull()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstOrDefaultAsync(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithMatch_ReturnsRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .FirstOrDefaultAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task SingleAsync_ReturnsOnlyMatchingRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result.Name.Should().Be("Custom Account 001");
    }

    [Fact]
    public async Task SingleAsync_WithMultipleMatches_Throws()
    {
        var act = () => Service.Queryable<CustomAccount>()
            .Where(a => a.Name != null)
            .SingleAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithNoMatch_ReturnsNull()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleOrDefaultAsync(a => a.Name == "NonExistent Account ZZZ");

        result.Should().BeNull();
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithOneMatch_ReturnsRecord()
    {
        var result = await Service.Queryable<CustomAccount>()
            .SingleOrDefaultAsync(a => a.Name == "Custom Account 001");

        result.Should().NotBeNull();
        result!.Name.Should().Be("Custom Account 001");
    }
}

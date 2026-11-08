using FluentAssertions;
#if !NETFRAMEWORK
using Microsoft.PowerPlatform.Dataverse.Client;
#endif
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using NSubstitute;
using PowerPortalsPro.Dataverse.Linq.Tests.Proxies;

namespace PowerPortalsPro.Dataverse.Linq.Tests.FetchXml;

/// <summary>
/// A query the provider can't translate throws when it's run, sync or async, before any request
/// reaches the service: it never falls back to sending part of the query. One query per kind of
/// guard, run the way a caller runs it rather than through <c>ToFetchXml</c>.
/// </summary>
public class UntranslatableQueryExecutionTests : FetchXmlTestBase
{
    public sealed record Sources(
        IQueryable<CustomAccount> Accounts, IQueryable<CustomContact> Contacts, IQueryable<CustomOpportunity> Opportunities);

    private static Sources SourcesFor(IOrganizationService service) =>
        new(service.Queryable<CustomAccount>(), service.Queryable<CustomContact>(), service.Queryable<CustomOpportunity>());

    public static TheoryData<string, string, Func<Sources, object?>> Queries => new()
    {
        {
            "Where after an ordered Take", "*after Take()*",
            s => s.Accounts.OrderBy(a => a.Name).Take(5).Where(a => a.Name.StartsWith("Empty Account")).ToList()
        },
        { "Count after Take", "*after Take()*", s => s.Accounts.Take(5).Count() },
        {
            "Where on a computed member", "*comparison*",
            s => s.Accounts.Select(a => new CustomAccount { Name = "x" + a.Name }).Where(x => x.Name == "xContoso").ToList()
        },
        {
            "Max of a typed joined column", "*joined entity's column*",
            s => s.Accounts.Join(s.Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
                .Max(x => x.c.LastName)
        },
        {
            "Distinct over a computed value", "*Distinct() can't follow a Select that computes*",
            s => s.Accounts.Select(a => a.Name.Substring(0, 1)).Distinct().ToList()
        },
        {
            "Sum over a numeric cast", "*requires exactly one column*",
            s => s.Accounts.Select(a => (long?)a.NumberOfEmployees).Sum()
        },
        {
            "Any on a left-joined entity", "*joined through a left join*",
            s => (from a in s.Accounts
                  join c in s.Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                  from c in contacts.DefaultIfEmpty()
                  where s.Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                  select a.Name).ToList()
        },
        {
            "Equals with a case-sensitive comparison", "*StringComparison.Ordinal*",
            s => s.Accounts.Where(a => a.Name.Equals("contoso", StringComparison.Ordinal)).ToList()
        },
    };

    [Theory]
    [MemberData(nameof(Queries))]
    public void Run_UntranslatableQuery_ThrowsBeforeAnyRequest(string query, string message, Func<Sources, object?> run)
    {
        var act = () => run(SourcesFor(_service));

        act.Should().Throw<NotSupportedException>(query).WithMessage(message);
        _service.DidNotReceiveWithAnyArgs().RetrieveMultiple(default!);
    }

#if !NETFRAMEWORK
    // The async operators need the async service's queryables.
    private static Sources AsyncSourcesFor(IOrganizationServiceAsync service) =>
        new(service.Queryable<CustomAccount>(), service.Queryable<CustomContact>(), service.Queryable<CustomOpportunity>());

    public static TheoryData<string, string, Func<Sources, Task>> AsyncQueries => new()
    {
        {
            "Where after an ordered Take", "*after Take()*",
            s => s.Accounts.OrderBy(a => a.Name).Take(5).Where(a => a.Name.StartsWith("Empty Account")).ToListAsync()
        },
        { "Count after Take", "*after Take()*", s => s.Accounts.Take(5).CountAsync() },
        {
            "Where on a computed member", "*comparison*",
            s => s.Accounts.Select(a => new CustomAccount { Name = "x" + a.Name }).Where(x => x.Name == "xContoso").ToListAsync()
        },
        {
            "Max of a typed joined column", "*joined entity's column*",
            s => s.Accounts.Join(s.Contacts, a => a.CustomAccountId, c => c.ParentAccount.Id, (a, c) => new { a, c })
                .MaxAsync(x => x.c.LastName)
        },
        {
            "Sum of distinct values", "*of distinct values*",
            s => s.Accounts.Select(a => a.NumberOfEmployees).Distinct().SumAsync()
        },
        {
            "FirstOrDefault with a predicate after Take", "*after Take()*",
            s => s.Accounts.Take(5).FirstOrDefaultAsync(a => a.Name == "Contoso")
        },
        {
            "Any on a left-joined entity", "*joined through a left join*",
            s => (from a in s.Accounts
                  join c in s.Contacts on a.CustomAccountId equals c.ParentAccount.Id into contacts
                  from c in contacts.DefaultIfEmpty()
                  where s.Opportunities.Any(o => o.Contact.Id == c.CustomContactId)
                  select a.Name).ToListAsync()
        },
    };

    [Theory]
    [MemberData(nameof(AsyncQueries))]
    public async Task RunAsync_UntranslatableQuery_ThrowsBeforeAnyRequest(string query, string message, Func<Sources, Task> run)
    {
        EntityMetadataCache.Clear();
        var service = Substitute.For<IOrganizationServiceAsync2>();

        var act = () => run(AsyncSourcesFor(service));

        await act.Should().ThrowAsync<NotSupportedException>(query).WithMessage(message);
        await service.DidNotReceiveWithAnyArgs().RetrieveMultipleAsync(default!, default);
        service.DidNotReceiveWithAnyArgs().RetrieveMultiple(default!);
    }
#endif
}

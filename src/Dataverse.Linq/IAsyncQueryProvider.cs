using System.Linq.Expressions;

namespace Dataverse.Linq;

/// <summary>
/// Extends <see cref="IQueryProvider"/> with an async execution method.
/// Follows the same pattern as EF Core's IAsyncQueryProvider.
/// </summary>
public interface IAsyncQueryProvider : IQueryProvider
{
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default);
}

using System.Linq.Expressions;

namespace PowerPortalsPro.Dataverse.Linq;

/// <summary>
/// Defines an async query execution method for LINQ providers.
/// A lightweight async query provider interface owned by this library.
/// </summary>
internal interface IAsyncQueryProvider : IQueryProvider
{
    TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default);
}

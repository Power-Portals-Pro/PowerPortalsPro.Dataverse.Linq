#if !NET6_0_OR_GREATER
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

// Polyfill for IAsyncQueryProvider which is defined in Microsoft.EntityFrameworkCore.Query
// on .NET 6+. On .NET Framework, we define it ourselves since EF Core is not available.
namespace Microsoft.EntityFrameworkCore.Query;

internal interface IAsyncQueryProvider : IQueryProvider
{
    TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default);
}
#endif

using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IBatchLimitSource
{
    public int MaxBatchItems => PayloadBounds.PostgresMaxBatchItems;
}

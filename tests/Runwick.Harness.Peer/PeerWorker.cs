using System.Text.Json.Nodes;
using Runwick.Harness.Store;

namespace Runwick.Harness.Peer;

/// <summary>Worker commands (<c>startWorker</c>/<c>stopWorker</c>); the worker engine arrives in SP-009.</summary>
internal static class PeerWorker
{
    public static Task<JsonObject> HandleAsync(string cmd, JsonObject m, Func<TestClient> client) =>
        throw new NotSupportedException($"'{cmd}' needs the worker engine (SP-009).");
}

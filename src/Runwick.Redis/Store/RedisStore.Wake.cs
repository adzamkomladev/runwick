using Runwick.Protocol;

namespace Runwick.Redis.Store;

internal sealed partial class RedisStore : IWakeNotificationSource
{
    /// <summary>
    /// Work wake hints (05 §7.2): the scripts publish the queue name on the application's wake channel once a job that is due now is
    /// whole in the store. Only the work channel exists; the scheduler keeps polling, as a store without notifications does.
    /// </summary>
    public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct) =>
        channel == WakeChannels.Work && application == runner.Client.Application
            ? runner.Client.SubscribeWakeAsync(onWake, onSubscribed, onLost, ct)
            : Task.CompletedTask;
}

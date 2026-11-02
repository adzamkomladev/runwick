using Npgsql;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IWakeNotificationSource
{
    // ------------------------------------------------------------------ LISTEN/NOTIFY wake (05 §7.2, 04 Changes 2026-11-02, PF-07)

    /// <summary>Notifications received for this store's application (tests and diagnostics).</summary>
    internal long NotificationsReceived;

    /// <summary>Successful LISTEN subscriptions, the first one included.</summary>
    internal long ListenerSubscriptions;

    /// <summary>
    /// The notify statement for wake keys of one application: one notification per distinct key, sent by PostgreSQL
    /// only when the enclosing transaction commits (identical payloads in one transaction are folded into one).
    /// </summary>
    internal static string NotifySql(string channel, string keysParameter) =>
        $"SELECT pg_notify('{channel}', @application || '/' || k) FROM unnest(@{keysParameter}::text[]) AS k";

    public async Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct)
    {
        if (channel is not (WakeChannels.Work or WakeChannels.Schedule))
        {
            throw new ArgumentOutOfRangeException(nameof(channel));
        }

        var listen = sources.ListenConnectionString();
        var prefix = application + "/";
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var conn = new NpgsqlConnection(listen);
                await using (conn.ConfigureAwait(false))
                {
                    await conn.OpenAsync(ct).ConfigureAwait(false);
                    conn.Notification += (_, e) =>
                    {
                        if (e.Channel == channel && e.Payload.StartsWith(prefix, StringComparison.Ordinal))
                        {
                            Interlocked.Increment(ref NotificationsReceived);
                            onWake(e.Payload[prefix.Length..]);
                        }
                    };
                    using (var subscribe = new NpgsqlCommand("LISTEN " + channel, conn))
                    {
                        await subscribe.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }

                    backoff = TimeSpan.FromSeconds(1);
                    Interlocked.Increment(ref ListenerSubscriptions);
                    onSubscribed();
                    while (!ct.IsCancellationRequested)
                    {
                        if (!await conn.WaitAsync(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false))
                        {
                            // Quiet for 30 s: prove the session is alive, so a dead one is replaced (and resubscribed).
                            using var ping = new NpgsqlCommand("SELECT 1", conn);
                            await ping.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException or InvalidOperationException
                or System.Net.Sockets.SocketException or OperationCanceledException)
            {
                // Lost or refused session: polling covers the gap; resubscribe after a backoff (1 s doubling to 30 s).
                onLost();
                try
                {
                    await Task.Delay(backoff, time, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(30).Ticks));
            }
        }
    }
}

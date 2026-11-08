using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Management;

namespace Runwick.AspNetCore;

public static class RunwickBasicDefaults
{
    public const string Scheme = "Basic";
}

public sealed class RunwickBasicOptions : AuthenticationSchemeOptions
{
    /// <summary>Required: checks a user name and password. There are no default credentials.</summary>
    public Func<string, string, CancellationToken, ValueTask<bool>>? Verifier { get; set; }

    public override void Validate()
    {
        base.Validate();
        if (Verifier is null)
        {
            throw new InvalidOperationException("AddRunwickBasic requires an explicit Verifier.");
        }
    }
}

/// <summary>Counts Basic failures per (source address, user name): 10 per minute, then refused without verifying.</summary>
internal sealed class BasicFailureLimiter(TimeProvider? time = null)
{
    const int Limit = 10;
    static readonly TimeSpan Window = TimeSpan.FromMinutes(1);
    readonly ConcurrentDictionary<string, (DateTimeOffset Start, int Count)> _failures = new(StringComparer.Ordinal);
    readonly TimeProvider _time = time ?? TimeProvider.System;

    public bool Blocked(string bucket) =>
        _failures.TryGetValue(bucket, out var f) && _time.GetUtcNow() - f.Start < Window && f.Count >= Limit;

    public void Failed(string bucket)
    {
        var now = _time.GetUtcNow();
        if (_failures.Count > 10_000)
        {
            foreach (var stale in _failures.Where(f => now - f.Value.Start >= Window).Select(f => f.Key).ToList())
            {
                _failures.TryRemove(stale, out _);
            }
        }

        _failures.AddOrUpdate(bucket, (now, 1), (_, f) => now - f.Start >= Window ? (now, 1) : (f.Start, f.Count + 1));
    }
}

internal sealed class RunwickBasicHandler(IOptionsMonitor<RunwickBasicOptions> options, ILoggerFactory logger, UrlEncoder encoder, BasicFailureLimiter limiter)
    : AuthenticationHandler<RunwickBasicOptions>(options, logger, encoder)
{
    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!AuthenticationHeaderValue.TryParse(Request.Headers.Authorization, out var header)
            || !string.Equals(header.Scheme, "Basic", StringComparison.OrdinalIgnoreCase))
        {
            return AuthenticateResult.NoResult();
        }

        if (!Request.IsHttps)
        {
            return AuthenticateResult.Fail("Basic authentication requires HTTPS.");
        }

        string user, password;
        try
        {
            var text = Encoding.UTF8.GetString(Convert.FromBase64String(header.Parameter ?? ""));
            var colon = text.IndexOf(':', StringComparison.Ordinal);
            if (colon <= 0)
            {
                return AuthenticateResult.Fail("Malformed Basic credentials.");
            }

            (user, password) = (text[..colon], text[(colon + 1)..]);
        }
        catch (FormatException)
        {
            return AuthenticateResult.Fail("Malformed Basic credentials.");
        }

        var bucket = Context.Connection.RemoteIpAddress + "|" + user;
        if (limiter.Blocked(bucket))
        {
            return AuthenticateResult.Fail("Too many failed attempts.");
        }

        if (!await Options.Verifier!(user, password, Context.RequestAborted).ConfigureAwait(false))
        {
            limiter.Failed(bucket);
            return AuthenticateResult.Fail("Invalid credentials.");
        }

        var identity = new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, user)], Scheme.Name);
        return AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), Scheme.Name));
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        Response.StatusCode = 401;
        Response.Headers.WWWAuthenticate = "Basic realm=\"Runwick\", charset=\"UTF-8\"";
        return Task.CompletedTask;
    }
}

/// <summary>
/// Process-local limits (16 §5): per-principal token buckets for reads, mutations and reveals (burst a quarter of
/// the per-minute rate), at most <see cref="ManagementOptions.MaxTrackedPrincipals"/> tracked principals and then one
/// shared restricted bucket; and the instance's active/waiting request limit.
/// </summary>
internal sealed class ManagementLimiter(ManagementOptions options, TimeProvider time) : IDisposable
{
    public enum Kind { Read, Mutation, Reveal }

    readonly ConcurrentDictionary<string, Bucket[]> _principals = new(StringComparer.Ordinal);
    readonly Bucket[] _shared = NewBuckets(options, time);
    readonly SemaphoreSlim _active = new(options.MaxActiveRequests, options.MaxActiveRequests);
    int _waiting;

    public int TrackedPrincipals => _principals.Count;

    static Bucket[] NewBuckets(ManagementOptions o, TimeProvider time) =>
        [new(o.ReadsPerMinute, time), new(o.MutationsPerMinute, time), new(o.RevealsPerMinute, time)];

    /// <summary>Null when allowed, else the Retry-After in whole seconds.</summary>
    public int? TryTake(string principal, Kind kind)
    {
        if (!_principals.TryGetValue(principal, out var buckets))
        {
            if (_principals.Count >= options.MaxTrackedPrincipals)
            {
                // Forget principals whose buckets are full again (idle); if none, use the shared bucket.
                foreach (var idle in _principals.Where(p => p.Value.All(b => b.IsFull)).Select(p => p.Key).ToList())
                {
                    _principals.TryRemove(idle, out _);
                }
            }

            buckets = _principals.Count < options.MaxTrackedPrincipals ? _principals.GetOrAdd(principal, _ => NewBuckets(options, time)) : _shared;
        }

        return buckets[(int)kind].TryTake();
    }

    /// <summary>False when the instance is busy: too many waiting, or no slot within the wait.</summary>
    public async Task<bool> EnterAsync(CancellationToken ct)
    {
        if (_active.Wait(0, CancellationToken.None))
        {
            return true;
        }

        if (Interlocked.Increment(ref _waiting) > options.MaxWaitingRequests)
        {
            Interlocked.Decrement(ref _waiting);
            return false;
        }

        try
        {
            return await _active.WaitAsync(options.MaxWait, ct).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _waiting);
        }
    }

    public void Exit() => _active.Release();

    public void Dispose() => _active.Dispose();

    sealed class Bucket(int perMinute, TimeProvider time)
    {
        readonly double _capacity = Math.Max(1, perMinute / 4.0);
        readonly double _perSecond = perMinute / 60.0;
        double _tokens = Math.Max(1, perMinute / 4.0);
        long _at = time.GetTimestamp();

        public bool IsFull
        {
            get
            {
                lock (this)
                {
                    Refill();
                    return _tokens >= _capacity;
                }
            }
        }

        public int? TryTake()
        {
            lock (this)
            {
                Refill();
                if (_tokens >= 1)
                {
                    _tokens -= 1;
                    return null;
                }

                return (int)Math.Ceiling((1 - _tokens) / _perSecond);
            }
        }

        void Refill()
        {
            var now = time.GetTimestamp();
            _tokens = Math.Min(_capacity, _tokens + time.GetElapsedTime(_at, now).TotalSeconds * _perSecond);
            _at = now;
        }
    }
}

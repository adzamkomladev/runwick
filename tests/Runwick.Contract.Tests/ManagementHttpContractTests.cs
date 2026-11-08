using System.Net;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection;
using Runwick.AspNetCore.Tests;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.ManagementHttpSupport;

namespace Runwick.Contract.Tests;

/// <summary>
/// MGMT-01, 02, 05, 07, 08, 10, 11 and 12 (16 §8) on every store: the cases of Runwick.AspNetCore.Tests over the same management
/// host with PostgreSQL, SQLite and Redis behind it. MGMT-10's instance limit is a host-side limiter and stays with the PostgreSQL run.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class ManagementHttpContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly Guid Id = Guid.NewGuid();

    /// <summary>Every route of 16 §5 with a syntactically valid target.</summary>
    static readonly (HttpMethod Method, string Path)[] Routes =
    [
        (HttpMethod.Get, "antiforgery"), (HttpMethod.Get, "jobs"), (HttpMethod.Get, $"jobs/{Id}"),
        (HttpMethod.Post, $"jobs/{Id}/payload:reveal"), (HttpMethod.Post, $"attempts/{Id}/error:reveal"),
        (HttpMethod.Post, $"jobs/{Id}/cancel"), (HttpMethod.Post, $"jobs/{Id}/retry"), (HttpMethod.Post, $"jobs/{Id}/rerun"), (HttpMethod.Post, "jobs:cancel"),
        (HttpMethod.Get, "queues"), (HttpMethod.Post, "queues/default/pause"), (HttpMethod.Post, "queues/default/resume"),
        (HttpMethod.Get, "workers"), (HttpMethod.Get, "scopes"), (HttpMethod.Post, "scopes/queue/default/capacity"),
        (HttpMethod.Get, "schedules"), (HttpMethod.Post, "schedules/s/pause"), (HttpMethod.Post, "schedules/s/resume"),
        (HttpMethod.Post, "schedules/s/trigger"), (HttpMethod.Post, "schedules/s/revalidate"),
        (HttpMethod.Get, "audit"), (HttpMethod.Get, "settings"),
    ];

    static HttpRequestMessage Req(MgmtHost h, HttpMethod method, string path, string scheme = "https") =>
        new(method, $"{scheme}://localhost{h.Base}{path}")
        {
            Content = method == HttpMethod.Post ? new StringContent("{}", System.Text.Encoding.UTF8, "application/json") : null,
        };

    // ------------------------------------------------------------------ MGMT-01, 02, 07, 08

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-01")]
    public async Task Every_route_needs_one_of_the_listed_schemes_and_the_host_is_unchanged(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt01"), kind);
        await using var h = await MgmtHost.StartAsync(f, browser: true);
        Users.Seed(h);
        await h.DispatchAsync(new PingJob("secret-payload"));

        foreach (var (method, path) in Routes)
        {
            // No credentials; an unlisted scheme; a bad Basic password; a good Basic password over HTTP.
            var none = Req(h, method, path);
            var wrong = Req(h, method, path);
            wrong.Headers.Add("X-Test-User", Users.Admin);
            wrong.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "token");
            var bad = Req(h, method, path);
            bad.Headers.Authorization = MgmtHost.Basic("mallory", "wrong");
            var plain = Req(h, method, path, "http");
            plain.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
            foreach (var request in new[] { none, wrong, bad, plain })
            {
                var (status, body, response) = await h.SendAsync(request);
                Assert.True(status == HttpStatusCode.Unauthorized, $"{method} {path}: {status}");
                Assert.Equal("unauthenticated", body!["code"]!.GetValue<string>());
                Assert.Equal(2, body.AsObject().Count);
                Assert.Null(response.Headers.Location);
                Assert.Equal("Basic", response.Headers.WwwAuthenticate.Single().Scheme);
            }
        }

        Assert.Equal(0, await Audits(f));

        // Failures are limited per source and user: mallory, after 22 failures, is refused even with the right password.
        var limited = Req(h, HttpMethod.Get, "settings");
        limited.Headers.Authorization = MgmtHost.Basic("mallory", "pw-mallory");
        Assert.Equal(HttpStatusCode.Unauthorized, (await h.SendAsync(limited)).Status);

        // The good password over HTTPS works; the host's default cookie scheme still redirects its own routes.
        var ok = Req(h, HttpMethod.Get, "settings");
        ok.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
        Assert.Equal(HttpStatusCode.OK, (await h.SendAsync(ok)).Status);
        var hello = await h.Client.GetAsync(new Uri("https://localhost/hello"), Ct);
        Assert.Equal(HttpStatusCode.Redirect, hello.StatusCode);
        Assert.Contains("/login-page", hello.Headers.Location!.ToString(), StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.OK, (await h.Client.GetAsync(new Uri("https://localhost/public"), Ct)).StatusCode);
    }

    static async Task<string> LoginAsync(MgmtHost h, string user)
    {
        var r = await h.Client.GetAsync(new Uri($"https://localhost/login/{user}"), Ct);
        return string.Join("; ", r.Headers.GetValues("Set-Cookie").Select(c => c.Split(';')[0]));
    }

    /// <summary>The antiforgery request token and the cookies it pairs with, for an authenticated caller.</summary>
    static async Task<(string Token, string Cookies)> AntiforgeryAsync(MgmtHost h, Action<HttpRequestMessage> authenticate, string cookies = "")
    {
        var req = Req(h, HttpMethod.Get, "antiforgery");
        authenticate(req);
        if (cookies.Length > 0)
        {
            req.Headers.Add("Cookie", cookies);
        }

        var (status, body, response) = await h.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, status);
        var set = response.Headers.GetValues("Set-Cookie").Select(c => c.Split(';')[0]);
        return (body!["token"]!.GetValue<string>(), string.Join("; ", cookies.Length > 0 ? set.Prepend(cookies) : set));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-02")]
    public async Task Browser_mutations_need_the_antiforgery_token_and_nothing_skips_it(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt02"), kind);
        await using var h = await MgmtHost.StartAsync(f, browser: true);
        Users.Seed(h);
        var cookie = await LoginAsync(h, Users.Admin);
        var (cookieToken, cookieJar) = await AntiforgeryAsync(h, _ => { }, cookie);
        var basic = MgmtHost.Basic(Users.Admin, "pw-admin");
        var (basicToken, basicJar) = await AntiforgeryAsync(h, r => r.Headers.Authorization = basic);

        async Task<HttpStatusCode> PauseAsync(Action<HttpRequestMessage> authenticate, string? token, bool inertia = false)
        {
            var req = Req(h, HttpMethod.Post, "queues/default/pause");
            req.Content = new StringContent($$"""{"commandId":"{{Guid.NewGuid()}}","expectedRevision":1}""", System.Text.Encoding.UTF8, "application/json");
            authenticate(req);
            if (token is not null)
            {
                req.Headers.Add("X-Runwick-Antiforgery", token);
            }

            if (inertia)
            {
                req.Headers.Add("X-Inertia", "true");
            }

            return (await h.SendAsync(req)).Status;
        }

        void Cookie(HttpRequestMessage r) => r.Headers.Add("Cookie", cookieJar);
        void Basic(HttpRequestMessage r)
        {
            r.Headers.Authorization = basic;
            r.Headers.Add("Cookie", basicJar);
        }

        foreach (var auth in new Action<HttpRequestMessage>[] { Cookie, Basic })
        {
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, null));
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, "not-a-token"));
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, null, inertia: true));
        }

        // Nothing changed: no audit, still revision 1 and not paused.
        Assert.Equal(0, await Audits(f));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_controls WHERE queue = 'default' AND NOT paused AND revision = 1"));

        // With the token the same request applies (cookie), and a token issued to the Basic caller works for Basic.
        Assert.Equal(HttpStatusCode.OK, await PauseAsync(Cookie, cookieToken));
        Assert.Equal(HttpStatusCode.Conflict, await PauseAsync(Basic, basicToken)); // stale revision: reached the store

        // A mutation path is never GET.
        var get = Req(h, HttpMethod.Get, "queues/default/pause");
        Cookie(get);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, (await h.SendAsync(get)).Status);

        // A machine-client group with a cookie or Basic scheme fails at startup.
        await using var machine = await DriverFixture.StartAsync(Ns("mgmt02m"), kind);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => MgmtHost.StartAsync(machine, browser: true, machineGroup: true));
        Assert.Contains("ForMachineClients", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-07")]
    public async Task Two_hosts_in_one_process_keep_their_own_prefixes_grants_and_results(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt07"), kind);
        await using var a = await MgmtHost.StartAsync(f, prefix: "/ops/a", routes: app => app.MapGet("/ops/sibling", () => "mvc"));
        await using var b = await MgmtHost.StartAsync(f, prefix: "/ops/b", identity: "mgmtb");
        a.Grants["u1"] = ManagementRoles.Operator;
        a.Grants["u2"] = ManagementRoles.Viewer;
        b.Grants["u1"] = [];
        var jobs = await Task.WhenAll(Enumerable.Range(0, 4).Select(i => a.DispatchAsync(new PingJob("j" + i))));

        // Two callers in parallel on host A: the operator's cancels apply, the viewer's are refused.
        var results = await Task.WhenAll(jobs.Select((job, i) => a.PostAsync($"jobs/{job}/cancel", i % 2 == 0 ? "u1" : "u2",
            new { commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 })));
        Assert.Equal([HttpStatusCode.OK, HttpStatusCode.Forbidden, HttpStatusCode.OK, HttpStatusCode.Forbidden], results.Select(r => r.Status));
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'u1'"));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'u2'"));

        // Host B maps the same user to nothing; host A's prefix is not B's; the sibling route is the host's own.
        Assert.Equal(HttpStatusCode.Forbidden, (await b.GetAsync("jobs", "u1")).Status);
        Assert.Equal(HttpStatusCode.OK, (await a.GetAsync("jobs", "u1")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await a.GetAsync("/ops/b/v1/jobs", "u1")).Status);
        var sibling = await a.Client.GetAsync(new Uri("http://localhost/ops/sibling"), Ct);
        Assert.Equal("mvc", await sibling.Content.ReadAsStringAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-08")]
    public async Task Forwarded_proto_from_a_known_proxy_enables_basic_and_path_base_works_without_redirects(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt08"), kind);
        await using var h = await MgmtHost.StartAsync(f, browser: true, middleware: app =>
        {
            app.Use((ctx, next) =>
            {
                ctx.Connection.RemoteIpAddress = IPAddress.Parse(ctx.Request.Headers["X-Test-Remote"].ToString() is { Length: > 0 } ip ? ip : "10.0.0.9");
                return next(ctx);
            });
            var forwarded = new ForwardedHeadersOptions { ForwardedHeaders = ForwardedHeaders.XForwardedProto };
            forwarded.KnownIPNetworks.Clear();
            forwarded.KnownProxies.Clear();
            forwarded.KnownProxies.Add(IPAddress.Parse("10.0.0.1"));
            app.UseForwardedHeaders(forwarded);
            app.UsePathBase("/base");
        });
        Users.Seed(h);

        async Task<HttpStatusCode> SettingsAsync(string remote, string path = "/base/runwick/api/v1/settings")
        {
            var req = new HttpRequestMessage(HttpMethod.Get, "http://localhost" + path);
            req.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
            req.Headers.Add("X-Forwarded-Proto", "https");
            req.Headers.Add("X-Test-Remote", remote);
            var (status, _, response) = await h.SendAsync(req);
            Assert.Null(response.Headers.Location);
            return status;
        }

        Assert.Equal(HttpStatusCode.OK, await SettingsAsync("10.0.0.1"));
        Assert.Equal(HttpStatusCode.Unauthorized, await SettingsAsync("10.0.0.2"));
        Assert.Equal(HttpStatusCode.OK, await SettingsAsync("10.0.0.1", "/runwick/api/v1/settings"));

        // A cookie-only caller without a cookie gets 401, never the cookie scheme's login redirect.
        var anonymous = await h.Client.GetAsync(new Uri("https://localhost/base/runwick/api/v1/jobs"), Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
        Assert.Null(anonymous.Headers.Location);
    }

    // ------------------------------------------------------------------ MGMT-05

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-05")]
    public async Task A_client_command_id_replays_conflicts_and_resolves_a_dropped_reply(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt05"), kind);
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        h.Grants["op"] = ManagementRoles.Operator;
        var job = await h.DispatchAsync(new PingJob("a"));
        var p = await PositionAsync(h, job);
        var id = Guid.NewGuid();

        var first = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        var second = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        Assert.Equal((HttpStatusCode.OK, false), (first.Status, first.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal((HttpStatusCode.OK, true), (second.Status, second.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal(first.Body["revision"]!.GetValue<long>(), second.Body["revision"]!.GetValue<long>());
        Assert.Equal(1, await Audits(f, id));

        var other = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "a different reason"));
        Assert.Equal((HttpStatusCode.Conflict, "command_conflict"), (other.Status, other.Body!["code"]!.GetValue<string>()));

        // The grant is removed: the replay is refused before the store, and discloses nothing of the receipt.
        h.Grants["op"] = ManagementRoles.Viewer;
        var denied = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        Assert.Equal(HttpStatusCode.Forbidden, denied.Status);
        Assert.Equal(["code", "message"], denied.Body!.AsObject().Select(x => x.Key).Order());
        Assert.Equal(1, await Audits(f, id));

        // A dropped reply after commit: the session resends the same command and the receipt answers; one transition, one audit row.
        h.Grants["op"] = ManagementRoles.Operator;
        var retryJob = await h.DispatchAsync(new PingJob("b"));
        var rp = await PositionAsync(h, retryJob);
        var dropId = Guid.NewGuid();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(dropId));
        var resolved = await h.PostAsync($"jobs/{retryJob}/cancel", "op", Control(dropId, rp));
        Assert.Equal(1, drop.Fired);
        Assert.Equal((HttpStatusCode.OK, true), (resolved.Status, resolved.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal((1L, 1L), (await Audits(f, dropId), await Receipts(f, dropId)));
        Assert.Equal(rp.Revision + 1, (await PositionAsync(h, retryJob)).Revision);

        // The client may also resend after an unknown outcome: the same ID and body replays.
        var resent = await h.PostAsync($"jobs/{retryJob}/cancel", "op", Control(dropId, rp));
        Assert.Equal((HttpStatusCode.OK, true), (resent.Status, resent.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal(1, await Audits(f, dropId));
    }

    // ------------------------------------------------------------------ MGMT-10

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-10")]
    public async Task Pages_ranges_and_bulk_are_bounded(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt10a"), kind);
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        foreach (var chunk in Enumerable.Range(0, 205).Chunk(100))
        {
            await h.Dispatcher.DispatchManyAsync([.. chunk.Select(i => new BatchDispatchItem(new PingJob("p" + i)))], cancellationToken: Ct);
        }

        var page = (await h.GetAsync("jobs?limit=500", Users.Admin)).Body!;
        Assert.Equal(200, page["items"]!.AsArray().Count);
        var rest = (await h.GetAsync("jobs?limit=500&cursor=" + page["next"]!.GetValue<string>(), Users.Admin)).Body!;
        Assert.Equal(5, rest["items"]!.AsArray().Count);
        Assert.Null(rest["next"]);
        Assert.Null(page["total"]);

        var from = Uri.EscapeDataString(DateTimeOffset.UtcNow.AddDays(-40).ToString("O"));
        Assert.Equal(HttpStatusCode.BadRequest, (await h.GetAsync($"jobs?from={from}", Users.Admin)).Status);

        var items = Enumerable.Range(0, 101).Select(_ => new { jobId = Guid.NewGuid(), commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 });
        var bulk = await h.PostAsync("jobs:cancel", Users.Admin, new { items });
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_request"), (bulk.Status, bulk.Body!["code"]!.GetValue<string>()));
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, (await h.PostAsync("jobs:cancel", Users.Admin, new { reason = new string('x', 70_000) })).Status);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await h.SendAsync(new HttpRequestMessage(HttpMethod.Post, h.Base + "jobs:cancel")
        {
            Headers = { { "X-Test-User", Users.Admin } },
            Content = new StringContent("items=1", System.Text.Encoding.UTF8, "application/x-www-form-urlencoded"),
        })).Status);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-10")]
    public async Task Bursts_over_the_per_principal_rates_get_429_with_retry_after(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt10b"), kind);
        await using var h = await MgmtHost.StartAsync(f, options: _ => { }); // the defaults: 120 reads, 30 mutations, 10 reveals per minute
        Users.Seed(h);

        // Burst is a quarter of the per-minute rate; tokens keep refilling (real time) while the burst runs, so up to
        // elapsed × rate extra requests may pass.
        async Task AllowedAsync(int burst, int perMinute, Func<Task<(HttpStatusCode Status, System.Text.Json.Nodes.JsonNode? Body, HttpResponseMessage Response)>> send)
        {
            var started = System.Diagnostics.Stopwatch.StartNew();
            for (var n = 0; ; n++)
            {
                var (status, _, response) = await send();
                if (status == HttpStatusCode.TooManyRequests)
                {
                    Assert.True(int.Parse(response.Headers.GetValues("Retry-After").Single(), System.Globalization.CultureInfo.InvariantCulture) >= 1);
                    var refilled = (int)Math.Ceiling(started.Elapsed.TotalMinutes * perMinute);
                    Assert.InRange(n, burst, burst + refilled);
                    return;
                }
            }
        }

        await AllowedAsync(30, 120, () => h.GetAsync("settings", Users.Admin));
        await AllowedAsync(7, 30, () => h.PostAsync("queues/default/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = 99 }));
        await AllowedAsync(2, 10, () => h.PostAsync($"jobs/{Guid.NewGuid()}/payload:reveal", Users.Admin));

        // Another principal has its own buckets.
        Assert.Equal(HttpStatusCode.OK, (await h.GetAsync("settings", Users.Viewer)).Status);
    }

    // ------------------------------------------------------------------ MGMT-11

    static object Policy(Guid commandId, long expectedRevision, int completed = 168, int failed = 720, int history = 2160) => new
    {
        commandId, expectedRevision, completedDetailHours = completed, failedDetailHours = failed, jobHistoryHours = history, auditHours = 2160,
        scalingEventsHours = 168, scheduleOccurrencesHours = 720, workerPresenceHours = 24, reason = "tighten",
    };

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-11")]
    public async Task Retention_over_http_expiry_purge_policy_and_permissions(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt11"), kind);
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("old"));
        await f.FailNextAsync("secret text");
        await RetentionContractTests.AgeJobsAsync(f, TimeSpan.FromDays(31));
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        await RetentionContractTests.PassAsync(c, RecordClasses.JobDetailFailed);

        // After detail expiry: identity, state, generations and error codes stay; the reveal is 410.
        var (status, body, _) = await h.GetAsync($"jobs/{job}", Users.Admin);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.NotNull(body!["job"]!["detailExpiredAt"]);
        Assert.Equal("failed", body["job"]!["state"]!.GetValue<string>());
        Assert.Single(body["generations"]!.AsArray());
        Assert.Equal("boom", body["attempts"]![0]!["errorCode"]!.GetValue<string>());
        var reveal = await h.PostAsync($"jobs/{job}/payload:reveal", Users.Admin, new { reason = "look" });
        Assert.Equal((HttpStatusCode.Gone, "detail_expired"), (reveal.Status, reveal.Body!["code"]!.GetValue<string>()));

        // After purge: 404, the same as an ID that never existed.
        await RetentionContractTests.AgeJobsAsync(f, TimeSpan.FromDays(90));
        await RetentionContractTests.PassAsync(c, RecordClasses.JobHistory);
        Assert.Equal(HttpStatusCode.NotFound, (await h.GetAsync($"jobs/{job}", Users.Admin)).Status);

        // GET retention: policy and progress; Settings.Read required.
        var view = await h.GetAsync("retention", Users.Viewer);
        Assert.Equal(HttpStatusCode.OK, view.Status);
        Assert.Equal((720, 0L), (view.Body!["policy"]!["failedDetailHours"]!.GetValue<int>(), view.Body["revision"]!.GetValue<long>()));
        Assert.Equal(RecordClasses.All.Count, view.Body["classes"]!.AsArray().Count);
        Assert.Contains(view.Body["classes"]!.AsArray(), x => x!["recordClass"]!.GetValue<string>() == "job_history" && x["totalRows"]!.GetValue<long>() == 1);
        h.Grants[Users.Nobody] = [new ManagementGrant(ManagementPermissions.JobsRead)];
        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("retention", Users.Nobody)).Status);

        // POST retention/policy: 403 without Retention.Manage, 400 on an invalid order, 200 once, 409 when stale.
        Assert.Equal(HttpStatusCode.Forbidden, (await h.PostAsync("retention/policy", Users.Viewer, Policy(Guid.NewGuid(), 0))).Status);
        var invalid = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, history: 100));
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_retention_policy"), (invalid.Status, invalid.Body!["code"]!.GetValue<string>()));
        var applied = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, completed: 24));
        Assert.Equal((HttpStatusCode.OK, 1L), (applied.Status, applied.Body!["revision"]!.GetValue<long>()));
        var stale = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, completed: 48));
        Assert.Equal((HttpStatusCode.Conflict, 1L), (stale.Status, stale.Body!["actualRevision"]!.GetValue<long>()));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'EditRetentionPolicy'"));
        var edited = await h.GetAsync("retention", Users.Viewer);
        Assert.Equal((24, 1L), (edited.Body!["policy"]!["completedDetailHours"]!.GetValue<int>(), edited.Body["revision"]!.GetValue<long>()));
        Assert.Equal(1L, (await h.GetAsync("settings", Users.Viewer)).Body!["retentionPolicyRevision"]!.GetValue<long>());
    }

    // ------------------------------------------------------------------ MGMT-12

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-12")]
    public async Task A_management_only_host_serves_every_read_and_control_without_presence(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt12"), kind);
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("a"));

        foreach (var path in new[] { "jobs", $"jobs/{job}", "queues", "workers?includeExpired=true", "scopes", "schedules", "audit", "settings" })
        {
            var (status, body, response) = await h.GetAsync(path, Users.Admin);
            Assert.True(status == HttpStatusCode.OK, $"{path}: {status} {body}");
            Assert.Equal("no-store, private", response.Headers.CacheControl!.ToString());
            Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        }

        var settings = (await h.GetAsync("settings", Users.Admin)).Body!;
        Assert.Equal(f.Application, settings["application"]!.GetValue<string>());
        Assert.DoesNotContain("worker", settings["roles"]!.AsArray().Select(r => r!.GetValue<string>()));

        var revealed = await h.PostAsync($"jobs/{job}/payload:reveal", Users.Admin, new { reason = "support" });
        Assert.Equal(HttpStatusCode.OK, revealed.Status);
        Assert.Equal("a", revealed.Body!["payload"]!["id"]!.GetValue<string>());

        var p = await PositionAsync(h, job);
        var cancelled = await h.PostAsync($"jobs/{job}/cancel", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedGeneration = p.Generation, expectedRevision = p.Revision });
        Assert.Equal(HttpStatusCode.OK, cancelled.Status);
        p = await PositionAsync(h, job);
        var retried = await h.PostAsync($"jobs/{job}/retry", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedGeneration = p.Generation, expectedRevision = p.Revision });
        Assert.Equal(HttpStatusCode.OK, retried.Status);

        var queues = (await h.GetAsync("queues", Users.Admin)).Body!["items"]!.AsArray();
        var revision = queues.Single(q => q!["name"]!.GetValue<string>() == "default")!["revision"]!.GetValue<long>();
        var paused = await h.PostAsync("queues/default/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = revision });
        Assert.Equal(HttpStatusCode.OK, paused.Status);
        var resumed = await h.PostAsync("queues/default/resume", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = paused.Body!["revision"]!.GetValue<long>() });
        Assert.Equal(HttpStatusCode.OK, resumed.Status);

        var scope = (await h.GetAsync("scopes?kind=job-type", Users.Admin)).Body!["items"]!.AsArray()
            .Single(s => s!["key"]!.GetValue<string>() == "test.capped")!;
        var capacity = await h.PostAsync("scopes/job-type/test.capped/capacity", Users.Admin,
            new { commandId = Guid.NewGuid(), capacity = 2, expectedPolicyRevision = scope["policyRevision"]!.GetValue<long>() });
        Assert.Equal(HttpStatusCode.OK, capacity.Status);

        // The schedule is registered elsewhere (a scheduler-side client); this host only controls it.
        await f.RegisterScheduleAsync("nightly");
        var (listStatus, listed, listResponse) = await h.GetAsync("schedules", Users.Admin);
        Assert.True(listStatus == HttpStatusCode.OK && listed is not null, $"schedules: {listStatus} {await listResponse.Content.ReadAsStringAsync(Ct)}");
        var schedule = listed!["items"]!.AsArray().FirstOrDefault() ?? throw new Xunit.Sdk.XunitException("no schedule listed: " + listed.ToJsonString());
        var control = schedule["controlRevision"]!.GetValue<long>();
        var definition = schedule["definitionRevision"]!.GetValue<long>();
        var pausedSchedule = await h.PostAsync("schedules/nightly/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = control });
        Assert.Equal(HttpStatusCode.OK, pausedSchedule.Status);
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync("schedules/nightly/resume", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = pausedSchedule.Body!["controlRevision"]!.GetValue<long>(), policy = "skipPaused" })).Status);
        var trigger = Guid.NewGuid();
        var triggered = await h.PostAsync("schedules/nightly/trigger", Users.Admin, new { commandId = trigger, expectedRevision = definition });
        Assert.Equal(HttpStatusCode.OK, triggered.Status);
        Assert.NotNull(triggered.Body!["jobId"]);
        var again = await h.PostAsync("schedules/nightly/trigger", Users.Admin, new { commandId = trigger, expectedRevision = definition });
        Assert.Equal((true, triggered.Body["jobId"]!.GetValue<Guid>()), (again.Body!["replayed"]!.GetValue<bool>(), again.Body["jobId"]!.GetValue<Guid>()));
        Assert.Equal(HttpStatusCode.NotFound, (await h.PostAsync("schedules/missing/pause", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = 1 })).Status);

        var bulk = await h.PostAsync("jobs:cancel", Users.Admin, new { items = new[] { new { jobId = Guid.NewGuid(), commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 } } });
        Assert.Equal(HttpStatusCode.OK, bulk.Status);
        Assert.Equal("not_found", bulk.Body!["items"]![0]!["status"]!.GetValue<string>());

        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.worker_instances"));
        Assert.Equal(6, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE target_kind IN ('job', 'queue', 'scope')"));
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE target_kind = 'schedule'"));
    }
}

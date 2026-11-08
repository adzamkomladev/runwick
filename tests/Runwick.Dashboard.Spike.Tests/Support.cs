using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Runwick.Dashboard.Spike.Tests;

/// <summary>In-memory hosts and a small browser-like client (it keeps cookies and reads the page) for the DASH cases.</summary>
internal static partial class Support
{
    public const string Hostile = "</script><script>alert(1)</script>\"'&<img src=x onerror=alert(2)>\u2028end";

    public static async Task<WebApplication> StartAsync(Action<WebApplicationBuilder> configure, Action<WebApplication> pipeline, string? contentRoot = null)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ContentRootPath = contentRoot ?? AppContext.BaseDirectory,
            EnvironmentName = "Production",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        configure(builder);
        var app = builder.Build();
        pipeline(app);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    /// <summary>A Basic scheme with alice (operate), victor (view only) and the given extra users.</summary>
    public static void AddBasic(this IServiceCollection services, string scheme, params string[] operators)
    {
        services.AddAuthentication().AddScheme<SpikeBasicOptions, SpikeBasicHandler>(scheme, o =>
        {
            o.Users["alice"] = ("pw-alice", ["view", "operate"]);
            o.Users["victor"] = ("pw-victor", ["view"]);
            foreach (var name in operators)
            {
                o.Users[name] = ("pw-" + name, ["view", "operate"]);
            }
        });
    }

    /// <summary>A Basic scheme that knows only <paramref name="user"/>, as an operator.</summary>
    public static void AddBasicOnly(this IServiceCollection services, string scheme, string user) =>
        services.AddAuthentication().AddScheme<SpikeBasicOptions, SpikeBasicHandler>(scheme, o => o.Users[user] = ("pw-" + user, ["view", "operate"]));

    public static void Use(this DashboardMountOptions options, string mountId, string scheme, IDashboardSpikeData data, string? title = null)
    {
        options.MountId = mountId;
        options.Title = title ?? mountId;
        options.Data = data;
        options.ViewPolicy = new AuthorizationPolicyBuilder(scheme).RequireClaim("runwick:permission", "view", "operate").Build();
        options.OperatePolicy = new AuthorizationPolicyBuilder(scheme).RequireClaim("runwick:permission", "operate").Build();
    }

    public static InMemoryDashboardSpikeData Data(string name = "alpha") => new InMemoryDashboardSpikeData().Add("1", name, "secret-payload-" + name);

    public static Browser Browser(this WebApplication app, string? user = null) => new(app.GetTestClient(), user);

    public static string? Header(this HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) || response.Content.Headers.TryGetValues(name, out values)
            ? string.Join(", ", values)
            : null;

    [GeneratedRegex("<meta name=\"runwick-csrf\" content=\"([^\"]*)\">")]
    public static partial Regex CsrfMeta();

    [GeneratedRegex("<script data-page=\"app\" type=\"application/json\">(.*?)</script><div id=\"app\">", RegexOptions.Singleline)]
    public static partial Regex PageScript();
}

/// <summary>Sends requests the way the Inertia client does, keeping cookies like a browser.</summary>
internal sealed class Browser(HttpClient client, string? user)
{
    private readonly Dictionary<string, string> _cookies = new(StringComparer.Ordinal);

    public string? User { get; set; } = user;

    public string? Csrf { get; set; }

    public string? Version { get; set; }

    public IReadOnlyDictionary<string, string> Cookies => _cookies;

    /// <summary>A full page load: reads the token and the version from the HTML.</summary>
    public async Task<(HttpResponseMessage Response, string Body, JsonElement Page)> LoadAsync(string url)
    {
        var response = await SendAsync(HttpMethod.Get, url, inertia: false);
        var body = await response.Content.ReadAsStringAsync();
        var page = default(JsonElement);
        if (Support.PageScript().Match(body) is { Success: true } script)
        {
            page = JsonDocument.Parse(script.Groups[1].Value).RootElement.Clone();
            Version = page.GetProperty("version").GetString();
        }

        if (Support.CsrfMeta().Match(body) is { Success: true } meta)
        {
            Csrf = WebUtility.HtmlDecode(meta.Groups[1].Value);
        }

        return (response, body, page);
    }

    /// <summary>An Inertia visit (XHR).</summary>
    public Task<HttpResponseMessage> VisitAsync(string url, Action<HttpRequestMessage>? configure = null) =>
        SendAsync(HttpMethod.Get, url, inertia: true, configure: configure);

    /// <summary>An Inertia form post with this browser's token.</summary>
    public Task<HttpResponseMessage> PostAsync(string url, long revision, string commandId = "cmd-1", Action<HttpRequestMessage>? configure = null) =>
        SendAsync(HttpMethod.Post, url, inertia: true, body: $"{{\"revision\":{revision}}}", configure: request =>
        {
            request.Headers.Add("X-Runwick-Command-Id", commandId);
            if (Csrf is not null)
            {
                request.Headers.Add("X-Runwick-Csrf", Csrf);
            }

            configure?.Invoke(request);
        });

    public async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, bool inertia, string? body = null, Action<HttpRequestMessage>? configure = null)
    {
        using var request = new HttpRequestMessage(method, url);
        if (User is not null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User}:pw-{User}")));
        }

        if (inertia)
        {
            request.Headers.Add("X-Inertia", "true");
            if (Version is not null)
            {
                request.Headers.Add("X-Inertia-Version", Version);
            }
        }

        if (_cookies.Count > 0)
        {
            request.Headers.Add("Cookie", string.Join("; ", _cookies.Select(c => $"{c.Key}={c.Value}")));
        }

        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        configure?.Invoke(request);
        var response = await client.SendAsync(request, TestContext.Current.CancellationToken);
        if (response.Headers.TryGetValues("Set-Cookie", out var cookies))
        {
            foreach (var cookie in cookies)
            {
                var pair = cookie.Split(';')[0].Split('=', 2);
                _cookies[pair[0]] = pair[1];
            }
        }

        return response;
    }

    public static async Task<JsonElement> JsonAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.Clone();
}

using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Runwick.Cli;

internal sealed record ApiResponse(int Status, JsonNode? Body, DateTimeOffset? Date);

/// <summary>
/// The HTTP side of every command (16 §5): auth, antiforgery for Basic, automatic resend of a mutation whose reply was
/// lost (19 §5.4), and the mapping of error bodies to CLI errors. Credentials and tokens live in this object only.
/// </summary>
internal sealed class ApiClient : IDisposable
{
    const string AntiforgeryHeader = "X-Runwick-Antiforgery";
    static readonly string[] Redacted = ["Authorization", "Cookie", "Set-Cookie", AntiforgeryHeader];

    readonly HttpClient _http;
    readonly CliEnvironment _env;
    readonly string _authorization;
    readonly bool _basic;
    readonly TimeSpan _timeout;
    readonly bool _verbose;
    string? _token;
    string? _cookie;

    public ApiClient(CliEnvironment env, string baseUrl, string auth, string? user, string secret, int timeoutSeconds, bool verbose)
    {
        _env = env;
        _http = new HttpClient(env.Handler ?? new SocketsHttpHandler(), disposeHandler: env.Handler is null)
        {
            BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/v1/"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        Auth = auth;
        _basic = auth == "basic";
        _authorization = _basic ? "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + secret)) : "Bearer " + secret;
        _timeout = TimeSpan.FromSeconds(timeoutSeconds);
        _verbose = verbose;
    }

    public string BaseUrl => _http.BaseAddress!.ToString();

    public string Auth { get; }

    public void Dispose() => _http.Dispose();

    public Task<ApiResponse> GetAsync(string path, CancellationToken ct) => SendOnceAsync(HttpMethod.Get, path, null, ct);

    /// <summary>A GET whose error statuses become a <see cref="CliFailure"/>.</summary>
    public async Task<JsonNode> ReadAsync(string path, string? permission, CancellationToken ct)
    {
        var r = await GetAsync(path, ct);
        return r.Status is >= 200 and < 300 ? r.Body ?? throw new CliFailure(CliErrors.Of("response_invalid", "The response has no body.")) : throw new CliFailure(ErrorOf(r, permission));
    }

    /// <summary>
    /// Sends a mutation. A lost reply or a 503 <c>outcome_unknown</c> is resent unchanged (same command ID) up to three
    /// times; if none answers the result is <c>outcome_unknown</c>. A stale antiforgery token is fetched again once.
    /// </summary>
    public async Task<ApiResponse> MutateAsync(string path, JsonNode body, CancellationToken ct)
    {
        var refreshed = false;
        for (var attempt = 0; ; attempt++)
        {
            if (_basic && _token is null)
            {
                await FetchAntiforgeryAsync(ct);
            }

            ApiResponse r;
            try
            {
                r = await SendOnceAsync(HttpMethod.Post, path, body, ct);
            }
            catch (LostReplyException) when (attempt < _env.ResendDelays.Count)
            {
                await Task.Delay(_env.ResendDelays[attempt], ct);
                continue;
            }
            catch (LostReplyException)
            {
                throw new CliFailure(CliErrors.Of("outcome_unknown", "The reply was lost and resends did not answer; resend with the same --command-id."));
            }

            if (r.Status == 503 && Code(r) == "outcome_unknown" && attempt < _env.ResendDelays.Count)
            {
                await Task.Delay(_env.ResendDelays[attempt], ct);
                continue;
            }

            if (r.Status == 400 && Code(r) == "antiforgery_invalid" && _basic && !refreshed)
            {
                (_token, refreshed) = (null, true);
                continue;
            }

            return r;
        }
    }

    async Task FetchAntiforgeryAsync(CancellationToken ct)
    {
        var r = await SendOnceAsync(HttpMethod.Get, "antiforgery", null, ct);
        if (r.Status != 200)
        {
            throw new CliFailure(ErrorOf(r, null));
        }

        _token = r.Body?["token"]?.GetValue<string>() ?? throw new CliFailure(CliErrors.Of("response_invalid", "The antiforgery response has no token."));
    }

    static string? Code(ApiResponse r) => r.Body is JsonObject o && o["code"] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

    async Task<ApiResponse> SendOnceAsync(HttpMethod method, string path, JsonNode? body, CancellationToken ct)
    {
        // "./": a path like "jobs:dispatch" would otherwise parse as a URI with scheme "jobs".
        using var request = new HttpRequestMessage(method, new Uri(_http.BaseAddress!, "./" + path));
        request.Headers.TryAddWithoutValidation("Authorization", _authorization);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        if (_cookie is not null)
        {
            request.Headers.TryAddWithoutValidation("Cookie", _cookie);
        }

        if (method == HttpMethod.Post && _basic && _token is not null)
        {
            request.Headers.TryAddWithoutValidation(AntiforgeryHeader, _token);
        }

        if (body is not null)
        {
            request.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        }

        Trace(">", method + " " + request.RequestUri, request.Headers);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(_timeout);
        HttpResponseMessage response;
        try
        {
            response = await _http.SendAsync(request, timeout.Token);
        }
        catch (HttpRequestException ex) when (ex.HttpRequestError is HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError
            or HttpRequestError.ProxyTunnelError)
        {
            throw new CliFailure(CliErrors.Of("connection_failed", "Could not connect to the server; nothing was sent."));
        }
        catch (HttpRequestException ex) when (ex.HttpRequestError == HttpRequestError.SecureConnectionError)
        {
            throw new CliFailure(CliErrors.Of("tls_error", "The TLS connection failed; nothing was sent."));
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException || (ex is OperationCanceledException && !ct.IsCancellationRequested))
        {
            // The request may have reached the server: the reply was lost (timeout, reset after write).
            throw method == HttpMethod.Post ? new LostReplyException() : new CliFailure(CliErrors.Of("connection_failed", "The server did not answer."));
        }

        using (response)
        {
            Trace("<", ((int)response.StatusCode).ToString(CultureInfo.InvariantCulture), response.Headers);
            if (response.Headers.TryGetValues("Set-Cookie", out var cookies))
            {
                // Antiforgery cookie, kept in memory for this process only.
                _cookie = string.Join("; ", cookies.Select(c => c.Split(';')[0]));
            }

            var text = await response.Content.ReadAsStringAsync(ct);
            JsonNode? json = null;
            if (text.Length > 0 && response.Content.Headers.ContentType?.MediaType == "application/json")
            {
                try
                {
                    json = JsonNode.Parse(text);
                }
                catch (JsonException)
                {
                    throw new CliFailure(CliErrors.Of("response_invalid", "The server answered with invalid JSON."));
                }
            }

            return new ApiResponse((int)response.StatusCode, json, response.Headers.Date);
        }
    }

    void Trace(string direction, string line, HttpHeaders headers)
    {
        if (!_verbose)
        {
            return;
        }

        _env.Error.WriteLine($"{direction} {line}");
        foreach (var (name, values) in headers)
        {
            _env.Error.WriteLine($"{direction} {name}: {(Redacted.Contains(name, StringComparer.OrdinalIgnoreCase) ? "[redacted]" : string.Join(", ", values))}");
        }
    }

    /// <summary>An error response as a CLI error (19 §5.6). A 404/405 without an API error body is a route this server lacks.</summary>
    public static CliError ErrorOf(ApiResponse r, string? permission)
    {
        var code = Code(r);
        if (code is null)
        {
            return r.Status is 404 or 405
                ? CliErrors.Of("endpoint_not_supported", $"The server has no such endpoint (HTTP {r.Status}).")
                : CliErrors.Of("response_invalid", $"Unexpected HTTP {r.Status} without an error body.");
        }

        var message = r.Body?["message"]?.GetValue<string>() ?? code;
        var actual = r.Body?["actualRevision"] is JsonValue a && a.TryGetValue<long>(out var rev) ? rev : (long?)null;
        return CliErrors.Of(code, message, actual, code == "forbidden" && permission is not null ? $"Ask for permission {permission}." : null);
    }

    sealed class LostReplyException : Exception;
}

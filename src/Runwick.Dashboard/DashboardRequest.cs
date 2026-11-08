using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Management;

namespace Runwick.Dashboard;

/// <summary>One request through a mount, after the gate: the trusted caller, the session and the page state.</summary>
internal sealed class DashboardRequest(DashboardMount mount, HttpContext http, ManagementCaller caller)
{
    public DashboardMount Mount { get; } = mount;

    public HttpContext Http { get; } = http;

    public ManagementCaller Caller { get; } = caller;

    public IManagementSession Session => Http.RequestServices.GetRequiredService<IManagementSession>();

    public CancellationToken Ct => Http.RequestAborted;

    /// <summary>PathBase (host prefix, trusted proxy prefix) plus the mount path.</summary>
    public string Base => BaseOf(Http);

    /// <summary>A page notice, e.g. <c>invalid_cursor</c> when a rejected cursor fell back to the first page.</summary>
    public string? Notice { get; set; }

    /// <summary>A control's outcome when it did not apply (21 §5).</summary>
    public object? Outcome { get; set; }

    // Inside the mount's branch, PathBase is the host's PathBase plus the mount path.
    public static string BaseOf(HttpContext context) => context.Request.PathBase.ToUriComponent();

    public static string[] Segments(HttpContext context) =>
        (context.Request.Path.Value ?? "").Split('/', StringSplitOptions.RemoveEmptyEntries);

    /// <summary>Whether any grant carries the permission (any queue). A display hint and a pre-check; the session decides.</summary>
    public bool Has(string permission) => Caller.Grants.Any(g => g.Permission == permission);

    /// <summary>The props a partial reload of <paramref name="component"/> asks for; null for a full page.</summary>
    public IReadOnlyList<string>? Partial(string component) =>
        Http.Request.Headers["X-Inertia-Partial-Component"] == component
            ? Http.Request.Headers["X-Inertia-Partial-Data"].ToString().Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            : null;

    public string? Q(string name) => Http.Request.Query[name] is { Count: > 0 } v && v.ToString().Length > 0 ? v.ToString() : null;

    public int? QInt(string name) => Q(name) is { } s
        ? int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : throw Invalid(name)
        : null;

    public DateTimeOffset? QTime(string name) => Q(name) is { } s
        ? DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var v) ? v : throw Invalid(name)
        : null;

    public Guid? QGuid(string name) => Q(name) is { } s ? Guid.TryParse(s, out var v) ? v : throw Invalid(name) : null;

    static ManagementValidationException Invalid(string name) => new("invalid_request", $"Query parameter '{name}' is not valid.");

    /// <summary>
    /// One list read. A rejected cursor (invalid or expired) renders the first page with a notice; another invalid filter
    /// renders an empty list with its code as the notice. Neither is an error screen (21 §4).
    /// </summary>
    public async Task<ManagementPage<T>> ListAsync<T>(Func<string?, Task<ManagementPage<T>>> read, string cursorParameter = "cursor")
    {
        var cursor = Q(cursorParameter);
        try
        {
            return await read(cursor);
        }
        catch (ManagementValidationException ex) when (ex.Code == "invalid_cursor" && cursor is not null)
        {
            Notice = ex.Code;
            return await ListAsync<T>(_ => read(null), "\0");
        }
        catch (ManagementValidationException ex)
        {
            Notice = ex.Code;
            return new ManagementPage<T>([], DateTimeOffset.UtcNow, null);
        }
    }

    // The adapter would serialize props with the host's HttpJsonOptions; a JsonElement is written as is (sp-023 gap 2).
    public static JsonElement Element(object? value) => JsonSerializer.SerializeToElement(value, DashboardMount.Json);
}

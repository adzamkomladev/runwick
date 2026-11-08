using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Runwick.Management;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// The POST pipeline (21 §5): this mount's antiforgery token (403, no session call), JSON only (415), 64 KiB (413), a
/// client command ID in <c>X-Runwick-Command-Id</c> (400). An applied control is a 303 to the resource's page; anything
/// else the session answers is that page with an <c>outcome</c> prop, never a 409 (the asset-version signal).
/// </summary>
internal static class DashboardControls
{
    const int MaxBody = 64 * 1024;

    /// <summary>The body as a JSON object, or null after answering 403, 413, 415 or 400.</summary>
    public static async Task<JsonElement?> PrepareAsync(DashboardRequest r)
    {
        var request = r.Http.Request;
        if (!r.Mount.ValidCsrf(r.Http))
        {
            await DashboardMount.WriteErrorAsync(r.Http, 403, "antiforgery_invalid", "The antiforgery token is missing or invalid.");
            return null;
        }

        if (request.ContentLength > MaxBody)
        {
            await DashboardMount.WriteErrorAsync(r.Http, 413, "payload_too_large", "The request body is too large.");
            return null;
        }

        if ((request.ContentLength > 0 || request.ContentType is not null) && !request.HasJsonContentType())
        {
            await DashboardMount.WriteErrorAsync(r.Http, 415, "unsupported_media_type", "Requests must be application/json.");
            return null;
        }

        using var buffer = new MemoryStream();
        var chunk = new byte[8192];
        int n;
        while ((n = await request.Body.ReadAsync(chunk, r.Ct)) > 0)
        {
            if (buffer.Length + n > MaxBody)
            {
                await DashboardMount.WriteErrorAsync(r.Http, 413, "payload_too_large", "The request body is too large.");
                return null;
            }

            await buffer.WriteAsync(chunk.AsMemory(0, n), r.Ct);
        }

        try
        {
            using var document = buffer.Length == 0 ? JsonDocument.Parse("{}") : JsonDocument.Parse(buffer.ToArray());
            if (document.RootElement.ValueKind == JsonValueKind.Object)
            {
                return document.RootElement.Clone();
            }
        }
        catch (JsonException)
        {
        }

        await DashboardMount.WriteErrorAsync(r.Http, 400, "invalid_request", "The request body is not a JSON object.");
        return null;
    }

    /// <summary>The body bound to <typeparamref name="T"/> (only its fields are read), or null after answering 400.</summary>
    public static async Task<(T Body, Guid CommandId)?> ReadAsync<T>(DashboardRequest r, bool commandId = true) where T : class, new()
    {
        if (await PrepareAsync(r) is not { } element)
        {
            return null;
        }

        var header = r.Http.Request.Headers[DashboardMount.CommandHeader].ToString();
        Guid id = default;
        if (commandId && (!Guid.TryParse(header, out id) || id == Guid.Empty))
        {
            await DashboardMount.WriteErrorAsync(r.Http, 400, "invalid_request", "X-Runwick-Command-Id must be a UUID.");
            return null;
        }

        try
        {
            return (element.Deserialize<T>(DashboardMount.Json) ?? new T(), id);
        }
        catch (JsonException)
        {
            await DashboardMount.WriteErrorAsync(r.Http, 400, "invalid_request", "The request body is not valid.");
            return null;
        }
    }

    /// <summary>Runs one control and answers 303 (applied) or the resource's page with an outcome.</summary>
    public static async Task RunAsync(DashboardRequest r, Guid commandId, Func<Task<(bool Ok, string? Code, long? Actual)>> send, string done,
        DashboardPage page, string pagePath)
    {
        try
        {
            var (ok, code, actual) = await send();
            if (ok)
            {
                // 303: the browser follows with a GET and never resends the POST.
                r.Http.Response.StatusCode = StatusCodes.Status303SeeOther;
                r.Http.Response.Headers.Location = r.Base + done;
                return;
            }

            r.Outcome = new { code, commandId, actualRevision = actual };
        }
        catch (RunwickException ex) when (ex is not (ManagementDeniedException or ManagementNotFoundException))
        {
            // Validation, outcome_unknown, store_unavailable, query budget: the same command ID resolves it later.
            r.Outcome = new { code = ex.Code, commandId };
        }

        await RenderAtAsync(r, page, pagePath);
    }

    /// <summary>Renders <paramref name="page"/> as if it were requested at <paramref name="path"/>, so the page URL is the resource's.</summary>
    public static Task RenderAtAsync(DashboardRequest r, DashboardPage page, string path)
    {
        r.Http.Request.Path = path;
        r.Http.Request.QueryString = QueryString.Empty;
        return page.RenderAsync(r);
    }

    public static string Escape(string segment) => Uri.EscapeDataString(segment);
}

internal sealed class ReasonBody
{
    public string? Reason { get; set; }
}

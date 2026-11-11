using Microsoft.AspNetCore.Http;
using Runwick.Dashboard.Pages;
using Runwick.Management;

namespace Runwick.Dashboard;

/// <summary>Routes relative to the mount (21 §4, §5). Pages are GET; controls and reveals are POST.</summary>
internal static class DashboardRouter
{
    public static async Task RouteAsync(DashboardRequest r)
    {
        try
        {
            await (DashboardRequest.Segments(r.Http) switch
            {
                [] or ["runs"] => Get(r, RunsPage.Instance),
                ["jobs"] => Get(r, JobTypesPage.Instance),
                ["overview"] => Get(r, OverviewPage.Instance),
                ["runs", var id] when Guid.TryParse(id, out var job) => Get(r, new RunPage(job)),
                ["jobs", var id] when Guid.TryParse(id, out var job) => MovedToRun(r, job), // links from before runs had their own name
                ["jobs", var name] => Get(r, new JobTypePage(name)),
                ["queues"] => Get(r, QueuesPage.Instance),
                ["queues", var name] => Get(r, new QueuePage(name)),
                ["workers"] => Get(r, WorkersPage.Instance),
                ["workers", var id] when Guid.TryParse(id, out var worker) => Get(r, new WorkerPage(worker)),
                ["schedules"] => Get(r, SchedulesPage.Instance),
                ["schedules", var key] => Get(r, new SchedulePage(key)),
                ["pools"] => Get(r, PoolsPage.Instance),
                ["pools", var pool] => Get(r, new PoolPage(pool)),
                ["scaling"] => Get(r, ScalingPage.Instance),
                ["store"] => Get(r, StorePage.Instance),
                ["settings"] => Get(r, SettingsPage.Instance),
                ["retention"] => Get(r, RetentionPage.Instance),
                ["audit"] => Get(r, AuditPage.Instance),
                ["runs:cancel"] => Post(r, () => RunsPage.BulkCancelAsync(r)),
                ["runs", var id, ("cancel" or "retry" or "rerun") and var op] => Post(r, () => Guid.TryParse(id, out var job)
                    ? RunPage.ControlAsync(r, job, op) : NotFound(r)),
                ["runs", var id, "payload:reveal"] => Post(r, () => Guid.TryParse(id, out var job) ? RunPage.RevealAsync(r, job, payload: true) : NotFound(r)),
                ["attempts", var id, "error:reveal"] => Post(r, () => Guid.TryParse(id, out var a) ? RunPage.RevealAsync(r, a, payload: false) : NotFound(r)),
                ["jobs", var name, ("dispatch" or "retry-failed" or "cancel-pending") and var op] => Post(r, () => JobTypePage.ControlAsync(r, name, op)),
                ["queues", var name, ("pause" or "resume") and var op] => Post(r, () => QueuesPage.ControlAsync(r, name, op == "pause")),
                ["scopes", var kind, var key, "capacity"] => Post(r, () => WorkersPage.CapacityAsync(r, kind, key)),
                ["schedules", var key, ("pause" or "resume" or "trigger" or "revalidate") and var op] => Post(r, () => SchedulesPage.ControlAsync(r, key, op)),
                ["retention", "policy"] => Post(r, () => RetentionPage.EditAsync(r)),
                _ => NotFound(r),
            });
        }
        catch (Exception ex) when (Map(ex) is { } error && !r.Http.Response.HasStarted)
        {
            if (error.Status is 403 or 404 or 503 && StatusPage.Fits(r))
            {
                r.Http.Response.Clear();
                await new StatusPage(error.Status, error.Code, error.Message, (ex as ManagementDeniedException)?.Permission).RenderStatusAsync(r);
                return;
            }

            r.Http.Response.Headers.Remove("X-Inertia");
            await DashboardMount.WriteErrorAsync(r.Http, error.Status, error.Code, error.Message, error.Body);
        }
    }

    /// <summary><c>GET /jobs/{id}</c> moved to <c>/runs/{id}</c> (21 §4): a permanent redirect that keeps the query.</summary>
    static Task MovedToRun(DashboardRequest r, Guid job)
    {
        if (!HttpMethods.IsGet(r.Http.Request.Method) && !HttpMethods.IsHead(r.Http.Request.Method))
        {
            return NotFound(r);
        }

        r.Http.Response.StatusCode = StatusCodes.Status301MovedPermanently;
        r.Http.Response.Headers.Location = r.Base + "/runs/" + job.ToString("D") + r.Http.Request.QueryString.ToUriComponent();
        return Task.CompletedTask;
    }

    static Task Get(DashboardRequest r, DashboardPage page)
    {
        if (HttpMethods.IsGet(r.Http.Request.Method) || HttpMethods.IsHead(r.Http.Request.Method))
        {
            return page.RenderAsync(r);
        }

        r.Http.Response.Headers.Allow = "GET, HEAD";
        return DashboardMount.WriteErrorAsync(r.Http, 405, "method_not_allowed", "Use GET.");
    }

    static Task Post(DashboardRequest r, Func<Task> handler)
    {
        if (HttpMethods.IsPost(r.Http.Request.Method))
        {
            return handler();
        }

        r.Http.Response.Headers.Allow = "POST";
        return DashboardMount.WriteErrorAsync(r.Http, 405, "method_not_allowed", "Use POST.");
    }

    static Task NotFound(DashboardRequest r) => StatusPage.Fits(r)
        ? new StatusPage(404, "not_found", "Not found.", null).RenderStatusAsync(r)
        : DashboardMount.WriteErrorAsync(r.Http, 404, "not_found", "Not found.");

    sealed record Error(int Status, string Code, string Message, object? Body = null);

    /// <summary>Session exceptions to answers (16 §5). A hidden or missing resource is 404 either way.</summary>
    static Error? Map(Exception ex) => ex switch
    {
        ManagementDeniedException => new(403, "forbidden", "The caller lacks the permission for this operation."),
        ManagementNotFoundException n => new(404, n.Code, n.Message),
        ManagementDetailExpiredException d => new(410, d.Code, d.Message, new { code = d.Code, message = d.Message, jobId = d.JobId }),
        ManagementValidationException v => new(400, v.Code, v.Message),
        ManagementQueryBudgetException q => new(503, q.Code, "The query did not finish within its budget."),
        RunwickConfigurationException => null,
        RunwickException e => new(503, e.Code, "The store is unavailable."),
        _ => null,
    };
}

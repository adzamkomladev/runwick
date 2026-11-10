using System.Collections.Concurrent;
using Microsoft.AspNetCore.HttpOverrides;

// Bun SSR is enabled by default; set INERTIA_SSR=0 for client-only rendering.
var ssr = Environment.GetEnvironmentVariable("INERTIA_SSR") != "0";
var development = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development";

var builder = WebApplication.CreateBuilder(args);

// Coolify's Traefik proxy terminates TLS. Trust its forwarded scheme so cookies are Secure and
// Inertia's absolute 409 locations use https. Do not publish the container port directly.
builder.Services.Configure<ForwardedHeadersOptions>(o =>
{
    o.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
    o.ForwardLimit = 1;
    o.KnownIPNetworks.Clear();
    o.KnownProxies.Clear();
});

builder.Services.AddResponseCompression();
builder.Services.AddAntiforgery(o => o.HeaderName = InertiaHeaders.XsrfToken);
builder.Services.AddInertia(o =>
{
    o.Share("appName", "Runwick");
    o.Share("version", "0.1.3");
    o.Share("baseUrl", ctx => $"{ctx.Request.Scheme}://{ctx.Request.Host}{ctx.Request.PathBase}");
    o.Vite.Prefetch = VitePrefetch.Waterfall;
    o.Pages.Directory = "ClientApp/pages";
    o.Ssr.BundlePath = "ssr/ssr.js";
    if (ssr)
    {
        // The SSR bundle is self-contained; Bun runs it without Node or node_modules.
        o.Ssr.UseNodeProcess(n =>
        {
            n.Executable = "bun";
            n.BasePort = int.Parse(Environment.GetEnvironmentVariable("INERTIA_SSR_PORT") ?? "13715", System.Globalization.CultureInfo.InvariantCulture);
        });
    }
});
if (development)
{
    builder.Services.AddInertiaDevTools();
}

var app = builder.Build();

app.UseForwardedHeaders();
app.UseResponseCompression();
app.UseStaticFiles(new StaticFileOptions
{
    OnPrepareResponse = c =>
    {
        if (c.Context.Request.Path.StartsWithSegments("/build/assets"))
        {
            c.Context.Response.Headers.CacheControl = "public, max-age=31536000, immutable";
        }
    },
});
app.UseInertia();
app.UseInertiaAntiforgery();

if (development)
{
    app.MapInertiaDevTools();
}

app.MapGet("/", () => Render("Home"));
app.MapGet("/health", () => Results.Ok("ok"));
app.MapMethods("/docs", ["GET", "HEAD"], () => Results.Redirect("/docs/introduction"));
app.MapGet("/docs/{**slug}", (string slug, IWebHostEnvironment environment) =>
    DocsCatalog.Exists(environment, slug)
        ? Render("Docs/Show", new InertiaProps { ["slug"] = slug })
        : Render("NotFound").WithStatusCode(404));
app.MapFallback(() => Render("NotFound").WithStatusCode(404));

app.Run();

/// <summary>The documentation pages that exist, read from <c>content/docs</c>.</summary>
internal static class DocsCatalog
{
    private static readonly ConcurrentDictionary<string, HashSet<string>> Cache = new(StringComparer.Ordinal);

    public static bool Exists(IWebHostEnvironment environment, string slug)
    {
        var root = Path.Combine(environment.ContentRootPath, "content", "docs");
        var pages = Cache.GetOrAdd(root, static path =>
        {
            if (!Directory.Exists(path))
            {
                return new HashSet<string>(StringComparer.Ordinal);
            }

            return Directory.EnumerateFiles(path, "*.mdx", SearchOption.AllDirectories)
                .Select(file => Path.GetRelativePath(path, file).Replace('\\', '/')[..^4])
                .ToHashSet(StringComparer.Ordinal);
        });

        return pages.Contains(slug.Trim('/'));
    }
}

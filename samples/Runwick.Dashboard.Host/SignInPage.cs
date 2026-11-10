using System.Net;
using Runwick.Dashboard;

/// <summary>
/// The sample host's sign-in page: three demo accounts, one per management role. It borrows the dashboard's stylesheet for its
/// fonts and colour tokens (so it matches the dashboard in light and dark) and adds its own few rules. A real host signs in with
/// its own identity provider; Runwick has no accounts of its own.
/// </summary>
static class SignInPage
{
    static readonly (string User, string Role, string Detail)[] Accounts =
    [
        ("admin", "Administrator", "Everything: also triggers runs and edits schedules, capacity and retention"),
        ("operator", "Operator", "Cancels, retries, pauses and triggers; sees payloads and audit"),
        ("viewer", "Viewer", "Reads pages and figures; no payloads, errors or audit"),
    ];

    public static string Html(string? returnUrl, bool signedOut, string dashboardBase)
    {
        static string E(string s) => WebUtility.HtmlEncode(s);
        var back = Uri.EscapeDataString(returnUrl ?? "");
        var css = string.Concat(DashboardAssets.Embedded.Css.Select(c => $"<link rel=\"stylesheet\" href=\"{E(dashboardBase + "/" + c)}\">"));
        var accounts = string.Concat(Accounts.Select(a => $"""
            <li><a class="account" data-testid="login-{a.User}" href="login/{a.User}?returnUrl={E(back)}">
              <span class="avatar" aria-hidden="true">{E(a.Role[..1])}</span>
              <span class="who"><strong>{E(a.Role)}</strong><span>{E(a.Detail)}</span></span>
              <svg class="chevron" viewBox="0 0 24 24" aria-hidden="true"><path d="M9 6l6 6l-6 6" /></svg>
            </a></li>
            """));
        var notice = signedOut ? """<p class="notice" role="status">You are signed out.</p>""" : "";
        var destination = returnUrl is { Length: > 0 } ? $"""<p class="dest">You'll continue to <code>{E(returnUrl)}</code></p>""" : "";
        return $$"""
            <!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Sign in · Runwick sample</title>{{css}}
            <script>try{const t=localStorage.getItem('runwick-dashboard-theme')??'system';if(t==='dark'||(t==='system'&&matchMedia('(prefers-color-scheme: dark)').matches))document.documentElement.classList.add('dark')}catch{}</script>
            <style>
              body{min-height:100dvh;display:grid;place-items:center;padding:24px 16px;font-family:var(--font-sans);background:var(--background);color:var(--foreground)}
              main{width:100%;max-width:26rem;display:grid;gap:24px}
              .brand{display:flex;align-items:center;gap:10px;font-weight:600;letter-spacing:-0.01em}
              .mark{display:grid;place-items:center;width:32px;height:32px;border-radius:8px;background:var(--color-midnight-violet-950,#1b1530);color:var(--highlight)}
              .dark .mark{background:var(--highlight);color:var(--color-midnight-violet-950,#1b1530)}
              .mark svg{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
              h1{font-size:1.5rem;line-height:1.2;font-weight:600;letter-spacing:-0.02em;text-wrap:balance}
              .lead{margin-top:6px;color:var(--muted-foreground);font-size:.875rem;line-height:1.5}
              ul{list-style:none;margin:0;padding:0;display:grid;gap:8px}
              .account{display:flex;align-items:center;gap:12px;padding:12px 14px;border:1px solid var(--border);border-radius:12px;background:var(--card);color:inherit;text-decoration:none;
                transition:transform .2s cubic-bezier(.2,.8,.2,1),box-shadow .2s cubic-bezier(.2,.8,.2,1),border-color .2s}
              .account:hover{border-color:color-mix(in oklch,var(--ring) 45%,var(--border));box-shadow:0 4px 14px -6px color-mix(in oklch,var(--foreground) 22%,transparent);transform:translateY(-1px)}
              .account:focus-visible{outline:3px solid color-mix(in oklch,var(--ring) 50%,transparent);outline-offset:2px}
              .account:active{transform:scale(.99)}
              .avatar{display:grid;place-items:center;flex:none;width:36px;height:36px;border-radius:9px;font-size:.8rem;font-weight:600;color:var(--primary);background:color-mix(in oklch,var(--primary) 14%,transparent)}
              .who{display:grid;gap:2px;min-width:0;flex:1}.who strong{font-weight:600;font-size:.925rem}.who span{color:var(--muted-foreground);font-size:.8rem}
              .chevron{width:16px;height:16px;flex:none;fill:none;stroke:var(--muted-foreground);stroke-width:2;stroke-linecap:round;stroke-linejoin:round;transition:transform .2s cubic-bezier(.2,.8,.2,1)}
              .account:hover .chevron{transform:translateX(2px)}
              .notice{padding:10px 12px;border-radius:10px;font-size:.85rem;background:color-mix(in oklch,var(--highlight) 22%,transparent)}
              .dest{color:var(--muted-foreground);font-size:.8rem;overflow-wrap:anywhere}.dest code{font-family:var(--font-mono);font-size:.78rem;color:var(--foreground)}
              footer{color:var(--muted-foreground);font-size:.75rem;line-height:1.5}
              @media (prefers-reduced-motion:reduce){.account,.chevron{transition:none}.account:hover,.account:active,.account:hover .chevron{transform:none} }
            </style></head>
            <body><main>
              <div class="brand"><span class="mark"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M13 3v7h6l-8 11v-7H5l8-11" /></svg></span>Runwick</div>
              <div><h1>Sign in to the dashboard</h1><p class="lead">This sample host has three demo accounts, one per management role. Pick one to continue.</p></div>
              {{notice}}
              <ul aria-label="Demo accounts">{{accounts}}</ul>
              {{destination}}
              <footer>A real application signs in through its own identity provider; Runwick maps those users to grants and keeps no accounts itself.</footer>
            </main></body></html>
            """;
    }
}

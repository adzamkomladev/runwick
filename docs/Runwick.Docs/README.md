# Runwick documentation site

The site at https://runwick.temtem.dev is an [Inertia.Net](https://github.com/adzamkomladev/Inertia.Net) app: an ASP.NET Core Minimal API renders `Home`, `Docs/Show` and `NotFound`; the client is React 19 with the Inertia v3 client, Tailwind CSS and MDX content. Bun 1.4+ installs packages, builds the client and SSR bundles, and runs the SSR server. The production container ships Bun and does not need Node.

## Run it

Run these from `docs/Runwick.Docs`. The site is not part of `Runwick.slnx` and has its own `Directory.Build.props` and `Directory.Packages.props`, so the repository's package rules do not apply to it.

```sh
bun install
bun run dev          # terminal 1: Vite on http://localhost:5170
dotnet run           # terminal 2: ASP.NET Core on http://localhost:5100
```

Open http://localhost:5100. In Development the app writes `wwwroot/hot` from Vite and renders the dev-server tags.

Without the Vite dev server, build both bundles first:

```sh
bun run build
dotnet run -c Release
```

The production build writes `wwwroot/build` with a Vite manifest and `ssr/ssr.js` with the Bun SSR server. `app.html`, `content/docs`, the built assets and the SSR bundle are copied to the output on publish.

## Content

Pages live in [`content/docs`](content/docs) as MDX with frontmatter:

```mdx
---
title: Props
description: Lazy, optional, deferred, merged and cached page data.
section: Essentials
order: 2
---
```

`section` must be one of the sections in `scripts/generate-docs.mjs` and `ClientApp/lib/content.ts` (`Get started`, `Jobs`, `Stores`, `Operations`, `Reference`); `order` controls the position inside the section. The prebuild script writes the navigation manifest, the lazy search index, and `wwwroot/llms.txt` and `wwwroot/llms-full.txt` for coding agents, so adding a page only requires the MDX file. Commit the regenerated files with the content change.

MDX components available in content:

| Component | Purpose |
|---|---|
| `<Callout type="info" title="...">` | Note, tip, warning, danger or success callout |
| `<Cards>` / `<Card title icon href>` | Link cards (`book`, `blocks`, `code`, `rocket`, `server`, `shield`, `sparkles`, `gauge`, `terminal`) |
| `<Tabs>` / `<Tab label="...">` | Tabbed examples, usually one per store |
| `<Steps>` / `<Step title="...">` | Numbered setup steps |
| `<Accordion>` / `<AccordionItem title="...">` | Collapsible FAQ entries |
| `<Badge>`, `<Kbd>` | Inline labels and key hints |

Internal links use normal Markdown and are rendered with the Inertia `Link` component automatically.

## Layout

- Sidebar: `ClientApp/components/DocsSidebar.tsx`, generated from frontmatter.
- Search: `ClientApp/components/SearchDialog.tsx`, `Cmd/Ctrl+K`.
- Theme: `ClientApp/components/ThemeToggle.tsx`, light/dark/system with no flash on load.
- TOC: `ClientApp/components/TableOfContents.tsx`, active heading tracking.
- MDX mapping: `ClientApp/components/Mdx.tsx`.

## SSR

SSR is enabled by default. `bun run build` writes both the client assets and `ssr/ssr.js`, and ASP.NET Core starts the renderer with `bun ssr/ssr.js` on port 13715. Node is not required.

```sh
bun run build
dotnet run -c Release
```

Set `INERTIA_SSR=0` to disable SSR at build time and runtime; the build then removes any stale `ssr/` output and the app uses client rendering. Keep the renderer on loopback; see the [Inertia.Net server-side rendering guide](https://inertia-dotnet.yebi.africa/docs/server-side-rendering).

## Deploy on Coolify

The docs app includes a production [`Dockerfile`](Dockerfile) that builds the client and SSR bundle with Bun, publishes the ASP.NET Core app, and ships Bun in the runtime image. The container build restores Inertia.Net from NuGet, so it only needs this directory as its build context.

1. Create a new resource in Coolify and connect this repository.
2. Choose **Dockerfile** as the build pack.
3. Set **Base Directory** to `/docs/Runwick.Docs` and **Dockerfile Location** to `/Dockerfile`.
4. Set the exposed port to `8080`.
5. Set the health check path to `/health`.
6. Set the domain to `https://runwick.temtem.dev` and point a DNS `A` (or `CNAME`) record for `runwick.temtem.dev` at the Coolify server. Coolify issues the certificate.
7. Optional: turn on automatic deployment on push to `main` with **Watch Paths** set to `docs/Runwick.Docs/**`, so library-only commits do not redeploy the site.
8. Deploy. No database, volumes or build-time environment variables are required.

Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `ASPNETCORE_ENVIRONMENT` | `Production` | Set in the image |
| `ASPNETCORE_HTTP_PORTS` | `8080` | Set in the image |
| `INERTIA_SSR` | `1` | Set to `0` to disable the managed Bun SSR server |
| `INERTIA_SSR_PORT` | `13715` | Internal Bun SSR port |

The image exposes `/health` and runs as the non-root `app` user. The site has no persistent data; docs content and built assets are baked into the image, so a redeploy is enough to publish content changes.

> **Keep the container port private.**
> The app trusts forwarded headers from its proxy so HTTPS cookies and Inertia redirects use the public scheme. Do not expose port `8080` directly to the internet; let Coolify's Traefik proxy be the only route to it.

If you run more than one replica, share the ASP.NET Core Data Protection key ring. The docs app has no user forms today, but antiforgery and Inertia state cookies are protected with it.

To build and run the same image locally:

```sh
cd docs/Runwick.Docs
docker build -t runwick-docs .
docker run --rm -p 8080:8080 runwick-docs
# client-only, without restarting the Bun renderer
docker run --rm -p 8080:8080 -e INERTIA_SSR=0 runwick-docs
```

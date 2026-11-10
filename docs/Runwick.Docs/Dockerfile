# syntax=docker/dockerfile:1
# Production image for the docs website only: client + SSR (Bun, supervised by the app).
# Build from this directory: docs/Runwick.Docs.

# 1. Frontend: client bundle (wwwroot/build) + SSR bundle (ssr/)
FROM oven/bun:1-debian AS web
WORKDIR /src
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

# 2. Backend: restore packages from NuGet; this context is only docs/Runwick.Docs.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS api
WORKDIR /src
COPY . .
RUN dotnet publish Runwick.Docs.csproj \
    -c Release -o /app /p:UseAppHost=false

# 3. Runtime: ASP.NET + the Bun binary that runs `ssr/ssr.js`.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
COPY --from=web /usr/local/bin/bun /usr/local/bin/bun
WORKDIR /app
COPY --from=api /app ./
COPY --from=web /src/wwwroot/build ./wwwroot/build
# .dockerignore drops wwwroot, so take the generated llms.txt files from the frontend build.
COPY --from=web /src/wwwroot/llms.txt /src/wwwroot/llms-full.txt ./wwwroot/
COPY --from=web /src/ssr ./ssr

ENV ASPNETCORE_ENVIRONMENT=Production \
    ASPNETCORE_HTTP_PORTS=8080 \
    INERTIA_SSR=1 \
    DOTNET_gcServer=0
EXPOSE 8080
USER $APP_UID

# No curl in the image; bun is already here for SSR.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["bun", "-e", "fetch('http://127.0.0.1:8080/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]

ENTRYPOINT ["dotnet", "Runwick.Docs.dll"]

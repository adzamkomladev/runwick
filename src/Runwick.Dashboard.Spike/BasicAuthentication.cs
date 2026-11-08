using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Runwick.Dashboard.Spike;

/// <summary>Spike Basic authentication: a fixed user table. HTTPS is the host's job; Basic alone does not stop CSRF.</summary>
public sealed class SpikeBasicOptions : AuthenticationSchemeOptions
{
    /// <summary>The realm in the challenge.</summary>
    public string Realm { get; set; } = "Runwick";

    /// <summary>User name to password and permission claims (<c>runwick:permission</c> values).</summary>
    public Dictionary<string, (string Password, string[] Permissions)> Users { get; } = new(StringComparer.Ordinal);
}

/// <summary>Validates Basic credentials against <see cref="SpikeBasicOptions.Users"/>.</summary>
public sealed class SpikeBasicHandler(IOptionsMonitor<SpikeBasicOptions> options, ILoggerFactory logger, UrlEncoder encoder)
    : AuthenticationHandler<SpikeBasicOptions>(options, logger, encoder)
{
    /// <inheritdoc />
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var header = Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(header[6..].Trim()));
        }
        catch (FormatException)
        {
            return Task.FromResult(AuthenticateResult.Fail("Malformed Basic credentials."));
        }

        var colon = decoded.IndexOf(':', StringComparison.Ordinal);
        if (colon < 0 || !Options.Users.TryGetValue(decoded[..colon], out var user)
            || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(decoded[(colon + 1)..]), Encoding.UTF8.GetBytes(user.Password)))
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid credentials."));
        }

        var claims = user.Permissions.Select(p => new Claim("runwick:permission", p)).Prepend(new Claim(ClaimTypes.Name, decoded[..colon]));
        var principal = new ClaimsPrincipal(new ClaimsIdentity(claims, Scheme.Name));
        return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(principal, Scheme.Name)));
    }

    /// <inheritdoc />
    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        Response.StatusCode = 401;
        Response.Headers.WWWAuthenticate = $"Basic realm=\"{Options.Realm}\", charset=\"UTF-8\"";
        return Task.CompletedTask;
    }
}

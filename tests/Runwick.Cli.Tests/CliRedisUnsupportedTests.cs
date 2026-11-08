namespace Runwick.Cli.Tests;

/// <summary>
/// The CLI has no Redis provider (spec 19, SP-026 report: <c>Runwick.Cli</c> does not reference <c>Runwick.Redis</c>), so a Redis
/// provider is a declared-unsupported capability that fails with <c>usage_error</c>, not a silent fallback.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
[Trait("Unsupported", "redis")]
public sealed class CliRedisUnsupportedTests
{
    [Theory]
    [Trait("Case", "UNS-RD-04")]
    [InlineData("migrate")]
    [InlineData("doctor")]
    public async Task The_CLI_refuses_provider_redis_with_usage_error(string command)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var env = new CliEnvironment
        {
            Out = output,
            Error = error,
            Variables = new Dictionary<string, string> { ["RCS"] = "127.0.0.1:6379" },
            ConfigPath = Path.Combine(Path.GetTempPath(), "rw-cli-no-config-" + Guid.NewGuid().ToString("N") + ".json"),
            PromptSecret = () => throw new InvalidOperationException("The CLI prompted."),
        };
        var exit = await RunwickCli.RunAsync(["--agent", command, "--provider", "redis", "--connection-env", "RCS"], env);
        Assert.Equal(2, exit);
        Assert.Empty(error.ToString());
        Assert.Contains("\"usage_error\"", output.ToString(), StringComparison.Ordinal);
    }
}

using Runwick.Cli;

// Builds the real environment and returns the exit code (19 §1); RunwickCli turns every failure into an error result.
using var interrupt = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    interrupt.Cancel();
};
return await RunwickCli.RunAsync(args, CliEnvironment.Console(interrupt.Token));

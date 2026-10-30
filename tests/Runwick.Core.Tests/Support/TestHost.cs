using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Runwick.Dispatching;
using Runwick.Registration;
using Runwick.Serialization;

namespace Runwick.Core.Tests.Support;

/// <summary>Builds registries and preparers without a database (registry resolution compiles and validates).</summary>
internal static class TestHost
{
    public static readonly Guid StoreId = Guid.Parse("5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f");

    public static readonly DateTimeOffset Now = new(2026, 10, 30, 10, 0, 0, TimeSpan.Zero);

    /// <summary>Application <c>billing</c> on a PostgreSQL store, plus <paramref name="configure"/>.</summary>
    public static Action<RunwickBuilder> App(Action<RunwickBuilder> configure) => b =>
    {
        b.Application("billing").UsePostgres("Host=x");
        configure(b);
    };

    public static JobRegistry Compile(Action<RunwickBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddRunwick(configure);
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<JobRegistry>();
    }

    public static JobRegistry CompileApp(Action<RunwickBuilder> configure) => Compile(App(configure));

    public static RunwickConfigurationException Fail(Action<RunwickBuilder> configure) =>
        Assert.Throws<RunwickConfigurationException>(() => Compile(configure));

    public static RunwickConfigurationException FailApp(Action<RunwickBuilder> configure) => Fail(App(configure));

    public static IReadOnlyList<string> Codes(RunwickConfigurationException ex) => [.. ex.Errors.Select(e => e.Code)];

    public static DispatchPreparer Preparer(JobRegistry registry, TimeProvider? time = null) =>
        new(registry, new ProducerOptions(), time ?? new FakeTimeProvider(Now));

    public static DispatchPreparer Preparer(Action<RunwickBuilder> configure) => Preparer(CompileApp(configure));

    public static PreparedDispatch Prepare(this DispatchPreparer preparer, IQueueJob job, DispatchOptions? options = null) =>
        preparer.Prepare(StoreId, job, options);

    public static DispatchValidationException PrepareFails(this DispatchPreparer preparer, IQueueJob job, DispatchOptions? options = null) =>
        Assert.Throws<DispatchValidationException>(() => preparer.Prepare(StoreId, job, options));

    public static string Hex(byte[] bytes) => Convert.ToHexStringLower(bytes);

    public static string Utf8(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    public static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    public static PreparedDispatch Import(ReadOnlySpan<byte> envelope, Guid? storeId = null, string application = "billing") =>
        PreparedEnvelopeCodec.ReadSingle(envelope, storeId ?? StoreId, application);

    public static DispatchValidationException ImportFails(ReadOnlySpan<byte> envelope, Guid? storeId = null, string application = "billing")
    {
        try
        {
            Import(envelope, storeId, application);
        }
        catch (DispatchValidationException ex)
        {
            return ex;
        }

        Assert.Fail("Import succeeded; a DispatchValidationException was expected.");
        return null!;
    }
}

/// <summary>Locates the repository root (the directory holding Runwick.slnx) and the published vectors.</summary>
internal static class Repo
{
    static readonly Lazy<string> RootPath = new(() =>
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Runwick.slnx")))
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException("Runwick.slnx not found above " + AppContext.BaseDirectory);
    });

    public static string Root => RootPath.Value;

    public static string WireVector(string name) => Path.Combine(Root, "tests", "vectors", "wire", name);

    public static JsonDocument LoadWireVector(string name) => JsonDocument.Parse(File.ReadAllBytes(WireVector(name)));
}

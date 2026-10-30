using System.Text.Json;
using Runwick.Serialization;

namespace Runwick.Core.Tests.Support;

/// <summary>Builds jobs whose serialized payload has an exact byte length.</summary>
internal static class Payloads
{
    /// <summary>A <see cref="BoundsJob"/> of exactly <paramref name="bytes"/> UTF-8 bytes, using ASCII parts of at most 64 KiB.</summary>
    public static BoundsJob OfSize(int bytes, int partSize = 65_536)
    {
        var count = Math.Max(1, (int)Math.Ceiling(bytes / (double)partSize));
        var overhead = RunwickJsonProfile.V1.Serialize(BoundsJob.WithParts(new string[count].Select(_ => "").ToArray()), typeof(BoundsJob)).Utf8.Length;
        var content = bytes - overhead;
        if (content < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes));
        }

        var parts = new string[count];
        for (var i = 0; i < count; i++)
        {
            var size = content / count + (i < content % count ? 1 : 0);
            parts[i] = new string((char)('a' + (i % 26)), size);
        }

        var job = BoundsJob.WithParts(parts);
        if (count * partSize < bytes - overhead)
        {
            throw new InvalidOperationException("part size too small");
        }

        return job;
    }

    public static int SerializedLength(IQueueJob job) => RunwickJsonProfile.V1.Serialize(job, job.GetType()).Utf8.Length;

    public static JsonElement Element(string json)
    {
        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 128 });
        return doc.RootElement.Clone();
    }

    public static BoundsJob Raw(string json) => new(null, null, null, Element(json));

    public static string Nested(int arrays) => new string('[', arrays) + new string(']', arrays);
}

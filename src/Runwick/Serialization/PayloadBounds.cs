namespace Runwick.Serialization;

/// <summary>Structural bounds of the <c>runwick.json</c> v1 profile (spec 02 §4.1).</summary>
internal static class PayloadBounds
{
    public const int MaxPayloadBytes = 1_048_576;
    public const int MaxDepth = 32;
    public const int MaxStringBytes = 262_144;
    public const int MaxPropertyNameBytes = 256;
    public const int MaxPropertiesPerObject = 256;
    public const int MaxArrayElements = 16_384;
    public const int MaxNumberLexemeBytes = 128;
    public const int MaxMetadataBytes = 16_384;
    public const int MaxMetadataEntries = 32;
    public const int MaxEnvelopeBytes = 2_097_152;
    public const int MaxBatchItems = 100;
    public const int MaxCapabilityTuples = 512;
}

/// <summary>A payload or envelope failed validation. Always known-not-applied.</summary>
internal sealed class PayloadValidationException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

using System.Text;

namespace Runwick.Serialization;

/// <summary>
/// Single-pass strict validator of payload bytes (spec 02 §4.2) that also yields the canonical
/// value used for hashing. Input is at most 1 MiB, so the produced tree is bounded by the input.
/// </summary>
internal ref struct JsonPayloadScanner
{
    readonly ReadOnlySpan<byte> _bytes;
    readonly ScannerLimits _limits;
    int _pos;

    JsonPayloadScanner(ReadOnlySpan<byte> bytes, ScannerLimits limits)
    {
        _bytes = bytes;
        _limits = limits;
        _pos = 0;
    }

    /// <summary>Validates a job payload (root must be an object) and returns its canonical value.</summary>
    public static CanonicalValue ScanPayload(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length > PayloadBounds.MaxPayloadBytes)
        {
            throw Fail("payload_too_large", $"Payload is {bytes.Length} bytes; the limit is {PayloadBounds.MaxPayloadBytes}.");
        }

        var value = Scan(bytes);
        return value is CanonicalObject ? value : throw Fail("payload_root_not_object", "The payload root must be a JSON object.");
    }

    /// <summary>Validates any JSON document (any root) under the payload rules and bounds.</summary>
    public static CanonicalValue Scan(ReadOnlySpan<byte> bytes) => Scan(bytes, ScannerLimits.Payload);

    /// <summary>Validates any JSON document under the §4.2 rules with the given structural bounds.</summary>
    public static CanonicalValue Scan(ReadOnlySpan<byte> bytes, ScannerLimits limits)
    {
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
        {
            throw Fail("payload_invalid_json", "A byte order mark is not allowed.");
        }

        var scanner = new JsonPayloadScanner(bytes, limits);
        var value = scanner.ReadValue(0);
        scanner.SkipWhitespace();
        if (scanner._pos != bytes.Length)
        {
            throw Fail("payload_invalid_json", "Content after the root value.");
        }

        return value;
    }

    static PayloadValidationException Fail(string code, string message) => new(code, message);

    PayloadValidationException Grammar(string what) => Fail("payload_invalid_json", $"{what} at byte {_pos}.");

    void SkipWhitespace()
    {
        while (_pos < _bytes.Length && _bytes[_pos] is 0x20 or 0x09 or 0x0A or 0x0D)
        {
            _pos++;
        }
    }

    byte Peek() => _pos < _bytes.Length ? _bytes[_pos] : throw Grammar("Unexpected end");

    CanonicalValue ReadValue(int depth)
    {
        SkipWhitespace();
        var b = Peek();
        switch (b)
        {
            case (byte)'{':
                if (depth + 1 > _limits.MaxDepth)
                {
                    throw Fail("payload_depth_exceeded", $"Nesting deeper than {_limits.MaxDepth} at byte {_pos}.");
                }

                return ReadObject(depth + 1);
            case (byte)'[':
                if (depth + 1 > _limits.MaxDepth)
                {
                    throw Fail("payload_depth_exceeded", $"Nesting deeper than {_limits.MaxDepth} at byte {_pos}.");
                }

                return ReadArray(depth + 1);
            case (byte)'"':
                var s = ReadString(out var byteLength);
                if (byteLength > _limits.MaxStringBytes)
                {
                    throw Fail("payload_string_too_long", $"String value of {byteLength} bytes exceeds {_limits.MaxStringBytes}.");
                }

                return CanonicalValue.Str(s);
            case (byte)'t':
                Expect("true"u8);
                return CanonicalValue.True;
            case (byte)'f':
                Expect("false"u8);
                return CanonicalValue.False;
            case (byte)'n':
                Expect("null"u8);
                return CanonicalValue.Null;
            case (byte)'-':
            case >= (byte)'0' and <= (byte)'9':
                return ReadNumber();
            default:
                throw Grammar("Unexpected character");
        }
    }

    void Expect(ReadOnlySpan<byte> literal)
    {
        if (!_bytes[_pos..].StartsWith(literal))
        {
            throw Grammar("Invalid literal");
        }

        _pos += literal.Length;
    }

    CanonicalValue ReadObject(int depth)
    {
        _pos++;
        var properties = new List<KeyValuePair<string, CanonicalValue>>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        SkipWhitespace();
        if (Peek() == (byte)'}')
        {
            _pos++;
            return new CanonicalObject(properties);
        }

        while (true)
        {
            SkipWhitespace();
            if (Peek() != (byte)'"')
            {
                throw Grammar("Expected a property name");
            }

            var name = ReadString(out var nameBytes);
            if (nameBytes > _limits.MaxPropertyNameBytes)
            {
                throw Fail("payload_property_name_too_long", $"Property name of {nameBytes} bytes exceeds {_limits.MaxPropertyNameBytes}.");
            }

            SkipWhitespace();
            if (Peek() != (byte)':')
            {
                throw Grammar("Expected ':'");
            }

            _pos++;
            if (!names.Add(name))
            {
                throw Fail("payload_duplicate_property", $"Duplicate decoded property name before byte {_pos}.");
            }

            properties.Add(new(name, ReadValue(depth)));
            if (properties.Count > _limits.MaxProperties)
            {
                throw Fail("payload_too_many_properties", $"More than {_limits.MaxProperties} properties in one object.");
            }

            SkipWhitespace();
            var next = Peek();
            _pos++;
            if (next == (byte)',')
            {
                continue;
            }

            if (next == (byte)'}')
            {
                return new CanonicalObject(properties);
            }

            _pos--;
            throw Grammar("Expected ',' or '}'");
        }
    }

    CanonicalValue ReadArray(int depth)
    {
        _pos++;
        var items = new List<CanonicalValue>();
        SkipWhitespace();
        if (Peek() == (byte)']')
        {
            _pos++;
            return new CanonicalArray(items);
        }

        while (true)
        {
            items.Add(ReadValue(depth));
            if (items.Count > _limits.MaxArrayElements)
            {
                throw Fail("payload_array_too_long", $"More than {_limits.MaxArrayElements} array elements.");
            }

            SkipWhitespace();
            var next = Peek();
            _pos++;
            if (next == (byte)',')
            {
                continue;
            }

            if (next == (byte)']')
            {
                return new CanonicalArray(items);
            }

            _pos--;
            throw Grammar("Expected ',' or ']'");
        }
    }

    CanonicalValue ReadNumber()
    {
        var start = _pos;
        while (_pos < _bytes.Length && _bytes[_pos] is (byte)'-' or (byte)'+' or (byte)'.' or (byte)'e' or (byte)'E' or >= (byte)'0' and <= (byte)'9')
        {
            _pos++;
        }

        var lexeme = Encoding.ASCII.GetString(_bytes[start.._pos]);
        if (!IsNumberLexeme(lexeme))
        {
            throw Fail("payload_invalid_number", $"Invalid number lexeme at byte {start}.");
        }

        if (lexeme.Length > _limits.MaxNumberBytes)
        {
            throw Fail("payload_number_too_long", $"Number lexeme of {lexeme.Length} bytes exceeds {_limits.MaxNumberBytes}.");
        }

        return CanonicalValue.Number(lexeme);
    }

    internal static bool IsNumberLexeme(string s)
    {
        var i = 0;
        if (i < s.Length && s[i] == '-')
        {
            i++;
        }

        if (i >= s.Length)
        {
            return false;
        }

        if (s[i] == '0')
        {
            i++;
        }
        else if (s[i] is >= '1' and <= '9')
        {
            while (i < s.Length && char.IsAsciiDigit(s[i]))
            {
                i++;
            }
        }
        else
        {
            return false;
        }

        if (i < s.Length && s[i] == '.')
        {
            i++;
            var digits = i;
            while (i < s.Length && char.IsAsciiDigit(s[i]))
            {
                i++;
            }

            if (i == digits)
            {
                return false;
            }
        }

        if (i < s.Length && s[i] is 'e' or 'E')
        {
            i++;
            if (i < s.Length && s[i] is '+' or '-')
            {
                i++;
            }

            var digits = i;
            while (i < s.Length && char.IsAsciiDigit(s[i]))
            {
                i++;
            }

            if (i == digits)
            {
                return false;
            }
        }

        return i == s.Length;
    }

    string ReadString(out int utf8Length)
    {
        _pos++;
        var builder = new StringBuilder();
        utf8Length = 0;
        while (true)
        {
            if (_pos >= _bytes.Length)
            {
                throw Grammar("Unterminated string");
            }

            var b = _bytes[_pos];
            if (b == (byte)'"')
            {
                _pos++;
                return builder.ToString();
            }

            if (b < 0x20)
            {
                throw Grammar("Unescaped control character in string");
            }

            if (b == (byte)'\\')
            {
                utf8Length += ReadEscape(builder);
                continue;
            }

            if (b < 0x80)
            {
                builder.Append((char)b);
                utf8Length++;
                _pos++;
                continue;
            }

            var length = Utf8SequenceLength(_bytes[_pos..]);
            if (length == 0)
            {
                throw Fail("payload_invalid_utf8", $"Malformed UTF-8 at byte {_pos}.");
            }

            builder.Append(Encoding.UTF8.GetString(_bytes.Slice(_pos, length)));
            utf8Length += length;
            _pos += length;
        }
    }

    int ReadEscape(StringBuilder builder)
    {
        if (_pos + 1 >= _bytes.Length)
        {
            throw Grammar("Unterminated escape");
        }

        var e = _bytes[_pos + 1];
        _pos += 2;
        char? simple = e switch
        {
            (byte)'"' => '"',
            (byte)'\\' => '\\',
            (byte)'/' => '/',
            (byte)'b' => '\b',
            (byte)'f' => '\f',
            (byte)'n' => '\n',
            (byte)'r' => '\r',
            (byte)'t' => '\t',
            _ => null,
        };
        if (simple is { } c)
        {
            builder.Append(c);
            return 1;
        }

        if (e != (byte)'u')
        {
            throw Grammar("Invalid escape");
        }

        var unit = ReadHex4();
        if (unit is >= 0xDC00 and <= 0xDFFF)
        {
            throw Fail("payload_invalid_utf8", "Unpaired low surrogate escape.");
        }

        if (unit is >= 0xD800 and <= 0xDBFF)
        {
            if (_pos + 1 >= _bytes.Length || _bytes[_pos] != (byte)'\\' || _bytes[_pos + 1] != (byte)'u')
            {
                throw Fail("payload_invalid_utf8", "Unpaired high surrogate escape.");
            }

            _pos += 2;
            var low = ReadHex4();
            if (low is < 0xDC00 or > 0xDFFF)
            {
                throw Fail("payload_invalid_utf8", "Unpaired high surrogate escape.");
            }

            builder.Append((char)unit).Append((char)low);
            return 4;
        }

        builder.Append((char)unit);
        return unit < 0x80 ? 1 : unit < 0x800 ? 2 : 3;
    }

    int ReadHex4()
    {
        if (_pos + 4 > _bytes.Length)
        {
            throw Grammar("Truncated \\u escape");
        }

        var value = 0;
        for (var i = 0; i < 4; i++)
        {
            var h = _bytes[_pos + i];
            var digit = h switch
            {
                >= (byte)'0' and <= (byte)'9' => h - '0',
                >= (byte)'a' and <= (byte)'f' => h - 'a' + 10,
                >= (byte)'A' and <= (byte)'F' => h - 'A' + 10,
                _ => throw Grammar("Invalid \\u escape"),
            };
            value = (value << 4) | digit;
        }

        _pos += 4;
        return value;
    }

    /// <summary>RFC 3629 well-formed sequence length at the start of <paramref name="s"/>, or 0.</summary>
    static int Utf8SequenceLength(ReadOnlySpan<byte> s)
    {
        static bool Cont(ReadOnlySpan<byte> s, int i) => i < s.Length && (s[i] & 0xC0) == 0x80;

        var b0 = s[0];
        if (b0 is >= 0xC2 and <= 0xDF)
        {
            return Cont(s, 1) ? 2 : 0;
        }

        if (b0 is >= 0xE0 and <= 0xEF)
        {
            if (!Cont(s, 1) || !Cont(s, 2))
            {
                return 0;
            }

            var b1 = s[1];
            if (b0 == 0xE0 && b1 < 0xA0)
            {
                return 0; // overlong
            }

            if (b0 == 0xED && b1 >= 0xA0)
            {
                return 0; // encoded surrogate
            }

            return 3;
        }

        if (b0 is >= 0xF0 and <= 0xF4)
        {
            if (!Cont(s, 1) || !Cont(s, 2) || !Cont(s, 3))
            {
                return 0;
            }

            var b1 = s[1];
            if ((b0 == 0xF0 && b1 < 0x90) || (b0 == 0xF4 && b1 >= 0x90))
            {
                return 0;
            }

            return 4;
        }

        return 0;
    }
}

using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;

namespace Hopwake;

/// <summary>One queued mark.</summary>
internal readonly record struct Mark(
    string JourneyId,
    string SpanId,
    long AtUnixNanos,
    string Device,
    ulong Seq,
    HopOp Op,
    string? MessageType,
    string? Alias);

/// <summary>Writes marks as an OTLP/JSON trace export: hex IDs, 64-bit integers as strings.</summary>
internal static class Otlp
{
    private const int KindConsumer = 5;

    public static byte[] Encode(string service, string tenant, string hop, IReadOnlyList<Mark> marks)
    {
        var buf = new ArrayBufferWriter<byte>(256 + marks.Count * 900);
        using (var w = new Utf8JsonWriter(buf))
        {
            w.WriteStartObject();
            w.WriteStartArray("resourceSpans");
            w.WriteStartObject();
            w.WriteStartObject("resource");
            w.WriteStartArray("attributes");
            Str(w, "service.name", service);
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteStartArray("scopeSpans");
            w.WriteStartObject();
            w.WriteStartObject("scope");
            w.WriteString("name", "hopwake/hook");
            w.WriteEndObject();
            w.WriteStartArray("spans");
            foreach (var m in marks)
            {
                var at = m.AtUnixNanos.ToString(CultureInfo.InvariantCulture);
                w.WriteStartObject();
                w.WriteString("traceId", m.JourneyId);
                w.WriteString("spanId", m.SpanId);
                w.WriteString("name", "hop " + hop);
                w.WriteNumber("kind", KindConsumer);
                w.WriteString("startTimeUnixNano", at);
                w.WriteString("endTimeUnixNano", at);
                w.WriteStartArray("attributes");
                Str(w, "hopwake.schema", "hop/v1");
                Str(w, "hopwake.tenant", tenant);
                Str(w, "hopwake.device.id", m.Device);
                Int(w, "hopwake.seq", unchecked((long)m.Seq)); // an OTLP int is signed 64-bit, as in the Go hook
                Str(w, "hopwake.journey.id", m.JourneyId);
                Str(w, "hopwake.hop", hop);
                Str(w, "hopwake.op", m.Op == HopOp.Serve ? "serve" : "store");
                Str(w, "hopwake.correlation", "derived");
                Int(w, "hopwake.payload.bytes", 0);
                Str(w, "messaging.system", "hook");
                Str(w, "messaging.destination.name", "");
                if (!string.IsNullOrEmpty(m.MessageType))
                {
                    Str(w, "hopwake.message.type", m.MessageType);
                }
                if (!string.IsNullOrEmpty(m.Alias))
                {
                    Str(w, "hopwake.device.alias", m.Alias);
                }
                w.WriteEndArray();
                w.WriteEndObject();
            }
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
        }
        return buf.WrittenSpan.ToArray();
    }

    private static void Str(Utf8JsonWriter w, string key, string value)
    {
        w.WriteStartObject();
        w.WriteString("key", key);
        w.WriteStartObject("value");
        w.WriteString("stringValue", value);
        w.WriteEndObject();
        w.WriteEndObject();
    }

    private static void Int(Utf8JsonWriter w, string key, long value)
    {
        w.WriteStartObject();
        w.WriteString("key", key);
        w.WriteStartObject("value");
        w.WriteString("intValue", value.ToString(CultureInfo.InvariantCulture));
        w.WriteEndObject();
        w.WriteEndObject();
    }
}

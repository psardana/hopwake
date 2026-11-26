using System;
using System.Collections.Generic;
using System.Text;

namespace Hopwake;

/// <summary>
/// Reads Kafka headers, such as a device or sequence number header, without
/// depending on a Kafka client. With Confluent.Kafka:
/// <c>KafkaHeaders.Get(msg.Headers.Select(h => KeyValuePair.Create(h.Key, h.GetValueBytes())), "seq")</c>.
/// </summary>
public static class KafkaHeaders
{
    /// <summary>
    /// The first header with this name, as UTF-8 text, as the tap reads it;
    /// null when there is none.
    /// </summary>
    public static string? Get(IEnumerable<KeyValuePair<string, byte[]>>? headers, string name)
    {
        if (headers is null)
        {
            return null;
        }
        foreach (var h in headers)
        {
            if (string.Equals(h.Key, name, StringComparison.Ordinal))
            {
                return h.Value is null ? "" : Encoding.UTF8.GetString(h.Value);
            }
        }
        return null;
    }
}

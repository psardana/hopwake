using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Hopwake;

/// <summary>
/// Journey IDs, derived exactly as Hopwake's taps derive them (docs/hooks.md).
/// </summary>
public static class JourneyId
{
    /// <summary>The journey ID of a reading with a sequence number, and a boot when the source has one.</summary>
    public static string Seq(string tenant, string device, ulong seq, string boot = "")
    {
        using var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Field(h, "hopwake/journey/v1");
        Field(h, tenant);
        Field(h, device);
        if (!string.IsNullOrEmpty(boot))
        {
            Field(h, "boot:" + boot);
        }
        Span<byte> n = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(n, seq);
        h.AppendData(n);
        return Finish(h);
    }

    /// <summary>
    /// The journey ID of a reading known by its device, message type and time.
    /// <paramref name="formattedTime"/> is the time as <see cref="HopTime.Format(DateTimeOffset, TimeSpan)"/> writes it.
    /// </summary>
    public static string Time(string tenant, string device, string messageType, string formattedTime)
    {
        using var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Field(h, "hopwake/journey/time/v1");
        Field(h, tenant);
        Field(h, device);
        Field(h, "type:" + messageType);
        Field(h, "time:" + formattedTime);
        return Finish(h);
    }

    private static void Field(IncrementalHash h, string s)
    {
        var b = Encoding.UTF8.GetBytes(s);
        Span<byte> n = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(n, (uint)b.Length);
        h.AppendData(n);
        h.AppendData(b);
    }

    private static string Finish(IncrementalHash h)
    {
        var id = h.GetHashAndReset().AsSpan(0, 16).ToArray();
        if (Array.TrueForAll(id, x => x == 0))
        {
            id[15] = 1; // an all-zero trace ID is invalid
        }
        return Convert.ToHexString(id).ToLowerInvariant();
    }
}

/// <summary>Aliases: other names a device goes by further down the pipeline.</summary>
public static class Alias
{
    /// <summary>An alias as it travels on hop events: <c>kind:value</c>.</summary>
    public static string Format(string kind, string value) => kind + ":" + value;
}

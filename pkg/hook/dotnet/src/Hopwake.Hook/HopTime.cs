using System;
using System.Globalization;

namespace Hopwake;

/// <summary>
/// Reading times as they enter a journey ID: rounded down to the time
/// precision, then written in UTC with up to nine fraction digits and no
/// trailing zeros, e.g. <c>2026-11-18T10:00:00.5Z</c>.
/// </summary>
public static class HopTime
{
    private const long TicksPerSecond = TimeSpan.TicksPerSecond;
    private const long NanosPerTick = 100;

    /// <summary>Formats a time. A zero or negative precision keeps the time as given.</summary>
    public static string Format(DateTimeOffset at, TimeSpan precision)
    {
        long t = at.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks;
        if (precision > TimeSpan.Zero)
        {
            t -= FloorMod(t, precision.Ticks);
        }
        long secs = FloorDiv(t, TicksPerSecond);
        long nanos = FloorMod(t, TicksPerSecond) * NanosPerTick;
        return Write(secs, nanos);
    }

    /// <summary>
    /// Formats a time given in nanoseconds since the Unix epoch, for times
    /// finer than <see cref="DateTimeOffset"/>'s 100 ns.
    /// </summary>
    public static string Format(long unixNanos, TimeSpan precision)
    {
        Int128 t = unixNanos;
        if (precision > TimeSpan.Zero)
        {
            Int128 p = (Int128)precision.Ticks * NanosPerTick;
            Int128 m = t % p;
            if (m < 0)
            {
                m += p;
            }
            t -= m;
        }
        Int128 s = t / 1_000_000_000;
        Int128 ns = t % 1_000_000_000;
        if (ns < 0)
        {
            ns += 1_000_000_000;
            s -= 1;
        }
        return Write((long)s, (long)ns);
    }

    private static string Write(long unixSeconds, long nanos)
    {
        var dt = DateTime.UnixEpoch.AddTicks(unixSeconds * TicksPerSecond);
        var text = dt.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss", CultureInfo.InvariantCulture);
        if (nanos != 0)
        {
            text += "." + nanos.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0');
        }
        return text + "Z";
    }

    private static long FloorDiv(long a, long b)
    {
        long q = a / b;
        return (a % b != 0 && (a < 0) != (b < 0)) ? q - 1 : q;
    }

    private static long FloorMod(long a, long b)
    {
        long m = a % b;
        return m < 0 ? m + b : m;
    }
}

/// <summary>Durations written as in the rules file and the environment: <c>1s</c>, <c>500ms</c>, <c>1m</c>, <c>1h30m</c>.</summary>
public static class Durations
{
    /// <summary>Parses a duration such as <c>1s</c>, <c>500ms</c>, <c>1.5m</c> or <c>250us</c>.</summary>
    /// <exception cref="FormatException">The text is not a duration, or is finer than 100 ns.</exception>
    public static TimeSpan Parse(string text) =>
        TryParse(text, out var d) ? d : throw new FormatException($"not a duration: \"{text}\"");

    /// <summary>
    /// Parses a duration the way Go's <c>time.ParseDuration</c> does: an optional
    /// sign, then one or more numbers each followed by a unit (ns, us, µs, ms, s, m, h).
    /// Durations that are not a whole number of 100 ns ticks are refused.
    /// </summary>
    public static bool TryParse(string? text, out TimeSpan duration)
    {
        duration = TimeSpan.Zero;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }
        int i = 0;
        bool negative = false;
        if (text[0] == '-' || text[0] == '+')
        {
            negative = text[0] == '-';
            i++;
        }
        if (text.Substring(i) == "0")
        {
            return true;
        }
        if (i == text.Length)
        {
            return false;
        }
        decimal nanos = 0;
        try
        {
            while (i < text.Length)
            {
                int start = i;
                while (i < text.Length && (char.IsAsciiDigit(text[i]) || text[i] == '.'))
                {
                    i++;
                }
                var number = text.Substring(start, i - start);
                if (number.Length == 0 || number == "." || number.IndexOf('.') != number.LastIndexOf('.'))
                {
                    return false;
                }
                start = i;
                while (i < text.Length && !char.IsAsciiDigit(text[i]) && text[i] != '.')
                {
                    i++;
                }
                decimal unit = text.Substring(start, i - start) switch
                {
                    "ns" => 1m,
                    "us" or "µs" or "μs" => 1_000m,
                    "ms" => 1_000_000m,
                    "s" => 1_000_000_000m,
                    "m" => 60_000_000_000m,
                    "h" => 3_600_000_000_000m,
                    _ => 0m,
                };
                if (unit == 0m)
                {
                    return false;
                }
                nanos += decimal.Parse(number, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture) * unit;
            }
            nanos = decimal.Truncate(nanos);
            if (nanos % 100 != 0 || nanos / 100 > TimeSpan.MaxValue.Ticks)
            {
                return false;
            }
            long ticks = (long)(nanos / 100);
            duration = TimeSpan.FromTicks(negative ? -ticks : ticks);
            return true;
        }
        catch (OverflowException)
        {
            return false;
        }
    }
}

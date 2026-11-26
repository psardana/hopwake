using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Hopwake.Tests;

// The shared vectors every hook must reproduce, read from pkg/hook/testdata.
public class VectorTests
{
    private static readonly Lazy<JsonDocument> Vectors = new(() => JsonDocument.Parse(File.ReadAllText(VectorsPath())));

    private static string VectorsPath()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var path = Path.Combine(dir.FullName, "testdata", "vectors.json");
            if (File.Exists(path))
            {
                return path;
            }
        }
        throw new FileNotFoundException("pkg/hook/testdata/vectors.json not found above " + AppContext.BaseDirectory);
    }

    private static IEnumerable<object[]> Section(string name)
    {
        int i = 0;
        foreach (var _ in Vectors.Value.RootElement.GetProperty(name).EnumerateArray())
        {
            yield return new object[] { i++ };
        }
    }

    public static IEnumerable<object[]> SeqVectors() => Section("seq");
    public static IEnumerable<object[]> TimeVectors() => Section("time");
    public static IEnumerable<object[]> AliasVectors() => Section("alias");

    private static JsonElement Vector(string section, int i) => Vectors.Value.RootElement.GetProperty(section)[i];

    private static string S(JsonElement v, string name) => v.GetProperty(name).GetString()!;

    [Fact]
    public void EverySectionHasVectors()
    {
        Assert.NotEmpty(SeqVectors());
        Assert.NotEmpty(TimeVectors());
        Assert.NotEmpty(AliasVectors());
    }

    [Theory]
    [MemberData(nameof(SeqVectors))]
    public void SeqJourneyId(int i)
    {
        var v = Vector("seq", i);
        Assert.Equal(S(v, "id"), JourneyId.Seq(S(v, "tenant"), S(v, "device"), ulong.Parse(S(v, "seq")), S(v, "boot")));
    }

    [Theory]
    [MemberData(nameof(TimeVectors))]
    public void TimeFormatAndJourneyId(int i)
    {
        var v = Vector("time", i);
        long nanos = long.Parse(S(v, "unix_nanos"));
        long precisionNs = long.Parse(S(v, "precision_ns"));
        Assert.Equal(0, precisionNs % 100);
        var precision = TimeSpan.FromTicks(precisionNs / 100);

        Assert.Equal(S(v, "formatted"), HopTime.Format(nanos, precision));
        if (S(v, "resolution") != "ns")
        {
            // DateTimeOffset holds 100 ns ticks, enough for every vector but the ns ones.
            var at = DateTimeOffset.UnixEpoch.AddTicks(nanos / 100);
            Assert.Equal(S(v, "formatted"), HopTime.Format(at, precision));
        }
        Assert.Equal(S(v, "id"), JourneyId.Time(S(v, "tenant"), S(v, "device"), S(v, "type"), S(v, "formatted")));
    }

    [Theory]
    [MemberData(nameof(AliasVectors))]
    public void AliasFormat(int i)
    {
        var v = Vector("alias", i);
        Assert.Equal(S(v, "formatted"), Alias.Format(S(v, "kind"), S(v, "value")));
    }
}

public class FormatTests
{
    [Theory]
    [InlineData("1s", 10_000_000L)]
    [InlineData("500ms", 5_000_000L)]
    [InlineData("1m", 600_000_000L)]
    [InlineData("1h", 36_000_000_000L)]
    [InlineData("250us", 2_500L)]
    [InlineData("250µs", 2_500L)]
    [InlineData("100ns", 1L)]
    [InlineData("1h30m", 54_000_000_000L)]
    [InlineData("1.5s", 15_000_000L)]
    [InlineData("0", 0L)]
    [InlineData("-1s", -10_000_000L)]
    public void DurationsParse(string text, long ticks) => Assert.Equal(TimeSpan.FromTicks(ticks), Durations.Parse(text));

    [Theory]
    [InlineData("")]
    [InlineData("1")]
    [InlineData("s")]
    [InlineData("1x")]
    [InlineData("1.2.3s")]
    [InlineData("1ns")]
    public void DurationsRefuse(string text)
    {
        Assert.False(Durations.TryParse(text, out _));
        Assert.Throws<FormatException>(() => Durations.Parse(text));
    }

    [Theory]
    [InlineData(0L, "2026-11-18T10:00:00Z")]
    [InlineData(500L, "2026-11-18T10:00:00.5Z")]
    [InlineData(120L, "2026-11-18T10:00:00.12Z")]
    public void TimesAsTheContractWritesThem(long millis, string want)
    {
        var at = new DateTimeOffset(2026, 11, 18, 10, 0, 0, TimeSpan.Zero).AddMilliseconds(millis);
        Assert.Equal(want, HopTime.Format(at, TimeSpan.Zero));
    }

    [Fact]
    public void TimesAreWrittenInUtc()
    {
        var at = new DateTimeOffset(2026, 11, 18, 11, 0, 0, TimeSpan.FromHours(1));
        Assert.Equal("2026-11-18T10:00:00Z", HopTime.Format(at, TimeSpan.Zero));
    }

    [Fact]
    public void TimesBeforeTheEpochRoundDown()
    {
        // 1969-12-31T23:59:59.5Z rounds down to the second before, not up to the epoch.
        Assert.Equal("1969-12-31T23:59:59Z", HopTime.Format(-500_000_000L, TimeSpan.FromSeconds(1)));
        Assert.Equal("1969-12-31T23:59:59Z", HopTime.Format(DateTimeOffset.UnixEpoch.AddMilliseconds(-500), TimeSpan.FromSeconds(1)));
        Assert.Equal("1969-12-31T23:59:59.5Z", HopTime.Format(-500_000_000L, TimeSpan.Zero));
    }

    [Fact]
    public void KafkaHeaderIsTheFirstWithTheName()
    {
        var headers = new[]
        {
            KeyValuePair.Create("device", "dev-00042"u8.ToArray()),
            KeyValuePair.Create("seq", "1683"u8.ToArray()),
            KeyValuePair.Create("seq", "9"u8.ToArray()),
        };
        Assert.Equal("1683", KafkaHeaders.Get(headers, "seq"));
        Assert.Equal("dev-00042", KafkaHeaders.Get(headers, "device"));
        Assert.Null(KafkaHeaders.Get(headers, "Seq"));
        Assert.Null(KafkaHeaders.Get(null, "seq"));
    }
}

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

namespace Hopwake.Tests;

public class HookTests
{
    private const string Tenant = "t";
    private const string HopName = "hook:test.check";
    private static readonly DateTimeOffset At = new DateTimeOffset(2026, 11, 18, 10, 0, 0, TimeSpan.Zero).AddMilliseconds(750);

    private static Hook NewHook(string endpoint, string? aliases = null) => new(new HookOptions
    {
        Tenant = Tenant,
        Hop = HopName,
        Endpoint = endpoint,
        Aliases = aliases,
        TimePrecision = TimeSpan.FromSeconds(1),
    });

    [Fact]
    public async Task MarksGoOutAsTheContractSays()
    {
        using var rx = new Receiver();
        long before = (DateTimeOffset.UtcNow.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) * 100;
        await using (var hook = NewHook(rx.Url))
        {
            hook.Mark(HopOp.Store, "dev-00042", 1683);
            hook.MarkTime(HopOp.Store, "EMS0000042", "ems.Measurement", At);
        }
        var spans = rx.Spans;
        Assert.Equal(2, spans.Count);

        var s = spans.Single(x => x.Attrs["hopwake.seq"] == "int:1683");
        var id = JourneyId.Seq(Tenant, "dev-00042", 1683);
        Assert.Equal(id, s.TraceId);
        Assert.Equal(16, s.SpanId.Length);
        Assert.NotEqual("0000000000000000", s.SpanId);
        Assert.Equal("hop " + HopName, s.Name);
        Assert.Equal(5, s.Kind);
        Assert.Equal(s.Start, s.End);
        Assert.True(long.Parse(s.Start) >= before);
        Assert.Equal(HopName, s.Service);
        Assert.Equal("hop/v1", s.Attrs["hopwake.schema"]);
        Assert.Equal(Tenant, s.Attrs["hopwake.tenant"]);
        Assert.Equal("dev-00042", s.Attrs["hopwake.device.id"]);
        Assert.Equal(id, s.Attrs["hopwake.journey.id"]);
        Assert.Equal(HopName, s.Attrs["hopwake.hop"]);
        Assert.Equal("store", s.Attrs["hopwake.op"]);
        Assert.Equal("derived", s.Attrs["hopwake.correlation"]);
        Assert.Equal("hook", s.Attrs["messaging.system"]);
        Assert.False(s.Attrs.ContainsKey("hopwake.message.type"));
        Assert.False(s.Attrs.ContainsKey("hopwake.device.alias"));

        var t = spans.Single(x => x.Attrs["hopwake.seq"] == "int:0");
        Assert.Equal(JourneyId.Time(Tenant, "EMS0000042", "ems.Measurement", "2026-11-18T10:00:00Z"), t.TraceId);
        Assert.Equal("ems.Measurement", t.Attrs["hopwake.message.type"]);
    }

    [Fact]
    public async Task ServeIsSentOnlyOnce()
    {
        using var rx = new Receiver();
        await using var hook = NewHook(rx.Url);
        hook.Mark(HopOp.Serve, "dev-00042", 1683);
        hook.Mark(HopOp.Serve, "dev-00042", 1683);
        hook.MarkTime(HopOp.Serve, "EMS0000042", "ems.Measurement", At);
        hook.MarkTime(HopOp.Serve, "EMS0000042", "ems.Measurement", At.AddMilliseconds(100)); // same second: same reading
        hook.Mark(HopOp.Store, "dev-00042", 1683);
        hook.Mark(HopOp.Store, "dev-00042", 1683);
        await hook.FlushAsync();
        var ops = rx.Spans.Select(s => s.Attrs["hopwake.op"]).OrderBy(x => x).ToList();
        Assert.Equal(new[] { "serve", "serve", "store", "store" }, ops);
    }

    [Fact]
    public void ServedMemoryForgetsPastItsLimit()
    {
        var served = new ServedOnce(3);
        Assert.True(served.First("a"));
        Assert.False(served.First("a"));
        Assert.True(served.First("b"));
        Assert.True(served.First("c"));
        Assert.False(served.First("a"));
        Assert.True(served.First("d")); // full: forgets a, b and c
        Assert.True(served.First("a"));
        Assert.Equal(100_000, Hook.ServedLimit);
    }

    [Fact]
    public async Task UnknownAliasReturnsFalseAndSendsNothing()
    {
        using var rx = new Receiver("""[{"kind":"systemId","value":"100042","device":"EMS0000042"}]""");
        await using var hook = NewHook(rx.Url, aliases: rx.Url);

        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!hook.MarkTimeAlias(HopOp.Serve, "systemId", "100042", "ems.Measurement", At))
        {
            Assert.True(DateTime.UtcNow < deadline, "the alias list never arrived");
            await Task.Delay(50);
        }
        Assert.False(hook.MarkTimeAlias(HopOp.Serve, "systemId", "999999", "ems.Measurement", At));
        Assert.False(hook.MarkTimeAlias(HopOp.Store, "vin", "100042", "ems.Measurement", At));
        await hook.FlushAsync();

        var s = Assert.Single(rx.Spans);
        Assert.Equal("EMS0000042", s.Attrs["hopwake.device.id"]);
        Assert.Equal("systemId:100042", s.Attrs["hopwake.device.alias"]);
        Assert.Equal(JourneyId.Time(Tenant, "EMS0000042", "ems.Measurement", "2026-11-18T10:00:00Z"), s.TraceId);
    }

    [Fact]
    public async Task WithoutAnAliasListEveryAliasIsUnknown()
    {
        using var rx = new Receiver();
        await using var hook = NewHook(rx.Url, aliases: "");
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("HOPWAKE_API")))
        {
            return; // the environment gives this hook a list
        }
        Assert.False(hook.MarkTimeAlias(HopOp.Serve, "systemId", "100042", "ems.Measurement", At));
        Assert.Equal(0, hook.Pending);
        await hook.FlushAsync();
        Assert.Empty(rx.Spans);
    }

    [Fact]
    public void AliasListIsReadAsTheJourneyServiceWritesIt()
    {
        var pairs = Hook.ParseAliases("""[{"kind":"systemId","value":"100042","device":"EMS0000042"},{"kind":"vin","value":"WVW:ZZZ","device":"car-1"}]"""u8.ToArray());
        Assert.Equal("EMS0000042", pairs["systemId:100042"]);
        Assert.Equal("car-1", pairs["vin:WVW:ZZZ"]);
        Assert.ThrowsAny<Exception>(() => Hook.ParseAliases("""{"kind":"systemId"}"""u8.ToArray()));
    }

    [Fact]
    public async Task FullQueueDropsInsteadOfBlocking()
    {
        // A listener that never answers: each send hangs until its 3 s timeout.
        var silent = new TcpListener(IPAddress.Loopback, 0);
        silent.Start();
        try
        {
            var hook = NewHook("http://127.0.0.1:" + ((IPEndPoint)silent.LocalEndpoint).Port);
            const int marks = 200_000;
            var sw = Stopwatch.StartNew();
            for (ulong i = 0; i < marks; i++)
            {
                hook.Mark(HopOp.Store, "dev-00042", i);
            }
            sw.Stop();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"marking took {sw.Elapsed}");
            Assert.True(hook.Pending <= Hook.QueueCapacity);
            Assert.True(hook.Dropped >= marks - Hook.QueueCapacity - Hook.MaxBatch, $"dropped {hook.Dropped}");

            sw.Restart();
            await hook.DisposeAsync();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(8), $"closing took {sw.Elapsed}");
        }
        finally
        {
            silent.Stop();
        }
    }

    [Fact]
    public async Task UnreachableEndpointReturnsAtOnce()
    {
        int port;
        {
            var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop(); // nothing listens here now
        }
        var hook = NewHook("http://127.0.0.1:" + port);
        var sw = Stopwatch.StartNew();
        for (ulong i = 0; i < 1_000; i++)
        {
            hook.Mark(HopOp.Store, "dev-00042", i);
            hook.MarkTime(HopOp.Serve, "EMS0000042", "ems.Measurement", At.AddSeconds(i));
        }
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"marking took {sw.Elapsed}");

        sw.Restart();
        await hook.FlushAsync();
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(4), $"flushing took {sw.Elapsed}");

        sw.Restart();
        hook.Dispose();
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(6), $"closing took {sw.Elapsed}");
        hook.Mark(HopOp.Store, "dev-00042", 1); // after close: dropped, no exception
    }

    [Fact]
    public void BadSettingsDoNotThrowButMissingTenantOrHopDoes()
    {
        Assert.Throws<ArgumentException>(() => new Hook(new HookOptions { Tenant = Tenant }));
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("HOPWAKE_TENANT")))
        {
            Assert.Throws<ArgumentException>(() => new Hook(new HookOptions { Hop = HopName }));
        }
        using var hook = new Hook(new HookOptions { Tenant = Tenant, Hop = HopName, Endpoint = "not a url", Aliases = "::" });
        hook.Mark(HopOp.Store, "dev-00042", 1);
        hook.Mark(HopOp.Store, "", 1);
        Assert.False(hook.MarkTimeAlias(HopOp.Serve, "systemId", "100042", "t", At));
    }

    [Fact]
    public void PrecisionInCodeWinsOverTheEnvironment()
    {
        using var none = new Hook(new HookOptions { Tenant = Tenant, Hop = HopName, Endpoint = "http://127.0.0.1:1", TimePrecision = TimeSpan.Zero });
        Assert.Equal(TimeSpan.Zero, none.TimePrecision);
        using var minute = new Hook(new HookOptions { Tenant = Tenant, Hop = HopName, Endpoint = "http://127.0.0.1:1", TimePrecision = TimeSpan.FromMinutes(1) });
        Assert.Equal(TimeSpan.FromMinutes(1), minute.TimePrecision);
    }
}

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Hopwake;

/// <summary>
/// Marks readings at one hop, for hops no tap can watch from outside, such as
/// a database write or an HTTP response:
/// <code>
/// await using var hook = new Hook(new HookOptions { Tenant = "reference", Hop = "hook:recorder.stored" });
/// hook.Mark(HopOp.Store, device, seq);
/// </code>
/// Marks are queued and sent in the background as OTLP/JSON spans. When
/// Hopwake is not running they are dropped quietly: a hook never slows or
/// stops the service, and nothing it does throws into it. A reading is marked
/// served only the first time, however often it is asked for.
/// </summary>
public sealed class Hook : IDisposable, IAsyncDisposable
{
    internal const int QueueCapacity = 65_536;
    internal const int MaxBatch = 2_048;
    internal const int ServedLimit = 100_000;
    private static readonly TimeSpan SendEvery = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan AliasesEvery = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CloseWait = TimeSpan.FromSeconds(5);

    private readonly string _tenant;
    private readonly string _hop;
    private readonly string _service;
    private readonly TimeSpan _precision;
    private readonly Uri? _traces;
    private readonly Channel<Mark> _queue;
    private readonly ServedOnce _served = new(ServedLimit);
    private readonly HttpClient _http = new() { Timeout = SendTimeout };
    private readonly SemaphoreSlim _sending = new(1, 1);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _sender;
    private readonly Task? _aliasFollower;
    private volatile Dictionary<string, string> _aliases = new(StringComparer.Ordinal);
    private long _dropped;
    private int _disposed;

    /// <summary>Starts a hook. It does not contact Hopwake until the first marks are sent.</summary>
    /// <exception cref="ArgumentException">There is no tenant or no hop.</exception>
    public Hook(HookOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _tenant = Setting(options.Tenant, "HOPWAKE_TENANT") ?? "";
        _hop = options.Hop ?? "";
        if (_tenant.Length == 0 || _hop.Length == 0)
        {
            throw new ArgumentException("hook: tenant and hop are required", nameof(options));
        }
        _service = string.IsNullOrEmpty(options.Service) ? _hop : options.Service;
        _precision = options.TimePrecision
            ?? (Durations.TryParse(Environment.GetEnvironmentVariable("HOPWAKE_TIME_PRECISION"), out var p) ? p : TimeSpan.Zero);
        _traces = HttpUrl(Setting(options.Endpoint, "HOPWAKE_OTLP_ENDPOINT") ?? "http://localhost:4318", "/v1/traces");

        // A full queue drops new marks rather than block the service.
        _queue = Channel.CreateBounded<Mark>(
            new BoundedChannelOptions(QueueCapacity) { FullMode = BoundedChannelFullMode.DropWrite },
            _ => Interlocked.Increment(ref _dropped));
        _sender = Task.Run(SendLoopAsync);

        var aliases = HttpUrl(Setting(options.Aliases, "HOPWAKE_API"), "/api/aliases");
        if (aliases is not null)
        {
            _aliasFollower = Task.Run(() => FollowAliasesAsync(aliases));
        }
    }

    /// <summary>The time precision marks by time are rounded down to.</summary>
    public TimeSpan TimePrecision => _precision;

    /// <summary>Marks dropped so far because the queue was full.</summary>
    internal long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Marks waiting to be sent.</summary>
    internal int Pending => _queue.Reader.Count;

    /// <summary>Says what happened to one reading, now.</summary>
    public void Mark(HopOp op, string device, ulong seq) => MarkBoot(op, device, "", seq);

    /// <summary><see cref="Mark"/> for sources with a boot counter or boot ID.</summary>
    public void MarkBoot(HopOp op, string device, string boot, ulong seq)
    {
        try
        {
            if (string.IsNullOrEmpty(device))
            {
                return;
            }
            Enqueue(op, JourneyId.Seq(_tenant, device, seq, boot ?? ""), device, seq, null, null);
        }
        catch (Exception)
        {
            // never into the service
        }
    }

    /// <summary>
    /// <see cref="Mark"/> for readings without a sequence number: they are known
    /// by device, message type and the time the device gave them.
    /// </summary>
    public void MarkTime(HopOp op, string device, string messageType, DateTimeOffset at) =>
        MarkTimeCore(op, device, null, messageType, at);

    /// <summary>
    /// <see cref="MarkTime"/> at a hop that knows both the device and another
    /// name it goes by further down, such as a system ID. Hopwake learns the pair.
    /// </summary>
    public void MarkTimeTeach(HopOp op, string device, string aliasKind, string aliasValue, string messageType, DateTimeOffset at) =>
        MarkTimeCore(op, device, Alias.Format(aliasKind, aliasValue), messageType, at);

    /// <summary>
    /// <see cref="MarkTime"/> at a hop that knows the device only by an alias.
    /// Returns false, and sends nothing, when Hopwake has not learned the alias yet.
    /// </summary>
    public bool MarkTimeAlias(HopOp op, string aliasKind, string aliasValue, string messageType, DateTimeOffset at)
    {
        try
        {
            var alias = Alias.Format(aliasKind, aliasValue);
            if (!_aliases.TryGetValue(alias, out var device))
            {
                return false;
            }
            MarkTimeCore(op, device, alias, messageType, at);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>Sends what is queued now. Failures are swallowed; each send gives up after 3 seconds.</summary>
    public Task FlushAsync() => FlushAsync(CancellationToken.None);

    /// <inheritdoc cref="FlushAsync()"/>
    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        try
        {
            await SendQueuedAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // never into the service
        }
    }

    /// <summary>Stops the hook and sends what is queued, waiting at most a few seconds.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        using var deadline = new CancellationTokenSource(CloseWait);
        _stop.Cancel();
        _queue.Writer.TryComplete();
        try
        {
            await _sender.WaitAsync(deadline.Token).ConfigureAwait(false);
            while (_queue.Reader.Count > 0 && await SendQueuedAsync(deadline.Token).ConfigureAwait(false))
            {
            }
        }
        catch (Exception)
        {
            // what is left is lost
        }
        if (_aliasFollower is not null)
        {
            try
            {
                await _aliasFollower.WaitAsync(deadline.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
        _http.Dispose();
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose() => Task.Run(() => DisposeAsync().AsTask()).GetAwaiter().GetResult();

    private void MarkTimeCore(HopOp op, string device, string? alias, string messageType, DateTimeOffset at)
    {
        try
        {
            if (string.IsNullOrEmpty(device))
            {
                return;
            }
            messageType ??= "";
            var id = JourneyId.Time(_tenant, device, messageType, HopTime.Format(at, _precision));
            Enqueue(op, id, device, 0, messageType, alias);
        }
        catch (Exception)
        {
            // never into the service
        }
    }

    private void Enqueue(HopOp op, string journeyId, string device, ulong seq, string? messageType, string? alias)
    {
        if (op == HopOp.Serve && !_served.First(journeyId))
        {
            return;
        }
        _queue.Writer.TryWrite(new Mark(journeyId, NewSpanId(), NowUnixNanos(), device, seq, op, messageType, alias));
    }

    private async Task SendLoopAsync()
    {
        using var timer = new PeriodicTimer(SendEvery);
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
            {
                await SendQueuedAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // Sends the marks queued when it starts, in batches. It stops at the first
    // batch that fails and leaves the rest queued for the next round.
    private async Task<bool> SendQueuedAsync(CancellationToken ct)
    {
        await _sending.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            int left = _queue.Reader.Count;
            var batch = new List<Mark>(Math.Min(left, MaxBatch));
            while (left > 0)
            {
                batch.Clear();
                while (batch.Count < MaxBatch && left > 0 && _queue.Reader.TryRead(out var m))
                {
                    batch.Add(m);
                    left--;
                }
                if (batch.Count == 0)
                {
                    break;
                }
                if (!await PostAsync(batch, ct).ConfigureAwait(false))
                {
                    return false;
                }
            }
            return true;
        }
        finally
        {
            _sending.Release();
        }
    }

    private async Task<bool> PostAsync(List<Mark> batch, CancellationToken ct)
    {
        if (_traces is null)
        {
            return false;
        }
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(SendTimeout);
            using var content = new ByteArrayContent(Otlp.Encode(_service, _tenant, _hop, batch));
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            using var resp = await _http.PostAsync(_traces, content, cts.Token).ConfigureAwait(false);
            return resp.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false; // Hopwake is not reachable: these marks are lost
        }
    }

    private async Task FollowAliasesAsync(Uri url)
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                cts.CancelAfter(SendTimeout);
                using var resp = await _http.GetAsync(url, cts.Token).ConfigureAwait(false);
                if (resp.StatusCode == HttpStatusCode.OK)
                {
                    var body = await resp.Content.ReadAsByteArrayAsync(cts.Token).ConfigureAwait(false);
                    _aliases = ParseAliases(body);
                }
            }
            catch (Exception)
            {
                // a failed fetch keeps the list we have
            }
            try
            {
                await Task.Delay(AliasesEvery, _stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>Reads the alias list: <c>[{"kind": "systemId", "value": "100042", "device": "EMS0000042"}]</c>.</summary>
    internal static Dictionary<string, string> ParseAliases(byte[] json)
    {
        using var doc = JsonDocument.Parse(json);
        var pairs = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var e in doc.RootElement.EnumerateArray())
        {
            var device = Text(e, "device");
            if (device.Length > 0)
            {
                pairs[Alias.Format(Text(e, "kind"), Text(e, "value"))] = device;
            }
        }
        return pairs;

        static string Text(JsonElement e, string name) =>
            e.TryGetProperty(name, out var v) ? v.GetString() ?? "" : "";
    }

    private static string? Setting(string? value, string env)
    {
        if (!string.IsNullOrEmpty(value))
        {
            return value;
        }
        var fromEnv = Environment.GetEnvironmentVariable(env);
        return string.IsNullOrEmpty(fromEnv) ? null : fromEnv;
    }

    private static Uri? HttpUrl(string? baseUrl, string path)
    {
        if (string.IsNullOrEmpty(baseUrl)
            || !Uri.TryCreate(baseUrl.TrimEnd('/') + path, UriKind.Absolute, out var u)
            || (u.Scheme != Uri.UriSchemeHttp && u.Scheme != Uri.UriSchemeHttps))
        {
            return null;
        }
        return u;
    }

    private static string NewSpanId()
    {
        Span<byte> b = stackalloc byte[8];
        do
        {
            RandomNumberGenerator.Fill(b);
        }
        while (BinaryPrimitives.ReadUInt64LittleEndian(b) == 0);
        return Convert.ToHexString(b).ToLowerInvariant();
    }

    private static long NowUnixNanos() =>
        (DateTimeOffset.UtcNow.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) * 100;
}

/// <summary>Remembers served journey IDs, up to a limit; past it, forgets them all and starts again.</summary>
internal sealed class ServedOnce
{
    private readonly int _limit;
    private readonly HashSet<string> _ids = new(StringComparer.Ordinal);
    private readonly object _lock = new();

    public ServedOnce(int limit) => _limit = limit;

    /// <summary>True the first time a journey ID is served.</summary>
    public bool First(string journeyId)
    {
        lock (_lock)
        {
            if (_ids.Contains(journeyId))
            {
                return false;
            }
            if (_ids.Count >= _limit)
            {
                _ids.Clear();
            }
            _ids.Add(journeyId);
            return true;
        }
    }
}

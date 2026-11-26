using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Hopwake.Tests;

/// <summary>One span as it arrived.</summary>
internal sealed record ReceivedSpan(
    string TraceId, string SpanId, string Name, int Kind, string Start, string End, string Service, Dictionary<string, string> Attrs);

/// <summary>Stands in for Hopwake: takes OTLP/JSON spans and serves an alias list.</summary>
internal sealed class Receiver : IDisposable
{
    private readonly HttpListener _listener = new();
    private readonly List<ReceivedSpan> _spans = new();
    private readonly string _aliases;

    public Receiver(string aliases = "[]")
    {
        _aliases = aliases;
        Url = "http://127.0.0.1:" + FreePort();
        _listener.Prefixes.Add(Url + "/");
        _listener.Start();
        _ = Task.Run(ServeAsync);
    }

    public string Url { get; }

    public List<ReceivedSpan> Spans
    {
        get
        {
            lock (_spans)
            {
                return new List<ReceivedSpan>(_spans);
            }
        }
    }

    public void Dispose() => _listener.Close();

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private async Task ServeAsync()
    {
        while (_listener.IsListening)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync();
            }
            catch (Exception)
            {
                return;
            }
            try
            {
                var path = ctx.Request.Url!.AbsolutePath;
                byte[] reply = "{}"u8.ToArray();
                if (ctx.Request.HttpMethod == "POST" && path == "/v1/traces")
                {
                    using var r = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                    var spans = Decode(await r.ReadToEndAsync());
                    lock (_spans)
                    {
                        _spans.AddRange(spans);
                    }
                }
                else if (ctx.Request.HttpMethod == "GET" && path == "/api/aliases")
                {
                    reply = Encoding.UTF8.GetBytes(_aliases);
                }
                else
                {
                    ctx.Response.StatusCode = 404;
                }
                ctx.Response.ContentType = "application/json";
                await ctx.Response.OutputStream.WriteAsync(reply);
                ctx.Response.Close();
            }
            catch (Exception)
            {
                ctx.Response.Abort();
            }
        }
    }

    private static List<ReceivedSpan> Decode(string body)
    {
        var list = new List<ReceivedSpan>();
        using var doc = JsonDocument.Parse(body);
        foreach (var rs in doc.RootElement.GetProperty("resourceSpans").EnumerateArray())
        {
            var service = Attrs(rs.GetProperty("resource").GetProperty("attributes"))["service.name"];
            foreach (var ss in rs.GetProperty("scopeSpans").EnumerateArray())
            {
                foreach (var s in ss.GetProperty("spans").EnumerateArray())
                {
                    list.Add(new ReceivedSpan(
                        s.GetProperty("traceId").GetString()!,
                        s.GetProperty("spanId").GetString()!,
                        s.GetProperty("name").GetString()!,
                        s.GetProperty("kind").GetInt32(),
                        s.GetProperty("startTimeUnixNano").GetString()!,
                        s.GetProperty("endTimeUnixNano").GetString()!,
                        service,
                        Attrs(s.GetProperty("attributes"))));
                }
            }
        }
        return list;
    }

    private static Dictionary<string, string> Attrs(JsonElement attrs)
    {
        var m = new Dictionary<string, string>();
        foreach (var a in attrs.EnumerateArray())
        {
            var v = a.GetProperty("value");
            m[a.GetProperty("key").GetString()!] = v.TryGetProperty("stringValue", out var sv)
                ? sv.GetString()!
                : "int:" + v.GetProperty("intValue").GetString();
        }
        return m;
    }
}

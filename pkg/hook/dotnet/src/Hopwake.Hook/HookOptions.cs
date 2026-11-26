using System;

namespace Hopwake;

/// <summary>What a hook can say happened to a reading.</summary>
public enum HopOp
{
    /// <summary>The service kept the reading.</summary>
    Store,

    /// <summary>The service handed the reading to someone.</summary>
    Serve,
}

/// <summary>
/// Where a hook's marks go and what they are called. A setting left null (or
/// empty) is taken from the environment, as docs/hooks.md lists.
/// </summary>
public sealed class HookOptions
{
    /// <summary>The tenant, as in the rules file. Default <c>HOPWAKE_TENANT</c>; required.</summary>
    public string? Tenant { get; set; }

    /// <summary>The hop name, matching a hook source in the rules file: <c>hook:</c> + match. Required.</summary>
    public string? Hop { get; set; }

    /// <summary>Hopwake's OTLP/HTTP address. Default <c>HOPWAKE_OTLP_ENDPOINT</c>, else <c>http://localhost:4318</c>.</summary>
    public string? Endpoint { get; set; }

    /// <summary>The <c>service.name</c> on the spans. Default the hop name.</summary>
    public string? Service { get; set; }

    /// <summary>
    /// The rules file's <c>time_precision</c>, so marks made by time agree with
    /// the taps' journey IDs. Default <c>HOPWAKE_TIME_PRECISION</c>, else none.
    /// <see cref="TimeSpan.Zero"/> means none, whatever the environment says.
    /// </summary>
    public TimeSpan? TimePrecision { get; set; }

    /// <summary>
    /// The journey service's base URL, for marks made under an alias. Default
    /// <c>HOPWAKE_API</c>; without it <see cref="Hook.MarkTimeAlias"/> always says unknown.
    /// </summary>
    public string? Aliases { get; set; }
}

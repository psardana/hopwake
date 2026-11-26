package dev.hopwake.hook;

import java.time.Duration;
import java.util.Map;

/**
 * Where marks go and what they are called. Anything left unset is read from the environment
 * (docs/hooks.md, "Configuration"): HOPWAKE_TENANT, HOPWAKE_OTLP_ENDPOINT,
 * HOPWAKE_TIME_PRECISION and HOPWAKE_API.
 */
public final class HookConfig {
    String tenant;
    String hop;
    String endpoint;
    String service;
    Duration timePrecision;
    String aliases;
    int queueSize = 50_000;

    private HookConfig() {
    }

    /** A configuration for one hop, such as "hook:lvc.stored". */
    public static HookConfig forHop(String hop) {
        HookConfig c = new HookConfig();
        c.hop = hop;
        return c;
    }

    public HookConfig tenant(String tenant) {
        this.tenant = tenant;
        return this;
    }

    /** Hopwake's OTLP/HTTP endpoint, without /v1/traces. */
    public HookConfig endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    /** The service.name on the spans; default the hop name. */
    public HookConfig service(String service) {
        this.service = service;
        return this;
    }

    /** Must match time_precision in the rules file, so marks by time agree with the taps. */
    public HookConfig timePrecision(Duration timePrecision) {
        this.timePrecision = timePrecision;
        return this;
    }

    /** The journey service's base URL, for marks made under an alias. */
    public HookConfig aliases(String aliases) {
        this.aliases = aliases;
        return this;
    }

    /** How many marks may wait to be sent; past this they are dropped. */
    public HookConfig queueSize(int queueSize) {
        this.queueSize = Math.max(10_000, queueSize);
        return this;
    }

    HookConfig resolve(Map<String, String> env) {
        HookConfig c = new HookConfig();
        c.hop = hop;
        c.tenant = first(tenant, env.get("HOPWAKE_TENANT"));
        c.endpoint = first(endpoint, env.get("HOPWAKE_OTLP_ENDPOINT"), "http://localhost:4318");
        c.service = first(service, hop);
        c.timePrecision = timePrecision != null ? timePrecision : HopTimes.parseDuration(env.get("HOPWAKE_TIME_PRECISION"));
        c.aliases = first(aliases, env.get("HOPWAKE_API"));
        c.queueSize = queueSize;
        if (c.tenant == null || c.hop == null || c.hop.isEmpty()) {
            throw new IllegalArgumentException("hopwake: tenant and hop are required");
        }
        if (c.endpoint.endsWith("/")) {
            c.endpoint = c.endpoint.substring(0, c.endpoint.length() - 1);
        }
        return c;
    }

    private static String first(String... values) {
        for (String v : values) {
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }
}

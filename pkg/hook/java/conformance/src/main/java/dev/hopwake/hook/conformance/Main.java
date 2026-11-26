package dev.hopwake.hook.conformance;

import dev.hopwake.hook.Hook;
import dev.hopwake.hook.HookConfig;
import dev.hopwake.hook.Op;
import java.time.Instant;

/** The hook conformance script (docs/hooks.md), run under cmd/hookcheck. */
public final class Main {
    public static void main(String[] args) throws Exception {
        Instant at = Instant.parse("2026-11-18T10:00:00.750Z");
        String type = "ems.measurements.Measurement";
        try (Hook h = Hook.create(HookConfig.forHop(System.getenv("HOPWAKE_HOP")).service("conformance-java"))) {
            h.mark(Op.STORE, "dev-00042", 1683);
            h.markBoot(Op.STORE, "dev-00042", "7", 12);
            h.mark(Op.SERVE, "dev-00042", 1683);
            h.mark(Op.SERVE, "dev-00042", 1683); // the same reading served again: not sent
            h.markTime(Op.STORE, "EMS0000042", type, at);
            h.markTimeTeach(Op.STORE, "EMS0000042", "systemId", "100042", type, at);
            long deadline = System.currentTimeMillis() + 15_000;
            while (!h.markTimeAlias(Op.SERVE, "systemId", "100042", type, at)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("the alias list never arrived");
                }
                Thread.sleep(100);
            }
            if (h.markTimeAlias(Op.SERVE, "systemId", "999999", type, at)) {
                throw new IllegalStateException("an unknown alias was marked");
            }
        }
    }
}

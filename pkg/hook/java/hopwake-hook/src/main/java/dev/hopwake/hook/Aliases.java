package dev.hopwake.hook;

/** Other names a device goes by further down a pipeline, such as a system ID. */
public final class Aliases {
    private Aliases() {
    }

    /** An alias as it travels on hop events: "kind:value". */
    public static String format(String kind, String value) {
        return kind + ":" + value;
    }
}

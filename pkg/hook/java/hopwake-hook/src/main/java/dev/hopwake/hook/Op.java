package dev.hopwake.hook;

/** What a service did with a reading. */
public enum Op {
    /** The service kept the reading, for example in a database. */
    STORE("store"),
    /** The service handed the reading to someone, for example in an API response. */
    SERVE("serve"),
    /** The service took the reading in, for example from a Kafka topic. */
    RECEIVE("receive"),
    /** The service passed the reading on, for example to another topic. */
    ROUTE("route");

    private final String wire;

    Op(String wire) {
        this.wire = wire;
    }

    /** The op as it travels on hop events. */
    public String wire() {
        return wire;
    }
}

package dev.hopwake.hook;

import java.time.Instant;
import java.util.Objects;

/**
 * Which reading a mark is about, in one of the ways Hopwake can know it: by sequence number, by
 * message type and time, by type and time while teaching an alias, or by an alias alone.
 */
public final class Reading {
    enum Kind { SEQ, TIME, TEACH, ALIAS }

    final Kind kind;
    final String device;
    final String boot;
    final long seq;
    final String type;
    final Instant time;
    final String aliasKind;
    final String aliasValue;

    private Reading(Kind kind, String device, String boot, long seq, String type, Instant time, String aliasKind, String aliasValue) {
        this.kind = kind;
        this.device = device;
        this.boot = boot;
        this.seq = seq;
        this.type = type;
        this.time = time;
        this.aliasKind = aliasKind;
        this.aliasValue = aliasValue;
    }

    /** A reading with a sequence number. */
    public static Reading seq(String device, long seq) {
        return new Reading(Kind.SEQ, Objects.requireNonNull(device), "", seq, null, null, null, null);
    }

    /** A reading with a boot counter and a sequence number. */
    public static Reading seq(String device, String boot, long seq) {
        return new Reading(Kind.SEQ, Objects.requireNonNull(device), boot == null ? "" : boot, seq, null, null, null, null);
    }

    /** A reading known by its device, message type and the time the device gave it. */
    public static Reading time(String device, String messageType, Instant at) {
        return new Reading(Kind.TIME, Objects.requireNonNull(device), "", 0, messageType, Objects.requireNonNull(at), null, null);
    }

    /** A reading known only by another name of its device, such as a system ID. */
    public static Reading byAlias(String aliasKind, String aliasValue, String messageType, Instant at) {
        return new Reading(Kind.ALIAS, null, "", 0, messageType, Objects.requireNonNull(at), Objects.requireNonNull(aliasKind), Objects.requireNonNull(aliasValue));
    }

    /** This time-known reading, also telling Hopwake that the device goes by {@code kind:value} further down. */
    public Reading teach(String aliasKind, String aliasValue) {
        if (kind != Kind.TIME) {
            throw new IllegalStateException("only a reading known by type and time can teach an alias");
        }
        return new Reading(Kind.TEACH, device, "", 0, type, time, Objects.requireNonNull(aliasKind), Objects.requireNonNull(aliasValue));
    }

    @Override
    public String toString() {
        switch (kind) {
            case SEQ: return device + (boot.isEmpty() ? "" : " boot " + boot) + " seq " + Long.toUnsignedString(seq);
            case TIME: return device + " " + type + " at " + time;
            case TEACH: return device + " (" + aliasKind + ":" + aliasValue + ") " + type + " at " + time;
            default: return aliasKind + ":" + aliasValue + " " + type + " at " + time;
        }
    }
}

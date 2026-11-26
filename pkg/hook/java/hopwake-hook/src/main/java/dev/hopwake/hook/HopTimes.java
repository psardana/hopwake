package dev.hopwake.hook;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;

/** Reading times as they enter a journey ID, and durations as Hopwake writes them. */
public final class HopTimes {
    private static final long NANOS = 1_000_000_000L;

    private HopTimes() {
    }

    /**
     * Rounds {@code at} down to {@code precision} (null or zero keeps it) and writes it in UTC,
     * with a fraction of up to nine digits and no trailing zeros.
     */
    public static String format(Instant at, Duration precision) {
        long sec = at.getEpochSecond();
        long nanos = at.getNano();
        if (precision != null && !precision.isZero() && !precision.isNegative()) {
            long p = precision.toNanos();
            if (p < NANOS) {
                nanos -= nanos % p;
            } else {
                long ps = p / NANOS;
                sec = Math.floorDiv(sec, ps) * ps;
                nanos = 0;
            }
        }
        LocalDateTime t = LocalDateTime.ofEpochSecond(sec, 0, ZoneOffset.UTC);
        StringBuilder b = new StringBuilder(32);
        b.append(String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d",
                t.getYear(), t.getMonthValue(), t.getDayOfMonth(), t.getHour(), t.getMinute(), t.getSecond()));
        if (nanos != 0) {
            String f = String.format(Locale.ROOT, "%09d", nanos);
            int end = f.length();
            while (f.charAt(end - 1) == '0') {
                end--;
            }
            b.append('.').append(f, 0, end);
        }
        return b.append('Z').toString();
    }

    /** The time of {@code unixNanos} nanoseconds after the Unix epoch. */
    public static Instant ofUnixNanos(long unixNanos) {
        return Instant.ofEpochSecond(Math.floorDiv(unixNanos, NANOS), Math.floorMod(unixNanos, NANOS));
    }

    /** Parses a duration written as Hopwake writes them: "1s", "500ms", "1m30s", "250us", "1h". */
    public static Duration parseDuration(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        String in = s.trim();
        long total = 0;
        int i = 0;
        while (i < in.length()) {
            int start = i;
            while (i < in.length() && (Character.isDigit(in.charAt(i)) || in.charAt(i) == '.')) {
                i++;
            }
            if (start == i) {
                throw new IllegalArgumentException("not a duration: " + s);
            }
            double n = Double.parseDouble(in.substring(start, i));
            int u = i;
            while (i < in.length() && !Character.isDigit(in.charAt(i))) {
                i++;
            }
            long unit;
            switch (in.substring(u, i)) {
                case "ns": unit = 1; break;
                case "us": case "µs": unit = 1_000; break;
                case "ms": unit = 1_000_000; break;
                case "s": unit = NANOS; break;
                case "m": unit = 60 * NANOS; break;
                case "h": unit = 3600 * NANOS; break;
                default: throw new IllegalArgumentException("not a duration: " + s);
            }
            total += Math.round(n * unit);
        }
        return Duration.ofNanos(total);
    }
}

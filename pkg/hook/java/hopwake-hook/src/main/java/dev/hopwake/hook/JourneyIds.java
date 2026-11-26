package dev.hopwake.hook;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Journey IDs, derived exactly as Hopwake's taps derive them (docs/hooks.md). */
public final class JourneyIds {
    private JourneyIds() {
    }

    /** The journey ID of a reading with a sequence number. {@code seq} is unsigned. */
    public static String seq(String tenant, String device, long seq) {
        return seq(tenant, device, "", seq);
    }

    /** As {@link #seq(String, String, long)}, for sources with a boot counter; an empty boot is no boot. */
    public static String seq(String tenant, String device, String boot, long seq) {
        MessageDigest h = sha256();
        field(h, "hopwake/journey/v1");
        field(h, tenant);
        field(h, device);
        if (boot != null && !boot.isEmpty()) {
            field(h, "boot:" + boot);
        }
        h.update(ByteBuffer.allocate(8).putLong(seq).array());
        return id(h.digest());
    }

    /** The journey ID of a reading known by its device, message type and formatted time. */
    public static String time(String tenant, String device, String messageType, String formattedTime) {
        MessageDigest h = sha256();
        field(h, "hopwake/journey/time/v1");
        field(h, tenant);
        field(h, device);
        field(h, "type:" + (messageType == null ? "" : messageType));
        field(h, "time:" + formattedTime);
        return id(h.digest());
    }

    private static void field(MessageDigest h, String s) {
        byte[] b = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
        h.update(ByteBuffer.allocate(4).putInt(b.length).array());
        h.update(b);
    }

    private static String id(byte[] sum) {
        byte[] id = new byte[16];
        System.arraycopy(sum, 0, id, 0, 16);
        boolean zero = true;
        for (byte x : id) {
            zero &= x == 0;
        }
        if (zero) {
            id[15] = 1;
        }
        return Hex.of(id);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

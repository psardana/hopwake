package hopwake.windows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.SortedSet;

/**
 * Describes one aggregated output so Hopwake can find the readings behind it.
 *
 * <p>An aggregate stands for many readings, so instead of linking each one, the job adds a
 * window record to its output as a Kafka header: the operator, the group key, the window, the hop
 * its inputs were read from, how many there were and which devices sent them. The Hopwake tap
 * reads the header from the output topic. Above {@link #MAX_DEVICES} devices only a digest of
 * the device list is sent.
 */
public final class WindowRecord {
  /** The header the tap reads. */
  public static final String HEADER = "hopwake-window";

  /** Above this many devices, the record carries a digest instead of the list. */
  public static final int MAX_DEVICES = 256;

  private static final ObjectMapper JSON = new ObjectMapper();

  private WindowRecord() {}

  /**
   * Builds the header value.
   *
   * @param operator what the output is, e.g. "median"
   * @param key the group key, e.g. the site
   * @param start window start
   * @param end window end
   * @param inputHop the Hopwake hop name the inputs were read from, e.g. "kafka:telemetry.normalized"
   * @param inputs how many readings went in
   * @param devices the devices that sent them
   */
  public static byte[] header(String operator, String key, Instant start, Instant end, String inputHop,
      int inputs, SortedSet<String> devices) {
    ObjectNode n = JSON.createObjectNode();
    n.put("v", 1);
    n.put("op", operator);
    n.put("key", key);
    n.put("start", start.toString());
    n.put("end", end.toString());
    n.put("input_hop", inputHop);
    n.put("inputs", inputs);
    n.put("device_count", devices.size());
    if (devices.size() <= MAX_DEVICES) {
      ArrayNode list = n.putArray("devices");
      devices.forEach(list::add);
    } else {
      n.put("device_digest", digest(devices));
    }
    return n.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** SHA-256 of the sorted device list, one device per line, in hex. */
  static String digest(SortedSet<String> devices) {
    try {
      MessageDigest d = MessageDigest.getInstance("SHA-256");
      for (String dev : devices) {
        d.update(dev.getBytes(StandardCharsets.UTF_8));
        d.update((byte) '\n');
      }
      return HexFormat.of().formatHex(d.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}

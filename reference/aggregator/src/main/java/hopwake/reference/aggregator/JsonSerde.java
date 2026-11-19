package hopwake.reference.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** A Kafka serde that stores values as JSON. */
public final class JsonSerde<T> implements Serde<T> {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Class<T> type;

  public JsonSerde(Class<T> type) {
    this.type = type;
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, value) -> {
      if (value == null) {
        return null;
      }
      try {
        return JSON.writeValueAsBytes(value);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> {
      if (bytes == null) {
        return null;
      }
      try {
        return JSON.readValue(bytes, type);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    };
  }
}

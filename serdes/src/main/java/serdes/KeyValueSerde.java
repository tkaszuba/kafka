package serdes;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerde<K, V> implements Serde<KeyValue<K, V>> {

  private final Serde<KeyValue<K, V>> inner;

  public KeyValueSerde(Serde<K> keySerde, Serde<V> valueSerde) {
    inner =
        Serdes.serdeFrom(
            new KeyValueSerializer<>(keySerde, valueSerde),
            new KeyValueDeserializer<>(keySerde, valueSerde));
  }

  @Override
  public Serializer<KeyValue<K, V>> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<KeyValue<K, V>> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }
}

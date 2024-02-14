package com.tkaszuba.kafka.serdes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerializer<K, V> implements Serializer<KeyValue<K, V>> {

  private final Serializer<K> keySerde;
  private final Serializer<V> valueSerde;

  public KeyValueSerializer(Serde<K> keySerde, Serde<V> valueSerde) {
    this(keySerde.serializer(), valueSerde.serializer());
  }

  public KeyValueSerializer(Serializer<K> keySerde, Serializer<V> valueSerde) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // do nothing
  }

  @Override
  public byte[] serialize(String topic, KeyValue<K, V> data) {
    if (data == null) return null;

    byte[] key = keySerde.serialize(topic, data.key);
    byte[] value = valueSerde.serialize(topic, data.value);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);

    try {
      dos.writeInt(key.length);
      dos.writeInt(value.length);
    } catch (NullPointerException e) {
      throw new SerializationException(
          "The serializer doesn't support null values; the stored key/values must not be null", e);
    } catch (IOException e) {
      throw new SerializationException("Unable to serialize KeyValue", e);
    }

    return combine(baos.toByteArray(), key, value);
  }

  @Override
  public void close() {
    // do nothing
  }

  public static byte[] combine(byte[] a, byte[] b, byte[] c) {
    int length = a.length + b.length + c.length;
    byte[] result = new byte[length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }
}

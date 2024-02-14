package com.tkaszuba.kafka.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.common.errors.SerializationException;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

public class KeyValueDeserializer<K, V> implements Deserializer<KeyValue<K, V>> {

  private final Deserializer<K> keySerde;
  private final Deserializer<V> valueSerde;

  public KeyValueDeserializer(Serde<K> keySerde, Serde<V> valueSerde) {
    this(keySerde.deserializer(), valueSerde.deserializer());
  }

  public KeyValueDeserializer(Deserializer<K> keySerde, Deserializer<V> valueSerde) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // do nothing
  }

  @Override
  public KeyValue<K, V> deserialize(String topic, byte[] bytes) {
    if (bytes == null || bytes.length == 0) return null;

    final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    try {
      final int keySize = dataInputStream.readInt();
      final int valueSize = dataInputStream.readInt();

      int start = Integer.BYTES * 2;

      byte[] key = Arrays.copyOfRange(bytes, start, start + keySize);
      byte[] value = Arrays.copyOfRange(bytes, start + keySize, start + keySize + valueSize);

      return new KeyValue<>(keySerde.deserialize(topic, key), valueSerde.deserialize(topic, value));

    } catch (IOException e) {
      throw new SerializationException("Unable to deserialize into KeyValue", e);
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}

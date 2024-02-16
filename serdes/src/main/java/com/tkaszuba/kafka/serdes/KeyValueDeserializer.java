/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tkaszuba.kafka.serdes;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;

public class KeyValueDeserializer<K, V> implements Deserializer<KeyValue<K, V>> {

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  public KeyValueDeserializer(Serde<K> keySerde, Serde<V> valueSerde) {
    this(keySerde.deserializer(), valueSerde.deserializer());
  }

  public KeyValueDeserializer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
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

      return new KeyValue<>(
          keyDeserializer.deserialize(topic, key), valueDeserializer.deserialize(topic, value));

    } catch (IOException e) {
      throw new SerializationException("Unable to deserialize into KeyValue", e);
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}

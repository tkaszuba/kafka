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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerializer<K, V> implements Serializer<KeyValue<K, V>> {

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public KeyValueSerializer(Serde<K> keySerde, Serde<V> valueSerde) {
    this(keySerde.serializer(), valueSerde.serializer());
  }

  public KeyValueSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // do nothing
  }

  @Override
  public byte[] serialize(String topic, KeyValue<K, V> data) {
    if (data == null) return null;

    byte[] key = keySerializer.serialize(topic, data.key);
    byte[] value = valueSerializer.serialize(topic, data.value);

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

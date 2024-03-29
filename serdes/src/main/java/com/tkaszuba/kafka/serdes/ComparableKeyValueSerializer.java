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

import com.tkaszuba.kafka.streams.ComparableKeyValue;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class ComparableKeyValueSerializer<K extends Comparable<?>, V extends Comparable<?>>
    implements Serializer<ComparableKeyValue<K, V>> {

  private final Serializer<KeyValue<K, V>> serializer;

  public ComparableKeyValueSerializer(Serde<KeyValue<K, V>> serde) {
    this(serde.serializer());
  }

  public ComparableKeyValueSerializer(Serializer<KeyValue<K, V>> serializer) {
    this.serializer = serializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, ComparableKeyValue<K, V> data) {
    return serializer.serialize(topic, data);
  }

  @Override
  public void close() {
    serializer.close();
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

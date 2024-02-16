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
import java.util.Optional;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;

public class ComparableKeyValueDeserializer<K extends Comparable<?>, V extends Comparable<?>>
    implements Deserializer<ComparableKeyValue<K, V>> {

  private final Deserializer<KeyValue<K, V>> deserializer;

  public ComparableKeyValueDeserializer(Serde<KeyValue<K, V>> serde) {
    this(serde.deserializer());
  }

  public ComparableKeyValueDeserializer(Deserializer<KeyValue<K, V>> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    deserializer.configure(configs, isKey);
  }

  @Override
  public ComparableKeyValue<K, V> deserialize(String topic, byte[] bytes) {
    return Optional.ofNullable(deserializer.deserialize(topic, bytes))
        .map(ComparableKeyValue::new)
        .orElse(null);
  }

  @Override
  public void close() {
    deserializer.close();
  }
}

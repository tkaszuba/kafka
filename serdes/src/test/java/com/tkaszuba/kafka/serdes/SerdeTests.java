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

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

class SerdeTests {

  private static final String TOPIC = "topic";

  @Test
  void testKeyValueSerde() {
    KeyValue<Long, String> keyValue = KeyValue.pair(1L, "test");

    KeyValueSerde<Long, String> serde = new KeyValueSerde<>(Serdes.Long(), Serdes.String());

    assertDoesNotThrow(() -> serde.serializer().configure(Collections.emptyMap(), false));
    assertDoesNotThrow(() -> serde.deserializer().configure(Collections.emptyMap(), false));

    assertThrows(
        SerializationException.class,
        () -> serde.serializer().serialize(TOPIC, new KeyValue<>(null, "test")));

    assertEquals(
        keyValue,
        serde.deserializer().deserialize(TOPIC, serde.serializer().serialize(TOPIC, keyValue)));

    // Tombstones
    assertNull(serde.deserializer().deserialize(TOPIC, serde.serializer().serialize(TOPIC, null)));

    serde.close();
  }
}

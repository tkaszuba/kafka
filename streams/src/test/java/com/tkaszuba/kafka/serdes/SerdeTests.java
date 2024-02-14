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
    KeyValue<Long, String> keyValue = new KeyValue<>(1L, "test");

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

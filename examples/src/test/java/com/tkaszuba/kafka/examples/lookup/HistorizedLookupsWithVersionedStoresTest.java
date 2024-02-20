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
package com.tkaszuba.kafka.examples.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tkaszuba.kafka.serdes.KeyValueSerde;
import com.tkaszuba.kafka.streams.examples.lookup.HistorizedLookupsWithVersionedStores;
import java.time.LocalDate;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HistorizedLookupsWithVersionedStoresTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, KeyValue<String, Long>> queryCommandTopic;
  private TestInputTopic<String, KeyValue<String, Long>> lookupTopic;
  private TestOutputTopic<String, String> queryResponseTopic;

  private final String key1 = "Key1";
  private final String key2 = "Key2";
  private final String commandId = "CommandId";

  private final KeyValue<String, Long> key1V1 = create("key1V1", LocalDate.of(2021, 1, 1));
  private final KeyValue<String, Long> key1V2 = create("key1V2", LocalDate.of(2021, 2, 1));
  private final KeyValue<String, Long> key1V3 = create("key1V3", LocalDate.of(2021, 3, 1));
  private final KeyValue<String, Long> key1V4 = create("key1V4", LocalDate.of(2021, 4, 1));
  private final KeyValue<String, Long> key2V1 = create("key2V1", LocalDate.of(2022, 1, 1));

  private final KeyValue<String, Long> key2V2 = create("key2V2", LocalDate.of(2022, 2, 1));
  private final KeyValue<String, Long> key2V3 = create("key2V3", LocalDate.of(2022, 3, 1));
  private final KeyValue<String, Long> key2V4 = create("key2V4", LocalDate.of(2022, 4, 1));

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<Long> longSerde = Serdes.Long();

  private final Serde<KeyValue<String, Long>> keyValueSerde =
      new KeyValueSerde<>(stringSerde, longSerde);

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    HistorizedLookupsWithVersionedStores.createStream(builder);

    final Properties props = new Properties();
    props.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

    testDriver = new TopologyTestDriver(builder.build(), props);

    lookupTopic =
        testDriver.createInputTopic(
            HistorizedLookupsWithVersionedStores.LookupTopic,
            stringSerde.serializer(),
            keyValueSerde.serializer());

    queryCommandTopic =
        testDriver.createInputTopic(
            HistorizedLookupsWithVersionedStores.QueryCommandTopic,
            stringSerde.serializer(),
            keyValueSerde.serializer());

    queryResponseTopic =
        testDriver.createOutputTopic(
            HistorizedLookupsWithVersionedStores.QueryResponseTopic,
            stringSerde.deserializer(),
            stringSerde.deserializer());
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testLookupOrdering() {
    pipeToLookupOutOfOrder();

    VersionedKeyValueStore<String, String> store =
        testDriver.getVersionedKeyValueStore(
            HistorizedLookupsWithVersionedStores.VersionedLookupStoreName);

    assertEquals("key1V4", store.get(key1).value());
    assertEquals("key2V4", store.get(key2).value());
  }

  @Test
  public void testExactMatch() {
    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2021, 2, 1)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V2", res.value());

    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2022, 3, 1)));
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key2V3", res2.value());
  }

  @Test
  public void testBetweenMatch() {
    pipeToLookupOutOfOrder();

    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2021, 2, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V2", res.value());

    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2022, 3, 27)));
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key2V3", res2.value());
  }

  @Test
  public void testEarliestMatch() {
    pipeToLookupOutOfOrder();

    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2020, 2, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V1", res.value());

    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2021, 3, 27)));
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key2V1", res2.value());
  }

  @Test
  public void testLatestMatch() {
    pipeToLookupOutOfOrder();

    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2022, 2, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V4", res.value());

    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2023, 3, 27)));
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key2V4", res2.value());
  }

  private void pipeToLookupOutOfOrder() {
    lookupTopic.pipeInput(key1, key1V3);
    lookupTopic.pipeInput(key1, key1V1);
    lookupTopic.pipeInput(key2, key2V2);
    lookupTopic.pipeInput(key2, key2V1);
    lookupTopic.pipeInput(key1, key1V2);
    lookupTopic.pipeInput(key1, key1V4);
    lookupTopic.pipeInput(key2, key2V4);
    lookupTopic.pipeInput(key2, key2V3);
  }

  private KeyValue<String, Long> create(String key, LocalDate date) {
    return new KeyValue<>(key, date.toEpochDay());
  }
}

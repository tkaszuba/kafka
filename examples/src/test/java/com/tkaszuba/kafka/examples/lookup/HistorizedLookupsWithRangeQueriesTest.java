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

import com.tkaszuba.kafka.serdes.ComparableKeyValueSerde;
import com.tkaszuba.kafka.serdes.KeyValueSerde;
import com.tkaszuba.kafka.streams.ComparableKeyValue;
import com.tkaszuba.kafka.streams.examples.lookup.HistorizedLookupsWithRangeQueries;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HistorizedLookupsWithRangeQueriesTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, ComparableKeyValue<String, Long>> inputTopic;
  private TestInputTopic<String, ComparableKeyValue<String, Long>> queryCommandTopic;
  private TestInputTopic<ComparableKeyValue<String, Long>, String> lookupTopic;
  private TestOutputTopic<String, String> queryResponseTopic;

  private final String key1 = "Key1";
  private final String key2 = "Key2";
  private final String commandId = "CommandId";

  private final ComparableKeyValue<String, Long> key1V1 = create(key1, LocalDate.of(2021, 1, 1));
  private final ComparableKeyValue<String, Long> key1V2 = create(key1, LocalDate.of(2021, 2, 1));
  private final ComparableKeyValue<String, Long> key1V3 = create(key1, LocalDate.of(2021, 3, 1));
  private final ComparableKeyValue<String, Long> key1V4 = create(key1, LocalDate.of(2021, 4, 1));
  private final ComparableKeyValue<String, Long> key2V1 = create(key2, LocalDate.of(2022, 1, 1));

  private final ComparableKeyValue<String, Long> key2V2 = create(key2, LocalDate.of(2022, 2, 1));
  private final ComparableKeyValue<String, Long> key2V3 = create(key2, LocalDate.of(2022, 3, 1));
  private final ComparableKeyValue<String, Long> key2V4 = create(key2, LocalDate.of(2022, 4, 1));

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<Long> longSerde = Serdes.Long();

  private final Serde<ComparableKeyValue<String, Long>> keyValueSerde =
      new ComparableKeyValueSerde<>(new KeyValueSerde<>(stringSerde, longSerde));

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    HistorizedLookupsWithRangeQueries.createStream(builder);

    final Properties props = new Properties();
    props.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

    testDriver = new TopologyTestDriver(builder.build(), props);
    inputTopic =
        testDriver.createInputTopic(
            HistorizedLookupsWithRangeQueries.InputTopic,
            stringSerde.serializer(),
            keyValueSerde.serializer());

    lookupTopic =
        testDriver.createInputTopic(
            HistorizedLookupsWithRangeQueries.LookupTopic,
            keyValueSerde.serializer(),
            stringSerde.serializer());

    queryCommandTopic =
        testDriver.createInputTopic(
            HistorizedLookupsWithRangeQueries.QueryCommandTopic,
            stringSerde.serializer(),
            keyValueSerde.serializer());

    queryResponseTopic =
        testDriver.createOutputTopic(
            HistorizedLookupsWithRangeQueries.QueryResponseTopic,
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

    KeyValueStore<ComparableKeyValue<String, Long>, String> store =
        testDriver.getKeyValueStore(HistorizedLookupsWithRangeQueries.VersionedLookupStoreName);

    assertEquals(8, store.approximateNumEntries());

    var list = new ArrayList<ComparableKeyValue<String, Long>>();
    try (KeyValueIterator<ComparableKeyValue<String, Long>, String> iterator = store.all()) {
      while (iterator.hasNext()) {
        list.add(iterator.next().key);
      }
    }

    assertEquals(key1V1, list.get(0));
    assertEquals(key1V2, list.get(1));
    assertEquals(key1V3, list.get(2));
    assertEquals(key1V4, list.get(3));
    assertEquals(key2V1, list.get(4));
    assertEquals(key2V2, list.get(5));
    assertEquals(key2V3, list.get(6));
    assertEquals(key2V4, list.get(7));
  }

  @Test
  public void testExactMatch() {
    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, key2V4);
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key2V4", res.value());

    queryCommandTopic.pipeInput(commandId, key1V1);
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key1V1", res2.value());
  }

  @Test
  public void testBeforeRangeMatch() {
    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2021, 2, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V2", res.value());
  }

  @Test
  public void testAfterRangeMatch() {
    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2020, 2, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key2V1", res.value());
  }

  @Test
  public void testLatestRangeMatch() {
    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, create(key1, LocalDate.of(2021, 4, 27)));
    var res = queryResponseTopic.readRecord();
    assertEquals(commandId, res.key());
    assertEquals("key1V4", res.value());

    pipeToLookupOutOfOrder();
    queryCommandTopic.pipeInput(commandId, create(key2, LocalDate.of(2023, 4, 27)));
    var res2 = queryResponseTopic.readRecord();
    assertEquals(commandId, res2.key());
    assertEquals("key2V4", res2.value());
  }

  private void pipeToLookupOutOfOrder() {
    lookupTopic.pipeInput(key1V3, "key1V3");
    lookupTopic.pipeInput(key1V1, "key1V1");
    lookupTopic.pipeInput(key2V2, "key2V4");
    lookupTopic.pipeInput(key2V1, "key2V1");
    lookupTopic.pipeInput(key1V2, "key1V2");
    lookupTopic.pipeInput(key1V4, "key1V4");
    lookupTopic.pipeInput(key2V4, "key2V4");
    lookupTopic.pipeInput(key2V3, "key2V3");
  }

  private ComparableKeyValue<String, Long> create(String key, LocalDate date) {
    return new ComparableKeyValue<>(key, date.toEpochDay());
  }
}

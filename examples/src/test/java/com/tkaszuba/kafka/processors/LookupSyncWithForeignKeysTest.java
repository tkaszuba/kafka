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
package com.tkaszuba.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.tkaszuba.kafka.streams.examples.lookup.LookupSyncWithForeignKeys;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupSyncWithForeignKeysTest {
  private static final Logger logger = LoggerFactory.getLogger(LookupSyncWithForeignKeysTest.class);

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private final String key = "Key1";
  private final String value1 = "Value1";
  private final String value2 = "Value2";

  private final Serde<String> stringSerde = Serdes.String();

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    LookupSyncWithForeignKeys.createStream(builder);

    final Properties props = new Properties();
    props.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

    testDriver = new TopologyTestDriver(builder.build(), props);
    inputTopic =
        testDriver.createInputTopic(
            LookupSyncWithForeignKeys.MasterData,
            stringSerde.serializer(),
            stringSerde.serializer());
    outputTopic =
        testDriver.createOutputTopic(
            LookupSyncWithForeignKeys.DerivedData,
            stringSerde.deserializer(),
            stringSerde.deserializer());
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testDerivationUpsert() {
    inputTopic.pipeInput(key, value1);

    var insert = outputTopic.readRecord();
    assertEquals(value1, insert.key());
    assertEquals(key, insert.value());

    inputTopic.pipeInput(key, value2);

    var update = outputTopic.readRecord();
    assertEquals(value2, update.key());
    assertEquals(key, update.value());
  }

  @Test
  public void testDerivationDelete() {
    inputTopic.pipeInput(key, value1);
    outputTopic.readRecord();

    inputTopic.pipeInput(key, (String) null);

    var delete = outputTopic.readRecord();
    assertEquals(value1, delete.key());
    assertNull(delete.value());
  }
}

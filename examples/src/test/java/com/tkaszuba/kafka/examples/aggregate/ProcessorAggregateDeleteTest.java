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
package com.tkaszuba.kafka.examples.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tkaszuba.kafka.streams.examples.aggregate.ProcessorAggregateDelete;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProcessorAggregateDeleteTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Integer> inputTopic;
  private TestOutputTopic<String, Integer> outputTopic;

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<Integer> integerSerde = Serdes.Integer();

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    ProcessorAggregateDelete.createStream(builder);

    final Properties props = new Properties();
    props.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, integerSerde.getClass().getName());

    testDriver = new TopologyTestDriver(builder.build(), props);
    inputTopic =
        testDriver.createInputTopic(
            ProcessorAggregateDelete.Incoming, stringSerde.serializer(), integerSerde.serializer());
    outputTopic =
        testDriver.createOutputTopic(
            ProcessorAggregateDelete.Outgoing,
            stringSerde.deserializer(),
            integerSerde.deserializer());
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testDelete() {
    String key = "Key1";

    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);

    inputTopic.pipeInput(key, null);
    inputTopic.pipeInput(key, ProcessorAggregateDelete.DeleteEvent);

    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);
    inputTopic.pipeInput(key, 1);

    assertEquals(1, outputTopic.readRecord().value());
    assertEquals(2, outputTopic.readRecord().value());
    assertEquals(3, outputTopic.readRecord().value());
    assertEquals(4, outputTopic.readRecord().value());

    assertEquals(1, outputTopic.readRecord().value());
    assertEquals(2, outputTopic.readRecord().value());
    assertEquals(3, outputTopic.readRecord().value());
    assertEquals(4, outputTopic.readRecord().value());
  }
}

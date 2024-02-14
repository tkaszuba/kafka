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

import com.tkaszuba.kafka.streams.processors.Suppress;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuppressTest {
  private static final Logger logger = LoggerFactory.getLogger(SuppressTest.class);

  private TimestampedKeyValueStore<String, String> store;
  private MockProcessorContext<String, String> context;
  private Suppress<String, String> processor;
  private Punctuator punctuator;
  private final long punctuationSchedule = 4000;

  @BeforeEach
  public void init() {
    long initialTime = Instant.now().toEpochMilli();
    String storeName = "testStore";
    store =
        Stores.timestampedKeyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.String())
            .withLoggingDisabled()
            .build();

    context = new MockProcessorContext<>();

    store.init(context.getStateStoreContext(), store);
    context.getStateStoreContext().register(store, null);

    long windowSize = 10000;
    processor = new Suppress<>(punctuationSchedule, windowSize, storeName);
    processor.init(context);

    punctuator = context.scheduledPunctuators().get(0).getPunctuator();

    context.setCurrentStreamTimeMs(initialTime);
    logger.info("Initial context timestamp set to {}", initialTime);
  }

  @Test
  public void testInitialization() {
    MockProcessorContext.CapturedPunctuator scheduledPunctuator =
        context.scheduledPunctuators().get(0);
    assertEquals(punctuationSchedule, scheduledPunctuator.getInterval().toMillis());
    assertEquals(PunctuationType.WALL_CLOCK_TIME, scheduledPunctuator.getType());
  }

  @Test
  public void testWindowClose() {
    processor.process(new Record<>("key1", "value1", Instant.now().toEpochMilli()));
    punctuator.punctuate(context.currentStreamTimeMs() + 6000);

    assertEquals(
        0,
        context.forwarded().size(),
        "No messages should be forwarded since window has not closed");
    assertEquals(1, store.approximateNumEntries());

    processor.process(new Record<>("key1", "value2", Instant.now().toEpochMilli()));
    context.setCurrentStreamTimeMs(context.currentStreamTimeMs() + 6000);

    assertEquals(
        0,
        context.forwarded().size(),
        "After adding a new record and running the punctuate but before the window has closed, the context should not forward any messages");
    assertEquals(1, store.approximateNumEntries());

    punctuator.punctuate(context.currentStreamTimeMs() + 5000);

    assertEquals(
        1,
        context.forwarded().size(),
        "After closing the window, the context should forward the last message");
    assertEquals("key1", context.forwarded().get(0).record().key());
    assertEquals("value2", context.forwarded().get(0).record().value());
    assertEquals(0, store.approximateNumEntries());
  }

  @Test
  public void testMultipleWindowsClose() {
    processor.process(new Record<>("key1", "value1", Instant.now().toEpochMilli()));
    context.setCurrentStreamTimeMs(context.currentStreamTimeMs() + 6000);

    assertEquals(
        0,
        context.forwarded().size(),
        "No messages should be forwarded since window has not closed");
    assertEquals(1, store.approximateNumEntries());

    processor.process(new Record<>("key2", "value2", context.currentStreamTimeMs()));
    assertEquals(2, store.approximateNumEntries());

    punctuator.punctuate(context.currentStreamTimeMs() + 5000);

    assertEquals(
        1,
        context.forwarded().size(),
        "After adding a new record with a different key and running the punctuate only the first message should be forwarded");
    assertEquals("key1", context.forwarded().get(0).record().key());
    assertEquals("value1", context.forwarded().get(0).record().value());
    assertEquals(1, store.approximateNumEntries());

    punctuator.punctuate(context.currentStreamTimeMs() + 11000);

    assertEquals(
        2,
        context.forwarded().size(),
        "After the second window is closed, the context should forward the last messag");
    assertEquals("key2", context.forwarded().get(1).record().key());
    assertEquals("value2", context.forwarded().get(1).record().value());
    assertEquals(0, store.approximateNumEntries());
  }
}

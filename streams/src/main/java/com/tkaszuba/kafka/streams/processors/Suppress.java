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
package com.tkaszuba.kafka.streams.processors;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suppress<K, V> extends ContextualProcessor<K, V, K, V> {
  private static final Logger logger = LoggerFactory.getLogger(Suppress.class);
  private final long schedule;
  private final long windowSize;
  private final String storeName;
  private final boolean useMessageTime;

  private TimestampedKeyValueStore<K, V> store;

  public Suppress(long schedule, long windowSize, String storeName) {
    this(schedule, windowSize, true, storeName);
  }

  public Suppress(long schedule, long windowSize, boolean useMessageTime, String storeName) {
    this.schedule = schedule;
    this.windowSize = windowSize;
    this.storeName = storeName;
    this.useMessageTime = useMessageTime;
  }

  @Override
  public void init(ProcessorContext<K, V> context) {
    super.init(context);
    store = context.getStateStore(storeName);
    context.schedule(
        Duration.ofMillis(schedule),
        PunctuationType.WALL_CLOCK_TIME,
        msgTime -> {
          logger.debug("Emit rate of {} ms was triggered", schedule);
          try (KeyValueIterator<K, ValueAndTimestamp<V>> iterator = store.all()) {
            while (iterator.hasNext()) {
              KeyValue<K, ValueAndTimestamp<V>> window = iterator.next();
              if (msgTime > window.value.timestamp()) {
                logger.debug(
                    "Window size {} ms was reached, forwarding items with key '{}'",
                    windowSize,
                    window.key);
                context.forward(new Record<>(window.key, window.value.value(), msgTime));
                store.delete(window.key);
              }
            }
          }
        });
  }

  @Override
  public void process(Record<K, V> record) {
    long timestamp = useMessageTime ? record.timestamp() : Instant.now().toEpochMilli();
    logger.debug("Adding key '{}' with timestamp {}", record.key(), timestamp);
    store.put(record.key(), ValueAndTimestamp.make(record.value(), timestamp + windowSize));
  }
}

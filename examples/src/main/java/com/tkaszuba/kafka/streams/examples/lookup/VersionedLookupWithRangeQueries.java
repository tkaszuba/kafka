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
package com.tkaszuba.kafka.streams.examples.lookup;

import com.tkaszuba.kafka.serdes.ComparableKeyValueSerde;
import com.tkaszuba.kafka.serdes.KeyValueSerde;
import com.tkaszuba.kafka.streams.ComparableKeyValue;
import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedLookupWithRangeQueries {
  private static final Logger logger =
      LoggerFactory.getLogger(VersionedLookupWithRangeQueries.class);

  public static final String NotFound = "VERSION NOT FOUND";

  public static final String InputTopic = "input-topic";
  public static final String LookupTopic = "lookup-topic";
  public static final String QueryCommandTopic = "query-command-topic";
  public static final String QueryResponseTopic = "query-response-topic";

  public static final String VersionedLookupStoreName = "VersionedLookup";

  private static final Serde<String> stringSerde = Serdes.String();

  private static final Serde<Long> longSerde = Serdes.Long();

  private static final Serde<ComparableKeyValue<String, Long>> keyValueSerde =
      new ComparableKeyValueSerde<>(new KeyValueSerde<>(stringSerde, longSerde));

  public static void createStream(final StreamsBuilder builder) {
    KStream<ComparableKeyValue<String, Long>, String> lookup =
        builder.stream(InputTopic, Consumed.with(stringSerde, keyValueSerde))
            .map((k, v) -> new ComparableKeyValue<>(v, k))
            .repartition(
                Repartitioned.<ComparableKeyValue<String, Long>, String>as(LookupTopic)
                    .withKeySerde(keyValueSerde)
                    .withValueSerde(stringSerde));

    GlobalKTable<ComparableKeyValue<String, Long>, String> versionedLookup =
        builder.globalTable(
            LookupTopic,
            Consumed.with(keyValueSerde, stringSerde),
            Materialized.<ComparableKeyValue<String, Long>, String>as(
                    Stores.inMemoryKeyValueStore(VersionedLookupStoreName))
                .withKeySerde(keyValueSerde)
                .withValueSerde(stringSerde));

    builder.stream(QueryCommandTopic, Consumed.with(stringSerde, keyValueSerde))
        .process(QueryCommandProcessor::new)
        .mapValues(
            (k, v) -> {
              if (v.isEmpty()) {
                logger.debug("Couldn't find a version for key {}, returning {}", k, NotFound);
                return NotFound;
              } else return v.get();
            })
        .to(QueryResponseTopic, Produced.with(stringSerde, stringSerde));
  }

  private static class QueryCommandProcessor
      extends ContextualProcessor<
          String, ComparableKeyValue<String, Long>, String, Optional<String>> {

    private TimestampedKeyValueStore<ComparableKeyValue<String, Long>, String> lookupStore;

    @Override
    public void init(ProcessorContext<String, Optional<String>> context) {
      super.init(context);
      this.lookupStore = context.getStateStore(VersionedLookupStoreName);
    }

    @Override
    public void process(Record<String, ComparableKeyValue<String, Long>> record) {
      Optional<String> lookup = beforeValue(record.value()).or(() -> afterValue(record.value()));
      context().forward(new Record<>(record.key(), lookup, context().currentStreamTimeMs()));
    }

    private Optional<String> beforeValue(ComparableKeyValue<String, Long> value) {
      ComparableKeyValue<String, Long> min = new ComparableKeyValue<>(value.key, 0L);
      try (KeyValueIterator<ComparableKeyValue<String, Long>, ValueAndTimestamp<String>> iterator =
          lookupStore.reverseRange(min, value)) {
        return iterator.hasNext()
            ? Optional.ofNullable(iterator.next().value.value())
            : Optional.empty();
      }
    }

    private Optional<String> afterValue(ComparableKeyValue<String, Long> value) {
      ComparableKeyValue<String, Long> max =
          new ComparableKeyValue<>(value.key, Instant.MAX.getEpochSecond());
      try (KeyValueIterator<ComparableKeyValue<String, Long>, ValueAndTimestamp<String>> iterator =
          lookupStore.range(value, max)) {
        return iterator.hasNext()
            ? Optional.ofNullable(iterator.next().value.value())
            : Optional.empty();
      }
    }
  }
}

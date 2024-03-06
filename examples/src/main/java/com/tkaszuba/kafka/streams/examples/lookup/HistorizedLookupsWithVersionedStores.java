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

import com.tkaszuba.kafka.serdes.KeyValueSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistorizedLookupsWithVersionedStores {
  private static final Logger logger =
      LoggerFactory.getLogger(HistorizedLookupsWithVersionedStores.class);

  public static final String NotFound = "VERSION NOT FOUND";

  public static final String LookupTopic = "lookup-topic";
  public static final String QueryCommandTopic = "query-command-topic";
  public static final String QueryResponseTopic = "query-response-topic";

  public static final String VersionedLookupStoreName = "VersionedLookup";

  private static final Serde<String> stringSerde = Serdes.String();

  private static final Serde<Long> longSerde = Serdes.Long();

  private static final Serde<KeyValue<String, Long>> keyValueSerde =
      new KeyValueSerde<>(stringSerde, longSerde);

  public static void createStream(final StreamsBuilder builder) {
    builder.addGlobalStore(
        new VersionedKeyValueStoreBuilder<>(
            Stores.persistentVersionedKeyValueStore(
                VersionedLookupStoreName, Duration.ofDays(5 * 365)),
            stringSerde,
            stringSerde,
            Time.SYSTEM),
        LookupTopic,
        Consumed.with(stringSerde, keyValueSerde),
        VersionedStoreSetupProcessor::new);

    builder.stream(QueryCommandTopic, Consumed.with(stringSerde, keyValueSerde))
        .process(VersionedStoreLookupProcessor::new)
        .mapValues(
            (k, v) -> {
              if (v.isEmpty()) {
                logger.debug("Couldn't find a version for key {}, returning {}", k, NotFound);
                return NotFound;
              } else return v.get();
            })
        .to(QueryResponseTopic, Produced.with(stringSerde, stringSerde));
  }

  private static class VersionedStoreSetupProcessor
      extends ContextualProcessor<String, KeyValue<String, Long>, Void, Void> {

    private VersionedKeyValueStore<String, String> lookupStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
      super.init(context);
      this.lookupStore = context.getStateStore(VersionedLookupStoreName);
    }

    @Override
    public void process(Record<String, KeyValue<String, Long>> record) {
      lookupStore.put(record.key(), record.value().key, record.value().value);
    }
  }

  private static class VersionedStoreLookupProcessor
      extends ContextualProcessor<String, KeyValue<String, Long>, String, Optional<String>> {

    private VersionedKeyValueStore<String, String> lookupStore;

    @Override
    public void init(ProcessorContext<String, Optional<String>> context) {
      super.init(context);
      this.lookupStore = context.getStateStore(VersionedLookupStoreName);
    }

    @Override
    public void process(Record<String, KeyValue<String, Long>> record) {
      Optional<String> lookup =
          Optional.ofNullable(lookupStore.get(record.value().key, record.value().value))
              .or(() -> beforeValue(record.value().key))
              .map(VersionedRecord::value);
      context().forward(new Record<>(record.key(), lookup, context().currentStreamTimeMs()));
    }

    private Optional<VersionedRecord<String>> beforeValue(String key) {
      QueryResult<VersionedRecordIterator<String>> result =
          lookupStore.query(
              MultiVersionedKeyQuery.<String, String>withKey(key)
                  .fromTime(Instant.ofEpochMilli(0L))
                  .withAscendingTimestamps(),
              PositionBound.unbounded(),
              new QueryConfig(false));
      try (VersionedRecordIterator<String> iterator = result.getResult()) {
        return iterator.hasNext() ? Optional.ofNullable(iterator.next()) : Optional.empty();
      }
    }
  }
}

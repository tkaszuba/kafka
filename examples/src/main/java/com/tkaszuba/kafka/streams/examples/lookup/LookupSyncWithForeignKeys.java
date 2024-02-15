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
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LookupSyncWithForeignKeys {
  private static final Logger logger = LoggerFactory.getLogger(LookupSyncWithForeignKeys.class);

  public static final String MasterData = "master-topic";
  public static final String DerivedData = "derived-topic";
  public static final String TombstoneMarker = "Tombstone";
  public static final String CurrentStateStoreName = "CurrentStateDerived";
  public static final String DeletesStateStoreName = "DeletesStateDerived";

  public static final String UpdatesStateStoreName = "UpdatesStateDerived";

  private static final Serde<String> stringSerde = Serdes.String();
  private static final Serde<KeyValue<String, String>> keyValueSerde =
      new KeyValueSerde<>(stringSerde, stringSerde);

  public static void createStream(final StreamsBuilder builder) {

    KTable<String, KeyValue<String, String>> currentState =
        builder.stream(DerivedData, Consumed.with(stringSerde, stringSerde))
            .mapValues((k, v) -> v != null ? new KeyValue<>(k, v) : null)
            .toTable(
                Named.as(CurrentStateStoreName),
                Materialized.<String, KeyValue<String, String>>as(
                        Stores.inMemoryKeyValueStore(CurrentStateStoreName))
                    .withKeySerde(stringSerde)
                    .withValueSerde(keyValueSerde));

    final KStream<String, String> master = builder.stream(MasterData);

    master
        .filter((k, v) -> v != null)
        .map((k, v) -> new KeyValue<>(v, k))
        .peek((k, v) -> logger.debug("Forwarding to derived lookup topic ({}, {})", k, v))
        .to(DerivedData, Produced.with(stringSerde, stringSerde));

    KTable<String, KeyValue<String, String>> deletesState =
        master
            .flatMapValues(
                (k, v) ->
                    (v == null)
                        ? Collections.unmodifiableList(
                            Arrays.asList(new KeyValue<>(k, TombstoneMarker), null))
                        : Collections.unmodifiableList(Arrays.asList(new KeyValue<>(v, k), null)))
            .toTable(
                Named.as(DeletesStateStoreName),
                Materialized.<String, KeyValue<String, String>>as(
                        Stores.inMemoryKeyValueStore(DeletesStateStoreName))
                    .withKeySerde(stringSerde)
                    .withValueSerde(keyValueSerde)
                    .withCachingDisabled()
                    .withLoggingDisabled());

    KTable<String, KeyValue<String, String>> updateState =
        currentState.join(
            deletesState,
            currentStateKeyValue -> currentStateKeyValue.value,
            (currentStateKeyValue, deletesStateKeyValue) -> {
              if (deletesStateKeyValue.equals(currentStateKeyValue)) {
                logger.warn(
                    "Duplicate update '{}' encountered in derived lookup topic. Ignoring.",
                    currentStateKeyValue);
                return null;
              } else {
                logger.debug(
                    "Found key in current derived lookup topic. Replacing '{}' with '{}'",
                    currentStateKeyValue,
                    TombstoneMarker);
                return new KeyValue<>(deletesStateKeyValue.key, TombstoneMarker);
              }
            },
            TableJoined.as(UpdatesStateStoreName),
            Materialized.<String, KeyValue<String, String>>as(
                    Stores.inMemoryKeyValueStore(UpdatesStateStoreName))
                .withKeySerde(stringSerde)
                .withValueSerde(keyValueSerde)
                .withCachingDisabled()
                .withLoggingDisabled());

    updateState
        .toStream()
        .filter((k, v) -> v != null)
        .mapValues(v -> (v.value).equals(TombstoneMarker) ? null : v.value)
        .peek(
            (k, v) -> logger.debug("Forwarding to derived lookup topic a tombstone ({}, {})", k, v))
        .to(DerivedData);
  }
}

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
package com.tkaszuba.kafka.streams.examples.aggregate;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimpleAggregateDelete {
  private static final Logger logger =
      LoggerFactory.getLogger(
          com.tkaszuba.kafka.streams.examples.aggregate.SimpleAggregateDelete.class);

  public static final String Incoming = "input-topic";
  public static final String Outgoing = "output-topic";
  public static final String AggStoreName = "agg-store";
  private static final Serde<String> stringSerde = Serdes.String();
  private static final Serde<Integer> integerSerde = Serdes.Integer();

  public static void createStream(final StreamsBuilder builder) {
    builder.stream(Incoming, Consumed.with(stringSerde, integerSerde))
        .groupByKey()
        .aggregate(
            () -> 0,
            (k, v, agg) -> (v == Integer.MIN_VALUE) ? null : agg + v,
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(AggStoreName)
                .withCachingDisabled())
        .toStream()
        .filter((k, v) -> v != null)
        .to(Outgoing);
  }
}

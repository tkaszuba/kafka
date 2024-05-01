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
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProcessorAggregateDelete {
  private static final Logger logger = LoggerFactory.getLogger(ProcessorAggregateDelete.class);

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
            (k, v, agg) -> (v == Integer.MIN_VALUE) ? v : agg + v,
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(AggStoreName)
                .withCachingDisabled())
        .toStream()
        .processValues(StorePurgeProcessor::new, AggStoreName)
        .to(Outgoing);
  }

  private static class StorePurgeProcessor
      extends ContextualFixedKeyProcessor<String, Integer, Integer> {

    private TimestampedKeyValueStore<String, Integer> dslStore;

    @Override
    public void init(FixedKeyProcessorContext<String, Integer> context) {
      super.init(context);
      this.dslStore = context.getStateStore(AggStoreName);
    }

    @Override
    public void process(FixedKeyRecord<String, Integer> record) {
      if (record.value() == Integer.MIN_VALUE) dslStore.delete(record.key());
      else context().forward(record);
    }
  }
}

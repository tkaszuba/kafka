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

package com.tkaszuba.kafka.streams;

import org.apache.kafka.streams.KeyValue;

public class ComparableKeyValue<K extends Comparable, V extends Comparable> extends KeyValue<K, V>
    implements Comparable<KeyValue<K, V>> {

  /**
   * Create a new comparable key-value pair.
   *
   * @param key the key
   * @param value the value
   */
  public ComparableKeyValue(final K key, final V value) {
    super(key, value);
  }

  public ComparableKeyValue(final KeyValue<K, V> keyValue) {
    this(keyValue.key, keyValue.value);
  }

  @Override
  public int compareTo(KeyValue<K, V> other) {
    int keyCompare = key.compareTo(other.key);
    return (keyCompare == 0) ? value.compareTo(other.value) : keyCompare;
  }
}

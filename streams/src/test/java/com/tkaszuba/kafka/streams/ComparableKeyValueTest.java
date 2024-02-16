package com.tkaszuba.kafka.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ComparableKeyValueTest {

  @Test
  public void compareTest() {
    assertEquals(
        0, new ComparableKeyValue<>("test", 1).compareTo(ComparableKeyValue.pair("test", 1)));
    assertEquals(
        1, new ComparableKeyValue<>("test1", 1).compareTo(ComparableKeyValue.pair("test", 1)));
    assertEquals(
        -1, new ComparableKeyValue<>("test", 1).compareTo(ComparableKeyValue.pair("test1", 1)));
    assertEquals(
        -1, new ComparableKeyValue<>("test", 0).compareTo(ComparableKeyValue.pair("test", 1)));
    assertEquals(
        1, new ComparableKeyValue<>("test", 1).compareTo(ComparableKeyValue.pair("test", 0)));
  }
}

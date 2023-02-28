package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;


public class PubSubMessageHeaderTest {
  @Test(expectedExceptions = NullPointerException.class)
  public void testPubsubMessageHeaderThrowsNPEIfKeyIsNull() {
    new PubSubMessageHeader(null, null);
  }

  @Test
  public void testPubsubMessageHeaderTestEquals() {
    PubSubMessageHeader header1 = new PubSubMessageHeader("key-0", "headers-value".getBytes());
    PubSubMessageHeader header2 = new PubSubMessageHeader("key-0", "headers-value".getBytes());
    PubSubMessageHeader header3 = new PubSubMessageHeader("key-1", "headers-value".getBytes());

    // reflexivity
    assertEquals(header1, header1);
    // symmetry
    assertEquals(header2, header1);
    assertEquals(header1, header2);

    assertNotEquals(header3, header1);
    assertNotEquals(header2, header3);

    assertNotEquals(header1, null);
    assertNotEquals(header3, null);

    assertNotEquals(new Object(), header1); // should return false if other ojb is not of the same type
  }

  @Test
  public void testPubsubMessageHeaderTestHashcode() {
    PubSubMessageHeader header1 = new PubSubMessageHeader("key-0", "headers-value".getBytes());
    PubSubMessageHeader header2 = new PubSubMessageHeader("key-0", "headers-value".getBytes());
    PubSubMessageHeader header3 = new PubSubMessageHeader("key-1", "headers-value".getBytes());
    assertEquals(header2, header1);
    assertEquals(header1, header2);
    assertEquals(header1.hashCode(), header2.hashCode());

    assertNotEquals(header3, header1);
    assertNotEquals(header1.hashCode(), header3.hashCode());
  }

}

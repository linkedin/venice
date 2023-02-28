package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.testng.annotations.Test;


public class PubSubMessageHeadersTest {
  @Test
  public void testPubsubMessageHeaders() {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add("key-0", "val-0".getBytes());
    headers.add(new PubSubMessageHeader("key-1", "val-1".getBytes()));
    headers.add("key-2", "val-2".getBytes());
    headers.add(new PubSubMessageHeader("key-3", "val-3".getBytes()));
    headers.add(new PubSubMessageHeader("key-0", "val-0-prime".getBytes())); // should keep only the most recent val
    headers.remove("key-2");

    List<PubSubMessageHeader> headerList = headers.toList();
    assertNotNull(headerList);
    assertEquals(headerList.size(), 3);
    assertTrue(headerList.contains(new PubSubMessageHeader("key-1", "val-1".getBytes())));
    assertTrue(headerList.contains(new PubSubMessageHeader("key-0", "val-0-prime".getBytes())));
    assertTrue(headerList.contains(new PubSubMessageHeader("key-3", "val-3".getBytes())));
  }
}

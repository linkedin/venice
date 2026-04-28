package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
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

  @Test
  public void testStripProtocolSchemaHeaderReturnsInputWhenVtpAbsent() {
    // Absent-case must be allocation-free: return the same instance.
    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });
    assertSame(PubSubMessageHeaders.stripProtocolSchemaHeader(headers), headers);
  }

  @Test
  public void testStripProtocolSchemaHeaderHandlesNullInput() {
    assertNull(PubSubMessageHeaders.stripProtocolSchemaHeader(null));
  }

  @Test
  public void testStripProtocolSchemaHeaderRemovesVtpAndPreservesOthers() {
    PubSubMessageHeaders input = new PubSubMessageHeaders().add(VENICE_TRANSPORT_PROTOCOL_HEADER, "schema".getBytes())
        .add(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 })
        .add("custom", "payload".getBytes());

    PubSubMessageHeaders out = PubSubMessageHeaders.stripProtocolSchemaHeader(input);

    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be stripped");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
    assertEquals(out.get("custom").value(), "payload".getBytes(), "unknown headers must be preserved");
    // Caller's input must not be mutated.
    assertNotNull(input.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "input headers must not be mutated");
  }
}

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
  public void testStripProtocolSchemaHeaderMutatesInPlaceForMutableInput() {
    // Production hot path: mutable headers + vtp present. Helper removes vtp in place
    // (no allocation) and returns the same instance.
    PubSubMessageHeaders input = new PubSubMessageHeaders().add(VENICE_TRANSPORT_PROTOCOL_HEADER, "schema".getBytes())
        .add(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 })
        .add("custom", "payload".getBytes());

    PubSubMessageHeaders out = PubSubMessageHeaders.stripProtocolSchemaHeader(input);

    assertSame(out, input, "mutable input should be modified in place, not copied");
    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be stripped");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
    assertEquals(out.get("custom").value(), "payload".getBytes(), "unknown headers must be preserved");
  }

  @Test
  public void testStripProtocolSchemaHeaderFallsBackToCopyForImmutableInput() {
    // Immutable wrapper whose remove() throws: helper must catch and return a fresh copy
    // without vtp. Input is left untouched.
    PubSubMessageHeader vtp = new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, "schema".getBytes());
    PubSubMessageHeader lcs = new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });
    PubSubMessageHeaders input = new PubSubMessageHeaders() {
      private final PubSubMessageHeaders delegate = new PubSubMessageHeaders().add(vtp).add(lcs);

      @Override
      public PubSubMessageHeader get(String k) {
        return delegate.get(k);
      }

      @Override
      public java.util.Iterator<PubSubMessageHeader> iterator() {
        return delegate.iterator();
      }

      @Override
      public PubSubMessageHeaders remove(String k) {
        throw new UnsupportedOperationException("immutable");
      }
    };

    PubSubMessageHeaders out = PubSubMessageHeaders.stripProtocolSchemaHeader(input);

    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be absent in output");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
    assertNotNull(input.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "immutable input must not be mutated");
  }

  @Test
  public void testStripProtocolSchemaHeaderCatchesAnyRuntimeExceptionFromRemove() {
    // The Javadoc promises "Never throws" because the strip runs on the ingestion hot path.
    // A subclass whose remove() throws something other than UnsupportedOperationException
    // (e.g. IllegalStateException) must still be handled by the copy fallback.
    PubSubMessageHeader vtp = new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, "schema".getBytes());
    PubSubMessageHeader lcs = new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });
    PubSubMessageHeaders input = new PubSubMessageHeaders() {
      private final PubSubMessageHeaders delegate = new PubSubMessageHeaders().add(vtp).add(lcs);

      @Override
      public PubSubMessageHeader get(String k) {
        return delegate.get(k);
      }

      @Override
      public java.util.Iterator<PubSubMessageHeader> iterator() {
        return delegate.iterator();
      }

      @Override
      public PubSubMessageHeaders remove(String k) {
        throw new IllegalStateException("simulated invariant violation");
      }
    };

    PubSubMessageHeaders out = PubSubMessageHeaders.stripProtocolSchemaHeader(input);

    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be absent after strip");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
  }

  @Test
  public void testStripProtocolSchemaHeaderCopyAlwaysReturnsFreshInstanceWhenVtpPresent() {
    PubSubMessageHeaders input = new PubSubMessageHeaders().add(VENICE_TRANSPORT_PROTOCOL_HEADER, "schema".getBytes())
        .add(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });

    PubSubMessageHeaders out = PubSubMessageHeaders.stripProtocolSchemaHeaderCopy(input);

    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be absent in output");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
    assertNotNull(input.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "shared input must not be mutated");
  }

  @Test
  public void testStripProtocolSchemaHeaderCopyReturnsInputWhenVtpAbsent() {
    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });
    assertSame(PubSubMessageHeaders.stripProtocolSchemaHeaderCopy(headers), headers);
  }

  @Test
  public void testStripProtocolSchemaHeaderCopyHandlesNullInput() {
    assertNull(PubSubMessageHeaders.stripProtocolSchemaHeaderCopy(null));
  }
}

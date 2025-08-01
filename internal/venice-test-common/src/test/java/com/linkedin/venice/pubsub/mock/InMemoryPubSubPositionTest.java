package com.linkedin.venice.pubsub.mock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class InMemoryPubSubPositionTest {
  @Test
  public void testCreationAndAccessors() {
    InMemoryPubSubPosition pos = InMemoryPubSubPosition.of(123L);
    assertEquals(pos.getInternalOffset(), 123L);
    assertEquals(pos.getNumericOffset(), 123L);

    InMemoryPubSubPosition same = InMemoryPubSubPosition.of(123L);
    assertEquals(pos, same);
    assertEquals(pos.hashCode(), same.hashCode());
    assertEquals(pos.toString(), "InMemoryPubSubPosition{123}");
  }

  @Test
  public void testNextPreviousAndDelta() {
    InMemoryPubSubPosition base = InMemoryPubSubPosition.of(10L);
    assertEquals(base.getNextPosition(), InMemoryPubSubPosition.of(11L));
    assertEquals(base.getPreviousPosition(), InMemoryPubSubPosition.of(9L));
    assertEquals(base.getPositionAfterNRecords(5L), InMemoryPubSubPosition.of(15L));
    assertEquals(base.getPositionAfterNRecords(0L), base);

    InMemoryPubSubPosition zero = InMemoryPubSubPosition.of(0L);
    assertEquals(zero.getPreviousPosition(), InMemoryPubSubPosition.of(-1L));

    InMemoryPubSubPosition minusOne = InMemoryPubSubPosition.of(-1L);
    assertEquals(minusOne.getPreviousPosition(), InMemoryPubSubPosition.of(-2L));

    // Validate exception for invalid previous position
    InMemoryPubSubPosition invalid = InMemoryPubSubPosition.of(-2L);
    assertThrows(IllegalStateException.class, invalid::getPreviousPosition);
  }

  @Test
  public void testWireFormatSerializationAndDeserialization() {
    long offset = 999L;
    InMemoryPubSubPosition original = InMemoryPubSubPosition.of(offset);
    PubSubPositionWireFormat wireFormat = original.getPositionWireFormat();

    PubSubPositionTypeRegistry registry = InMemoryPubSubPositionFactory.getPositionTypeRegistryWithInMemoryPosition();
    PubSubPositionDeserializer deserializer = new PubSubPositionDeserializer(registry);

    // test repeated deserialization
    for (int i = 0; i < 5; i++) {
      PubSubPosition deserialized = deserializer.toPosition(wireFormat);
      assertTrue(deserialized instanceof InMemoryPubSubPosition);
      assertEquals(deserialized, original);
      assertEquals(((InMemoryPubSubPosition) deserialized).getInternalOffset(), offset);
    }
  }

  @Test
  public void testOfFromByteBuffer() {
    long offset = 888L;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(offset);
    buffer.flip();
    InMemoryPubSubPosition position = InMemoryPubSubPosition.of(buffer);
    assertEquals(position.getInternalOffset(), offset);

    assertThrows(IllegalArgumentException.class, () -> InMemoryPubSubPosition.of((ByteBuffer) null));
    assertThrows(IllegalArgumentException.class, () -> InMemoryPubSubPosition.of(ByteBuffer.allocate(2)));
  }
}

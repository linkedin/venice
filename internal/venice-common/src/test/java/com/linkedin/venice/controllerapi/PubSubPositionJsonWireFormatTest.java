package com.linkedin.venice.controllerapi;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.protocols.controller.PubSubPositionGrpcWireFormat;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class PubSubPositionJsonWireFormatTest {
  PubSubPosition position = InMemoryPubSubPosition.of(123L);
  ByteBuffer positionBytes = position.getPositionWireFormat().getRawBytes();
  String base64PositionBytes = position.toJsonWireFormat().getBase64PositionBytes();

  @Test
  void testConstructorAndGetters() {
    PubSubPositionJsonWireFormat format =
        new PubSubPositionJsonWireFormat(InMemoryPubSubPosition.INMEMORY_PUBSUB_POSITION_TYPE_ID, base64PositionBytes);
    assertEquals(format.getTypeId().intValue(), InMemoryPubSubPosition.INMEMORY_PUBSUB_POSITION_TYPE_ID);
    assertEquals(format.getBase64PositionBytes(), base64PositionBytes);
  }

  @Test
  void testSetters() {
    PubSubPositionJsonWireFormat format = new PubSubPositionJsonWireFormat();
    format.setTypeId(2);
    format.setBase64PositionBytes(base64PositionBytes);
    assertEquals(format.getTypeId().intValue(), 2);
    assertEquals(format.getBase64PositionBytes(), base64PositionBytes);
  }

  @Test
  void testFromWireFormatByteBuffer() {
    PubSubPositionWireFormat wireFormat = mock(PubSubPositionWireFormat.class);
    when(wireFormat.getType()).thenReturn(3);
    when(wireFormat.getRawBytes()).thenReturn(positionBytes);
    PubSubPositionJsonWireFormat format = PubSubPositionJsonWireFormat.fromWireFormatByteBuffer(wireFormat);
    assertEquals(3, format.getTypeId().intValue());
    assertEquals(format.getBase64PositionBytes(), base64PositionBytes);
  }

  @Test
  void testFromGrpcWireFormat() {
    PubSubPositionGrpcWireFormat actualGrpcWireFormat = PubSubUtil.getPubSubPositionGrpcWireFormat(position);
    PubSubPositionGrpcWireFormat expectedGrpcWireFormat = PubSubPositionGrpcWireFormat.newBuilder()
        .setTypeId(InMemoryPubSubPosition.INMEMORY_PUBSUB_POSITION_TYPE_ID)
        .setBase64PositionBytes(base64PositionBytes)
        .build();
    assertEquals(actualGrpcWireFormat, expectedGrpcWireFormat);
  }

  @Test
  void testEqualsAndHashCode() {
    PubSubPositionJsonWireFormat format1 = new PubSubPositionJsonWireFormat(1, "abc");
    PubSubPositionJsonWireFormat format2 = new PubSubPositionJsonWireFormat(1, "abc");
    PubSubPositionJsonWireFormat format3 = new PubSubPositionJsonWireFormat(2, "def");
    assertEquals(format1, format2);
    assertNotEquals(format1, format3);
    assertEquals(format1.hashCode(), format2.hashCode());
  }

  @Test
  void testToString() {
    PubSubPositionJsonWireFormat format = new PubSubPositionJsonWireFormat(1, "abc");
    String str = format.toString();
    assertTrue(str.contains("typeId=1"));
    assertTrue(str.contains("positionBytes=abc"));
  }
}

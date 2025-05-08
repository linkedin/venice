package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class EarliestPositionFactoryTest {
  private static final int TYPE_ID = -2;

  @Test
  public void testCreateFromWireFormatReturnsSingleton() {
    EarliestPositionFactory factory = new EarliestPositionFactory(TYPE_ID);

    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(TYPE_ID);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[0]));

    PubSubPosition result = factory.createFromWireFormat(wireFormat);

    assertNotNull(result);
    assertSame(result, PubSubSymbolicPosition.EARLIEST);
  }

  @Test
  public void testGetPubSubPositionClassName() {
    EarliestPositionFactory factory = new EarliestPositionFactory(TYPE_ID);
    assertEquals(factory.getPubSubPositionClassName(), PubSubSymbolicPosition.EARLIEST.getClass().getName());
  }

  @Test
  public void testCreateFromWireFormatThrowsOnTypeMismatch() {
    EarliestPositionFactory factory = new EarliestPositionFactory(TYPE_ID);

    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(456); // wrong type
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[0]));

    VeniceException ex = expectThrows(VeniceException.class, () -> factory.createFromWireFormat(wireFormat));
    assertTrue(ex.getMessage().contains("Position type ID mismatch"));
  }
}

package com.linkedin.venice.kafka;

import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.message.VeniceMessageSerializer;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.testng.annotations.Test;

/**
 * Tests for the Kafka Serialization class
 * 1. Verify magic byte, schema version, operation type and payload are serialized/de-serialized correctly.
 * 2. Repeat for a PUT message and a DELETE message
 */
public class TestKafkaSerializer {

  @Test
  public void testSerialization() {

    VeniceMessageSerializer serializer = new VeniceMessageSerializer(new VerifiableProperties());

    /* TEST 1 */
    VeniceMessage vm = new VeniceMessage(OperationType.PUT, "p1");
    byte[] byteArray = serializer.toBytes(vm);
    VeniceMessage vm2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(vm2.getMagicByte(), 13);

    // Placeholder Schema Version is 17
    Assert.assertEquals(vm2.getSchemaVersion(), 17);

    Assert.assertEquals(vm2.getOperationType(), OperationType.PUT);
    Assert.assertEquals("p1", vm2.getPayload());

    /* TEST 2 */
    vm = new VeniceMessage(OperationType.DELETE, "d1");
    byteArray = serializer.toBytes(vm);
    vm2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(vm2.getMagicByte(), 13);

    // Placeholder Schema Version is 17
    Assert.assertEquals(vm2.getSchemaVersion(), 17);

    Assert.assertEquals(vm2.getOperationType(), OperationType.DELETE);
    Assert.assertEquals("d1", vm2.getPayload());

  }

}

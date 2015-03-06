package com.linkedin.venice.kafka;

import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.serialization.VeniceMessageSerializer;
import com.linkedin.venice.utils.ByteUtils;
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

    byte[] val1 = "p1".getBytes();

    /* TEST 1 */
    VeniceMessage vm = new VeniceMessage(OperationType.PUT, val1);
    byte[] byteArray = serializer.toBytes(vm);
    VeniceMessage vm2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(vm2.getMagicByte(), VeniceMessage.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(vm2.getSchemaVersionId(), VeniceMessage.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(vm2.getOperationType(), OperationType.PUT);
    Assert.assertTrue(ByteUtils.compare(val1, vm2.getPayload()) == 0);

    /* TEST 2 */
    byte[] val2 = "d1".getBytes();
    vm = new VeniceMessage(OperationType.DELETE, val2);
    byteArray = serializer.toBytes(vm);
    vm2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(vm2.getMagicByte(), VeniceMessage.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(vm2.getSchemaVersionId(), VeniceMessage.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(vm2.getOperationType(), OperationType.DELETE);
    Assert.assertTrue(ByteUtils.compare(val2, vm2.getPayload()) == 0);
  }
}

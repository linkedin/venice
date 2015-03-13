package com.linkedin.venice.kafka;

import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.serialization.KafkaValueSerializer;
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
    KafkaValueSerializer serializer = new KafkaValueSerializer(new VerifiableProperties());

    byte[] val1 = "p1".getBytes();

    /* TEST 1 */
    KafkaValue kafkaValue = new KafkaValue(OperationType.PUT, val1);
    byte[] byteArray = serializer.toBytes(kafkaValue);
    KafkaValue kafkaValue2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(kafkaValue2.getMagicByte(), KafkaValue.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(kafkaValue2.getSchemaVersionId(), KafkaValue.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(kafkaValue2.getOperationType(), OperationType.PUT);
    Assert.assertTrue(ByteUtils.compare(val1, kafkaValue2.getValue()) == 0);

    /* TEST 2 */
    byte[] val2 = "d1".getBytes();
    kafkaValue = new KafkaValue(OperationType.DELETE, val2);
    byteArray = serializer.toBytes(kafkaValue);
    kafkaValue2 = serializer.fromBytes(byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(kafkaValue2.getMagicByte(), KafkaValue.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(kafkaValue2.getSchemaVersionId(), KafkaValue.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(kafkaValue2.getOperationType(), OperationType.DELETE);
    Assert.assertTrue(ByteUtils.compare(val2, kafkaValue2.getValue()) == 0);
  }
}

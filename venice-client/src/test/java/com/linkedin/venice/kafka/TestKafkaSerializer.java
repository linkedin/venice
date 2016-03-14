package com.linkedin.venice.kafka;

import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.utils.ByteUtils;
import junit.framework.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Kafka Key and Value Serialization classes
 * 1. Verify magic byte, schema version, operation type and payload are serialized/de-serialized correctly.
 * 2. Repeat for a PUT message and a DELETE message
 */
public class TestKafkaSerializer {

  private static final String TEST_TOPIC = "TEST_TOPIC";

  @Test
  public void testKafkaKeySerializer() {
    KafkaKeySerializer serializer = new KafkaKeySerializer();

    byte[] key1 = "p1".getBytes();



    /* TEST 1 */
    KafkaKey kafkaKey = new KafkaKey(OperationType.WRITE, key1);
    byte[] byteArray = serializer.serialize(TEST_TOPIC, kafkaKey);
    KafkaKey kafkaKey2 = serializer.deserialize(TEST_TOPIC, byteArray);

    // Placeholder Magic Byte is 22
    Assert.assertEquals(kafkaKey2.getOperationType(), kafkaKey.getOperationType());

    // Placeholder Bytes
    Assert.assertTrue(ByteUtils.compare(key1, kafkaKey2.getKey()) == 0);

    /* TEST 2 */
    byte[] key2 = "d1".getBytes();
    kafkaKey = new KafkaKey(OperationType.WRITE, key2);
    byteArray = serializer.serialize(TEST_TOPIC, kafkaKey);
    kafkaKey2 = serializer.deserialize(TEST_TOPIC, byteArray);

    // Placeholder Magic Byte is 22
    Assert.assertEquals(kafkaKey2.getOperationType(), kafkaKey.getOperationType());

    // Placeholder Bytes
    Assert.assertTrue(ByteUtils.compare(key2, kafkaKey2.getKey()) == 0);

    /* TEST 3 */
    kafkaKey = new ControlFlagKafkaKey(OperationType.BEGIN_OF_PUSH, key1, 2L);
    byteArray = serializer.serialize(TEST_TOPIC, kafkaKey);
    kafkaKey2 = serializer.deserialize(TEST_TOPIC, byteArray);

    // Placeholder Magic Byte is 22
    Assert.assertEquals(kafkaKey2.getOperationType(), kafkaKey.getOperationType());

    // Placeholder Bytes
    Assert.assertTrue(ByteUtils.compare(key1, kafkaKey2.getKey()) == 0);
  }

  @Test
  public void testValueSerializer() {
    KafkaValueSerializer serializer = new KafkaValueSerializer();

    byte[] val1 = "p1".getBytes();

    /* TEST 1 */
    KafkaValue kafkaValue = new KafkaValue(OperationType.PUT, val1);
    byte[] byteArray = serializer.serialize(TEST_TOPIC, kafkaValue);
    KafkaValue kafkaValue2 = serializer.deserialize(TEST_TOPIC, byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(kafkaValue2.getMagicByte(), KafkaValue.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(kafkaValue2.getSchemaVersionId(), KafkaValue.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(kafkaValue2.getOperationType(), OperationType.PUT);
    Assert.assertTrue(ByteUtils.compare(val1, kafkaValue2.getValue()) == 0);

    /* TEST 2 */
    byte[] val2 = "d1".getBytes();
    kafkaValue = new KafkaValue(OperationType.DELETE, val2);
    byteArray = serializer.serialize(TEST_TOPIC, kafkaValue);
    kafkaValue2 = serializer.deserialize(TEST_TOPIC, byteArray);

    // Placeholder Magic Byte is 13
    Assert.assertEquals(kafkaValue2.getMagicByte(), KafkaValue.DEFAULT_MAGIC_BYTE);

    // Placeholder Schema Version is 17
    Assert.assertEquals(kafkaValue2.getSchemaVersionId(), KafkaValue.DEFAULT_SCHEMA_ID);

    Assert.assertEquals(kafkaValue2.getOperationType(), OperationType.DELETE);
    Assert.assertTrue(ByteUtils.compare(val2, kafkaValue2.getValue()) == 0);
  }
}

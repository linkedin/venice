package com.linkedin.venice.offsets;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;

import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInMemoryOffsetManager {
  @Test
  public void canSaveOffsets() {
    String topic = Utils.getUniqueString("topic");

    OffsetManager om = new InMemoryOffsetManager();
    OffsetRecord record = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    record.checkpointLocalVtPosition(ApacheKafkaOffsetPosition.of(1234));
    OffsetRecord oldRecord = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    oldRecord.checkpointLocalVtPosition(ApacheKafkaOffsetPosition.of(234));
    OffsetRecord newRecord = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    newRecord.checkpointLocalVtPosition(ApacheKafkaOffsetPosition.of(11234));

    om.put(topic, 0, record);
    Assert.assertEquals(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING), record);

    om.put(topic, 0, oldRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING), record);

    om.put(topic, 0, newRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING), newRecord);

    om.clearOffset(topic, 1);
    Assert.assertEquals(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING), newRecord);

    om.clearOffset(topic, 0);
    Assert.assertNotEquals(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING), newRecord);
    Assert.assertNotNull(om.getLastOffset(topic, 0, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING));

  }
}

package com.linkedin.venice.offsets;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInMemoryOffsetManager {
  @Test
  public void canSaveOffsets() {
    String topic = Utils.getUniqueString("topic");

    OffsetManager om = new InMemoryOffsetManager();
    OffsetRecord record = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    record.setCheckpointLocalVersionTopicOffset(1234);
    OffsetRecord oldRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    oldRecord.setCheckpointLocalVersionTopicOffset(234);
    OffsetRecord newRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    newRecord.setCheckpointLocalVersionTopicOffset(11234);

    om.put(topic, 0, record);
    Assert.assertEquals(om.getLastOffset(topic, 0), record);

    om.put(topic, 0, oldRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0), record);

    om.put(topic, 0, newRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0), newRecord);

    om.clearOffset(topic, 1);
    Assert.assertEquals(om.getLastOffset(topic, 0), newRecord);

    om.clearOffset(topic, 0);
    Assert.assertNotEquals(om.getLastOffset(topic, 0), newRecord);
    Assert.assertNotNull(om.getLastOffset(topic, 0));

  }
}

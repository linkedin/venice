package com.linkedin.venice.offsets;

import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInMemoryOffsetManager {
  @Test
  public void canSaveOffsets() {
    PubSubPosition position1 = ApacheKafkaOffsetPosition.of(1234L);
    PubSubPosition position2 = ApacheKafkaOffsetPosition.of(234L);
    PubSubPosition position3 = ApacheKafkaOffsetPosition.of(11234L);

    String topic = Utils.getUniqueString("topic");

    OffsetManager om = new InMemoryOffsetManager();
    OffsetRecord record = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    record.setCheckpointLocalVersionTopicOffset(position1.getNumericOffset(), position1.getWireFormatBytes());
    OffsetRecord oldRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    oldRecord.setCheckpointLocalVersionTopicOffset(position2.getNumericOffset(), position2.getWireFormatBytes());
    OffsetRecord newRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    newRecord.setCheckpointLocalVersionTopicOffset(position3.getNumericOffset(), position3.getWireFormatBytes());

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

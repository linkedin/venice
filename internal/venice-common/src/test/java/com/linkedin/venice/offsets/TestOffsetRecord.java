package com.linkedin.venice.offsets;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestOffsetRecord {
  @Test
  public void testToBytes() {
    OffsetRecord offsetRecord1 = TestUtils.getOffsetRecord(100);
    OffsetRecord offsetRecord2 =
        new OffsetRecord(offsetRecord1.toBytes(), AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    Assert.assertTrue(offsetRecord2.getProducerPartitionStateMap() instanceof VeniceConcurrentHashMap);
    Assert.assertEquals(offsetRecord2, offsetRecord1);

    offsetRecord1 = TestUtils.getOffsetRecord(100);
    offsetRecord1.endOfPushReceived(100);
    offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes(), AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    Assert.assertEquals(offsetRecord2, offsetRecord1);
  }

}

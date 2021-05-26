package com.linkedin.venice.offsets;

import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

public class TestOffsetRecord {
  @Test
  public void testToBytes() {
    OffsetRecord offsetRecord1 = TestUtils.getOffsetRecord(100);
    OffsetRecord offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes(), AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    Assert.assertEquals(offsetRecord2, offsetRecord1);

    offsetRecord1 = TestUtils.getOffsetRecord(100);
    offsetRecord1.endOfPushReceived(100);
    offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes(), AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    Assert.assertEquals(offsetRecord2, offsetRecord1);
  }

}

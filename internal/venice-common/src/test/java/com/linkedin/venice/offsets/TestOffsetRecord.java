package com.linkedin.venice.offsets;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestOffsetRecord {
  private static final String TEST_KAFKA_URL1 = "test-1";
  private static final String TEST_KAFKA_URL2 = "test-2";

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

  @Test
  public void testResetUpstreamOffsetMap() {
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(100);
    offsetRecord.setLeaderUpstreamOffset(TEST_KAFKA_URL1, 1L);

    Assert.assertEquals(offsetRecord.getUpstreamOffset(TEST_KAFKA_URL1), 1L);

    Map<String, Long> testMap = new HashMap<>();
    testMap.put(TEST_KAFKA_URL2, 2L);
    offsetRecord.resetUpstreamOffsetMap(testMap);
    // no upstream found for it so fall back to use the leaderOffset which is 1
    Assert.assertEquals(offsetRecord.getUpstreamOffset(TEST_KAFKA_URL1), 1L);
    Assert.assertEquals(offsetRecord.getUpstreamOffset(TEST_KAFKA_URL2), 2L);
  }
}

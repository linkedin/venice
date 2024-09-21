package com.linkedin.venice.offsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestOffsetRecord {
  private static final String TEST_KAFKA_URL1 = "test-1";
  private static final String TEST_KAFKA_URL2 = "test-2";
  private OffsetRecord offsetRecord;
  private GUID guid;
  private ProducerPartitionState state;
  private String kafkaUrl;

  TestOffsetRecord() {
    offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    guid = new GUID();
    state = new ProducerPartitionState();
    kafkaUrl = "test_kafka_url";
  }

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

  @Test
  public void testBatchUpdateEOIP() {
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(100);
    offsetRecord.setPendingReportIncPushVersionList(Arrays.asList("a", "b", "c"));
    Assert.assertEquals(offsetRecord.getPendingReportIncPushVersionList(), Arrays.asList("a", "b", "c"));
  }

  @Test
  public void testSetRealtimeTopicProducerState() {
    // Call the method
    offsetRecord.setRealtimeTopicProducerState(kafkaUrl, guid, state);

    // Verify that the state was set correctly
    ProducerPartitionState result = offsetRecord.getRealTimeProducerState(kafkaUrl, guid);
    assertEquals(result, state, "The state should match the expected value");
  }

  @Test
  public void testRemoveRealTimeTopicProducerState() {
    // Set up the state
    offsetRecord.setRealtimeTopicProducerState(kafkaUrl, guid, state);

    // Call the method to remove the state
    offsetRecord.removeRealTimeTopicProducerState(kafkaUrl, guid);

    // Verify that the state was removed correctly
    ProducerPartitionState result = offsetRecord.getRealTimeProducerState(kafkaUrl, guid);
    assertNull(result, "The state should be null after removal");
  }
}

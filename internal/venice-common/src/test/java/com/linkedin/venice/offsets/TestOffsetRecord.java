package com.linkedin.venice.offsets;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestOffsetRecord {
  private static final String TEST_KAFKA_URL1 = "test-1";
  private static final String TEST_KAFKA_URL2 = "test-2";
  private OffsetRecord offsetRecord;
  private GUID guid;
  private ProducerPartitionState state;
  private String kafkaUrl;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    offsetRecord = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    guid = new GUID();
    state = new ProducerPartitionState();
    kafkaUrl = "test_kafka_url";
  }

  @Test
  public void testToBytes() {
    OffsetRecord offsetRecord1 = TestUtils
        .getOffsetRecord(ApacheKafkaOffsetPosition.of(100), Optional.empty(), DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    OffsetRecord offsetRecord2 = new OffsetRecord(
        offsetRecord1.toBytes(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    Assert.assertTrue(offsetRecord2.getProducerPartitionStateMap() instanceof VeniceConcurrentHashMap);
    Assert.assertEquals(offsetRecord2, offsetRecord1);

    offsetRecord1 = TestUtils
        .getOffsetRecord(ApacheKafkaOffsetPosition.of(100), Optional.empty(), DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord1.endOfPushReceived();
    offsetRecord2 = new OffsetRecord(
        offsetRecord1.toBytes(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    Assert.assertEquals(offsetRecord2, offsetRecord1);
  }

  @Test
  public void testResetUpstreamOffsetMap() {
    OffsetRecord offsetRecord = TestUtils
        .getOffsetRecord(ApacheKafkaOffsetPosition.of(100), Optional.empty(), DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    PubSubPosition p1 = ApacheKafkaOffsetPosition.of(1L);
    PubSubPosition p2 = ApacheKafkaOffsetPosition.of(2L);
    offsetRecord.checkpointRtPosition(TEST_KAFKA_URL1, p1);

    Assert.assertEquals(offsetRecord.getCheckpointedRtPosition(TEST_KAFKA_URL1), p1);

    Map<String, PubSubPosition> testMap = new HashMap<>();
    testMap.put(TEST_KAFKA_URL2, p2);
    offsetRecord.checkpointRtPositions(testMap);
    // no upstream found for it so fall back to use the leaderOffset which is 1
    Assert.assertEquals(offsetRecord.getCheckpointedRtPosition(TEST_KAFKA_URL1), p1);
    Assert.assertEquals(offsetRecord.getCheckpointedRtPosition(TEST_KAFKA_URL2), p2);
  }

  @Test
  public void testBatchUpdateEOIP() {
    OffsetRecord offsetRecord = TestUtils
        .getOffsetRecord(ApacheKafkaOffsetPosition.of(100), Optional.empty(), DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
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

  private static Supplier<ByteBuffer> buf(long offset) {
    return () -> ApacheKafkaOffsetPosition.of(offset).toWireFormatBuffer();
  }

  private static Supplier<ByteBuffer> bytes(byte... arr) {
    return () -> ByteBuffer.wrap(arr);
  }

  private static final Supplier<ByteBuffer> NULL_BUF = () -> null;
  private static final Supplier<ByteBuffer> EMPTY_BUF = () -> ByteBuffer.allocate(0);
  private static final Supplier<ByteBuffer> NO_REMAINING_BUF = () -> {
    ByteBuffer b = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    b.position(3);
    return b;
  };
  private static final Supplier<ByteBuffer> LATEST_BUF = PubSubSymbolicPosition.LATEST::toWireFormatBuffer;
  private static final Supplier<ByteBuffer> EARLIEST_BUF = PubSubSymbolicPosition.EARLIEST::toWireFormatBuffer;

  @DataProvider(name = "dpDeserialize", parallel = true)
  public Object[][] dpDeserialize() {
    return new Object[][] {
        // name, bufferSupplier, offset, expectedNumeric, expectApacheKafka
        { "valid>offset", buf(1000), 500L, 1000L, true }, { "valid==offset", buf(1000), 1000L, 1000L, true },
        { "valid>offset_2k_vs1.5k", buf(2000), 1500L, 2000L, true }, { "regress_behind", buf(500), 1000L, 1000L, true },
        { "regress_far", buf(500), 2000L, 2000L, true }, { "zero_vs_100", buf(0), 100L, 100L, true },
        { "null_bytes", NULL_BUF, 1500L, 1500L, true }, { "empty_bytes", EMPTY_BUF, 2500L, 2500L, true },
        { "no_remaining", NO_REMAINING_BUF, 3500L, 3500L, true },
        { "malformed_fffe", bytes((byte) 0xFF, (byte) 0xFE, (byte) 0xFD), 4000L, 4000L, true },
        { "corrupted_short_seq", bytes((byte) 0xFF, (byte) 0xFE, (byte) 0xFD), 5000L, 5000L, true },
        { "invalid_length", bytes((byte) 0xFF), 6000L, 6000L, true },
        { "max_long_offset", NULL_BUF, Long.MAX_VALUE, Long.MAX_VALUE, true },
        { "zero_offset_valid_pos", buf(1000), 0L, 1000L, true }, { "negative_offset", NULL_BUF, -1L, -1L, false },
        { "EARLIEST_vs_1000", EARLIEST_BUF, 1000L, -1L, false },
        { "LATEST_vs_1000", LATEST_BUF, 1000L, Long.MAX_VALUE, false },
        { "LATEST_vs_max-1", LATEST_BUF, Long.MAX_VALUE - 1, Long.MAX_VALUE, false },
        // Consistency checks (same inputs twice)
        { "consistency_sameA", buf(1000), 500L, 1000L, true }, { "consistency_sameB", buf(1000), 500L, 1000L, true },
        // Consistency fallback paths equivalence
        { "consistency_fallback_null", NULL_BUF, 1500L, 1500L, true },
        { "consistency_fallback_empty", EMPTY_BUF, 1500L, 1500L, true }, };
  }

  @Test(dataProvider = "dpDeserialize", timeOut = 10_000)
  public void testDeserializePositionWithOffsetFallback(
      String name,
      Supplier<ByteBuffer> bufferSupplier,
      long offset,
      long expectedNumeric,
      boolean expectApacheKafka) {
    PubSubPosition result = offsetRecord.deserializePositionWithOffsetFallback(bufferSupplier.get(), offset);
    if (expectApacheKafka) {
      assertTrue(result instanceof ApacheKafkaOffsetPosition, name + ": expected AK position");
      assertEquals(
          ((ApacheKafkaOffsetPosition) result).getInternalOffset(),
          expectedNumeric,
          name + ": numeric mismatch");
    } else if (expectedNumeric == -1L) {
      assertEquals(result, PubSubSymbolicPosition.EARLIEST, name + ": expected EARLIEST");
    } else {
      assertEquals(result, PubSubSymbolicPosition.LATEST, name + ": expected LATEST");
    }
  }
}

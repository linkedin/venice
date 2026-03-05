package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.WriterChunkingHelper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PartitionConsumptionStateTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final PubSubTopicPartition TOPIC_PARTITION =
      new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("topic1_v1"), 0);
  private PubSubContext pubSubContext;

  @BeforeMethod
  public void setUp() {
    pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
  }

  @Test
  public void testUpdateChecksum() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    pcs.initializeExpectedChecksum();
    byte[] rmdPayload = new byte[] { 127 };
    byte[] key1 = new byte[] { 1 };
    byte[] key2 = new byte[] { 2 };
    byte[] key3 = new byte[] { 3 };
    byte[] key4 = new byte[] { 4 };
    byte[] valuePayload1 = new byte[] { 10 };
    byte[] valuePayload3 = new byte[] { 11 };
    byte[] valuePayload4 = new byte[] { 12 };

    Put put = new Put();
    // Try to update a value chunk (should only update value payload)
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    put.putValue = ByteBuffer.wrap(valuePayload1);
    put.replicationMetadataPayload = WriterChunkingHelper.EMPTY_BYTE_BUFFER;
    pcs.maybeUpdateExpectedChecksum(key1, put);
    // Try to update a RMD chunk (should not be updated)
    put.putValue = WriterChunkingHelper.EMPTY_BYTE_BUFFER;
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key2, put);
    // Try to update a regular value with RMD (should only update value payload)
    put.schemaId = 1;
    put.putValue = ByteBuffer.wrap(valuePayload3);
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key3, put);
    // Try to update a manifest (should only update value payload)
    put.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    put.putValue = ByteBuffer.wrap(valuePayload4);
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key4, put);

    byte[] checksum = pcs.getExpectedChecksum();
    Assert.assertNotNull(checksum);

    // Calculate expected checksum.
    CheckSum expectedChecksum = CheckSum.getInstance(CheckSumType.MD5);
    expectedChecksum.update(key1);
    expectedChecksum.update(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion());
    expectedChecksum.update(valuePayload1, 0, valuePayload1.length);
    expectedChecksum.update(key3);
    expectedChecksum.update(1);
    expectedChecksum.update(valuePayload3, 0, valuePayload3.length);
    expectedChecksum.update(key4);
    expectedChecksum.update(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    expectedChecksum.update(valuePayload4, 0, valuePayload4.length);

    Assert.assertEquals(expectedChecksum.getCheckSum(), checksum);
  }

  /**
   * Test the different transientRecordMap operations.
   */
  @Test
  public void testTransientRecordMap() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    assertEquals(pcs.getPubSubContext(), pubSubContext);
    PubSubPosition consumedPosition1Mock = mock(PubSubPosition.class);
    PubSubPosition consumedPosition2Mock = mock(PubSubPosition.class);
    PubSubPosition consumedPosition3Mock = mock(PubSubPosition.class);

    byte[] key1 = new byte[] { 65, 66, 67, 68 };
    byte[] key2 = new byte[] { 65, 66, 67, 68 };
    byte[] key3 = new byte[] { 65, 66, 67, 69 };
    byte[] value1 = new byte[] { 97, 98, 99 };
    byte[] value2 = new byte[] { 97, 98, 99, 100 };

    String schema = "\"string\"";
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord record = new GenericData.Record(aaSchema);
    // Test removal succeeds if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, consumedPosition1Mock, key1, 5, record);
    PartitionConsumptionState.TransientRecord tr1 = pcs.getTransientRecord(key2);
    Assert.assertEquals(tr1.getValue(), null);
    Assert.assertEquals(tr1.getValueLen(), -1);
    Assert.assertEquals(tr1.getValueOffset(), -1);
    Assert.assertEquals(tr1.getValueSchemaId(), 5);
    // Assert.assertEquals(tr1.getReplicationMetadata(), replicationMetadataKey1_1);

    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
    PartitionConsumptionState.TransientRecord tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition1Mock, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 0);

    // Test removal fails if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, consumedPosition1Mock, key1, value1, 100, value1.length, 5, null);
    pcs.setTransientRecord(-1, consumedPosition2Mock, key3, 5, null);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);
    pcs.setTransientRecord(-1, consumedPosition3Mock, key1, value2, 100, value2.length, 5, null);

    tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition1Mock, key1);
    Assert.assertNotNull(tr2);
    Assert.assertEquals(tr2.getValue(), value2);
    Assert.assertEquals(tr2.getValueLen(), value2.length);
    Assert.assertEquals(tr2.getValueOffset(), 100);
    Assert.assertEquals(tr2.getValueSchemaId(), 5);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);

    tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition3Mock, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
  }

  @Test
  public void testIsLeaderCompleted() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    // default is LEADER_NOT_COMPLETED
    assertEquals(pcs.getLeaderCompleteState(), LeaderCompleteState.LEADER_NOT_COMPLETED);
    assertFalse(pcs.isLeaderCompleted());

    // test with LEADER_COMPLETED
    pcs.setLeaderCompleteState(LeaderCompleteState.LEADER_COMPLETED);
    assertTrue(pcs.isLeaderCompleted());
  }

  @Test
  public void testAddIncPushVersionToPendingReportList() {
    List<String> pendingReportIncrementalPush = new ArrayList<>();
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pendingReportIncrementalPush).when(offsetRecord).getPendingReportIncPushVersionList();
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        offsetRecord,
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    pcs.addIncPushVersionToPendingReportList("a");
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().size(), 1);
    for (int i = 0; i < 50; i++) {
      pcs.addIncPushVersionToPendingReportList("v_" + i);
    }
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().size(), 50);
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().get(0), "v_0");
  }

  @Test
  public void testDolStateOperations() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));

    // Initially, DoL state should be null
    assertNull(pcs.getDolState());

    // Test setDolState
    DolStamp dolStamp = new DolStamp(42L, "test-host-123");
    pcs.setDolState(dolStamp);
    assertNotNull(pcs.getDolState());
    assertEquals(pcs.getDolState(), dolStamp);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 42L);
    assertEquals(pcs.getDolState().getHostId(), "test-host-123");

    // Test clearDolState
    pcs.clearDolState();
    assertNull(pcs.getDolState());
  }

  @Test
  public void testHighestLeadershipTermOperations() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));

    // Initially, highest leadership term should be -1 (default uninitialized value)
    assertEquals(pcs.getHighestLeadershipTerm(), -1L);

    // Test setHighestLeadershipTerm
    pcs.setHighestLeadershipTerm(100L);
    assertEquals(pcs.getHighestLeadershipTerm(), 100L);

    // Test updating to a higher term
    pcs.setHighestLeadershipTerm(200L);
    assertEquals(pcs.getHighestLeadershipTerm(), 200L);

    // Test setting to a lower term (should be allowed)
    pcs.setHighestLeadershipTerm(50L);
    assertEquals(pcs.getHighestLeadershipTerm(), 50L);
  }

  @Test
  public void testActiveLeaderTermOperations() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));

    // Initially should be DEFAULT_TERM_ID (-1)
    assertEquals(pcs.getActiveLeaderTerm(), VeniceWriter.DEFAULT_TERM_ID);

    // Set to a valid leadership term
    pcs.setActiveLeaderTerm(100L);
    assertEquals(pcs.getActiveLeaderTerm(), 100L);

    // Update to a higher term
    pcs.setActiveLeaderTerm(200L);
    assertEquals(pcs.getActiveLeaderTerm(), 200L);

    // Clear back to DEFAULT_TERM_ID (simulating LEADER_TO_STANDBY)
    pcs.setActiveLeaderTerm(VeniceWriter.DEFAULT_TERM_ID);
    assertEquals(pcs.getActiveLeaderTerm(), VeniceWriter.DEFAULT_TERM_ID);
  }

  @Test
  public void testActiveLeaderTermIndependentFromHighestLeadershipTerm() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));

    // Set activeLeaderTerm and highestLeadershipTerm to different values
    pcs.setActiveLeaderTerm(100L);
    pcs.setHighestLeadershipTerm(200L);

    // Verify they are independent
    assertEquals(pcs.getActiveLeaderTerm(), 100L);
    assertEquals(pcs.getHighestLeadershipTerm(), 200L);

    // Changing one should not affect the other
    pcs.setHighestLeadershipTerm(300L);
    assertEquals(pcs.getActiveLeaderTerm(), 100L);

    pcs.setActiveLeaderTerm(VeniceWriter.DEFAULT_TERM_ID);
    assertEquals(pcs.getHighestLeadershipTerm(), 300L);
  }

  @Test
  public void testDolStateWithMultipleUpdates() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));

    // Set first DolStamp
    DolStamp dolStamp1 = new DolStamp(1L, "host-1");
    pcs.setDolState(dolStamp1);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 1L);

    // Replace with second DolStamp
    DolStamp dolStamp2 = new DolStamp(2L, "host-2");
    pcs.setDolState(dolStamp2);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 2L);
    assertEquals(pcs.getDolState().getHostId(), "host-2");

    // Clear and verify
    pcs.clearDolState();
    assertNull(pcs.getDolState());
  }
}

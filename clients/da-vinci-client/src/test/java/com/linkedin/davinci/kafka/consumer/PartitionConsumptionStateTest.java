package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.WriterChunkingHelper;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionConsumptionStateTest {
  private static final String replicaId = Utils.getReplicaId("topic1", 0);

  @Test
  public void testUpdateChecksum() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(replicaId, 0, 1, mock(OffsetRecord.class), false);
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
    PartitionConsumptionState pcs = new PartitionConsumptionState(replicaId, 0, 1, mock(OffsetRecord.class), false);

    byte[] key1 = new byte[] { 65, 66, 67, 68 };
    byte[] key2 = new byte[] { 65, 66, 67, 68 };
    byte[] key3 = new byte[] { 65, 66, 67, 69 };
    byte[] value1 = new byte[] { 97, 98, 99 };
    byte[] value2 = new byte[] { 97, 98, 99, 100 };

    String schema = "\"string\"";
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord record = new GenericData.Record(aaSchema);
    // Test removal succeeds if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, 1, key1, 5, record);
    PartitionConsumptionState.TransientRecord tr1 = pcs.getTransientRecord(key2);
    Assert.assertEquals(tr1.getValue(), null);
    Assert.assertEquals(tr1.getValueLen(), -1);
    Assert.assertEquals(tr1.getValueOffset(), -1);
    Assert.assertEquals(tr1.getValueSchemaId(), 5);
    // Assert.assertEquals(tr1.getReplicationMetadata(), replicationMetadataKey1_1);

    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
    PartitionConsumptionState.TransientRecord tr2 = pcs.mayRemoveTransientRecord(-1, 1, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 0);

    // Test removal fails if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, 1, key1, value1, 100, value1.length, 5, null);
    pcs.setTransientRecord(-1, 2, key3, 5, null);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);
    pcs.setTransientRecord(-1, 3, key1, value2, 100, value2.length, 5, null);

    tr2 = pcs.mayRemoveTransientRecord(-1, 1, key1);
    Assert.assertNotNull(tr2);
    Assert.assertEquals(tr2.getValue(), value2);
    Assert.assertEquals(tr2.getValueLen(), value2.length);
    Assert.assertEquals(tr2.getValueOffset(), 100);
    Assert.assertEquals(tr2.getValueSchemaId(), 5);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);

    tr2 = pcs.mayRemoveTransientRecord(-1, 3, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);

  }

  @Test
  public void testIsLeaderCompleted() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(replicaId, 0, 1, mock(OffsetRecord.class), false);
    // default is LEADER_NOT_COMPLETED
    assertEquals(pcs.getLeaderCompleteState(), LeaderCompleteState.LEADER_NOT_COMPLETED);
    assertFalse(pcs.isLeaderCompleted());

    // test with LEADER_COMPLETED
    pcs.setLeaderCompleteState(LeaderCompleteState.LEADER_COMPLETED);
    assertTrue(pcs.isLeaderCompleted());
  }
}

package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionConsumptionStateTest {
  /**
   * Test the different transientRecordMap operations.
   */
  @Test
  public void testTransientRecordMap() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(0, 1, mock(OffsetRecord.class), false);

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
}

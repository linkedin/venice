package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.offsets.OffsetRecord;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class PartitionConsumptionStateTest {

  /**
   * Test the different transientRecordMap operations.
   */
  @Test
  public void testTransientRecordMap() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(0, 1, mock(OffsetRecord.class), false, false,
        IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);

    byte[] key1 = new byte[]{65,66,67,68};
    byte[] key2 = new byte[]{65,66,67,68};
    byte[] key3 = new byte[]{65,66,67,69};
    byte[] value1 = new byte[]{97,98,99};
    byte[] value2 = new byte[]{97,98,99,100};


    ByteBuffer replicationMetadataKey1_1 = ByteBuffer.wrap("replication_metadata_key1_1".getBytes());
    //Test removal succeeds if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord("", 1, key1, 5, replicationMetadataKey1_1);
    PartitionConsumptionState.TransientRecord tr1 = pcs.getTransientRecord(key2);
    Assert.assertEquals(tr1.getValue(), null);
    Assert.assertEquals(tr1.getValueLen(), -1);
    Assert.assertEquals(tr1.getValueOffset(), -1);
    Assert.assertEquals(tr1.getValueSchemaId(), 5);
    Assert.assertEquals(tr1.getReplicationMetadata(), replicationMetadataKey1_1);

    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
    PartitionConsumptionState.TransientRecord tr2 = pcs.mayRemoveTransientRecord("", 1, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 0);


    //Test removal fails if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord("", 1, key1, value1, 100, value1.length, 5, null);
    pcs.setTransientRecord("", 2, key3, 5, null);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);
    pcs.setTransientRecord("", 3, key1, value2, 100, value2.length, 5, null);

    tr2 = pcs.mayRemoveTransientRecord("", 1, key1);
    Assert.assertNotNull(tr2);
    Assert.assertEquals(tr2.getValue(), value2);
    Assert.assertEquals(tr2.getValueLen(), value2.length);
    Assert.assertEquals(tr2.getValueOffset(), 100);
    Assert.assertEquals(tr2.getValueSchemaId(), 5);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);

    tr2 = pcs.mayRemoveTransientRecord("", 3, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);

  }
}

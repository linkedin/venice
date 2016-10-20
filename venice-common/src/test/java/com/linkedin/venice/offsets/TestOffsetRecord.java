package com.linkedin.venice.offsets;

import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

public class TestOffsetRecord {
  @Test
  public void testToBytes() {
    OffsetRecord offsetRecord1 = TestUtils.getOffsetRecord(100);
    OffsetRecord offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes());
    Assert.assertEquals(offsetRecord2, offsetRecord1);

    offsetRecord1 = TestUtils.getOffsetRecord(100);
    offsetRecord1.complete();
    offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes());
    Assert.assertEquals(offsetRecord2, offsetRecord1);
  }

  @Test
  public void testGetEventTimeEpochMs() {
    OffsetRecord offsetRecord = new OffsetRecord();

    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    GUID producerGuid1 = GuidUtils.getGUID(veniceProperties);
    GUID producerGuid2 = GuidUtils.getGUID(veniceProperties);

    // Sanity check
    Assert.assertFalse(producerGuid1.equals(producerGuid2), "Producer GUIDs should be unique!");

    long firstTimestamp = 1;
    long secondTimestamp = 2;
    long thirdTimestamp = 3;

    ProducerPartitionState producerPartitionState1 = new ProducerPartitionState();
    producerPartitionState1.messageTimestamp = firstTimestamp;

    ProducerPartitionState producerPartitionState2 = new ProducerPartitionState();
    producerPartitionState2.messageTimestamp = secondTimestamp;

    offsetRecord.setProducerPartitionState(producerGuid1, producerPartitionState1);
    offsetRecord.setProducerPartitionState(producerGuid2, producerPartitionState2);

    Assert.assertEquals(offsetRecord.getEventTimeEpochMs(), secondTimestamp,
        "OffsetRecord should return the latest timestamp that it has seen!");

    producerPartitionState1.messageTimestamp = thirdTimestamp;

    offsetRecord.setProducerPartitionState(producerGuid1, producerPartitionState1);

    Assert.assertEquals(offsetRecord.getEventTimeEpochMs(), thirdTimestamp,
        "OffsetRecord should return the latest timestamp that it has seen!");
  }
}

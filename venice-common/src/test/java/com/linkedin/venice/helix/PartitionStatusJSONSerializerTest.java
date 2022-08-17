package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionStatusJSONSerializerTest {
  @Test
  public void testPartitionStatusSerializeAndDeserialize() throws IOException {
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("i1", ExecutionStatus.COMPLETED);
    partitionStatus.updateReplicaStatus("i2", ExecutionStatus.ERROR);
    partitionStatus.updateReplicaStatus("i3", ExecutionStatus.PROGRESS);

    PartitionStatusJSONSerializer serializer = new PartitionStatusJSONSerializer();
    byte[] data = serializer.serialize(partitionStatus, null);
    Assert.assertEquals(serializer.deserialize(data, null), partitionStatus);
  }
}

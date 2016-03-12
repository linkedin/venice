package com.linkedin.venice.helix;

import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.OfflineJob;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test case for job serialization and deserialization.
 */
public class TestJobJSONSerializer {
  @Test
  public void testOfflineJobSerializeAndDeserialize()
      throws IOException {
    OfflineJobJSONSerializer serializer = new OfflineJobJSONSerializer();
    long jobId=1;
    String topic = "test1";
    int numberOfPartition = 1;
    int replicaFactor = 1;
    OfflineJob job  = new OfflineJob(jobId,topic,numberOfPartition,replicaFactor);
    job.setStatus(ExecutionStatus.COMPLETED);

    byte[] data = serializer.serialize(job);

    OfflineJob newJob = serializer.deserialize(data);
    Assert.assertEquals(newJob.getJobId(),jobId);
    Assert.assertEquals(newJob.getKafkaTopic(),topic);
    Assert.assertEquals(newJob.getNumberOfPartition(),numberOfPartition);
    Assert.assertEquals(newJob.getReplicaFactor(),replicaFactor);
    Assert.assertEquals(newJob.getStatus(),ExecutionStatus.COMPLETED);
  }
}

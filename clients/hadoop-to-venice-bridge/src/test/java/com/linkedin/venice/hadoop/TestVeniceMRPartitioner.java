package com.linkedin.venice.hadoop;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceMRPartitioner extends AbstractTestVeniceMR {
  @Test
  public void testGetPartition() {
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    final int partitionNum = 97;
    VeniceMRPartitioner partitioner = new VeniceMRPartitioner();
    JobConf jobConf = setupJobConf();
    jobConf.set(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    partitioner.configure(jobConf);

    int partitionId = partitioner.getPartition(
        new BytesWritable(keyFieldValue.getBytes()),
        new BytesWritable(valueFieldValue.getBytes()),
        partitionNum);

    Assert.assertEquals(partitionId, 68);
  }
}

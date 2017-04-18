package com.linkedin.venice.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestVeniceMRPartitioner extends AbstractTestVeniceMR {
  @Test
  public void testGetPartition() {
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    final int partitionNum = 97;
    VeniceMRPartitioner partitioner = new VeniceMRPartitioner();
    partitioner.configure(setupJobConf());

    int partitionId = partitioner.getPartition(
        new BytesWritable(keyFieldValue.getBytes()),
        new BytesWritable(valueFieldValue.getBytes()),
        partitionNum
    );

    Assert.assertEquals(partitionId, 68);
  }
}

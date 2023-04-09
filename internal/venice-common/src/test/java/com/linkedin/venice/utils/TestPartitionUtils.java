package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPartitionUtils {
  @Test
  public void testCalculatePartitionCount() {
    long storageQuota = 10;
    long partitionSize = 10;
    int minPartitionCount = 1;
    int maxPartitionCount = 16;
    int roundUpSize = 10;
    int partitionCount = PartitionUtils.calculatePartitionCount(
        "store",
        storageQuota,
        0,
        partitionSize,
        minPartitionCount,
        maxPartitionCount,
        true,
        roundUpSize);
    Assert.assertEquals(partitionCount, 10, "Partition count should round up to 10");
  }
}

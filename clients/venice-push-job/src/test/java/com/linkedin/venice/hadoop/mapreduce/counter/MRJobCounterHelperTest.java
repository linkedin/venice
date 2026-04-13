package com.linkedin.venice.hadoop.mapreduce.counter;

import java.util.Map;
import org.apache.hadoop.mapred.Counters;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MRJobCounterHelperTest {
  @Test
  public void testGetPerPartitionRecordCounts() {
    Counters counters = new Counters();
    // Simulate per-partition record counts by incrementing counters in the expected group
    Counters.Counter counter0 = counters.findCounter("Per Partition Record Count", "0");
    counter0.increment(100);
    Counters.Counter counter1 = counters.findCounter("Per Partition Record Count", "1");
    counter1.increment(200);
    Counters.Counter counter5 = counters.findCounter("Per Partition Record Count", "5");
    counter5.increment(50);

    Map<Integer, Long> result = MRJobCounterHelper.getPerPartitionRecordCounts(counters);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals((long) result.get(0), 100L);
    Assert.assertEquals((long) result.get(1), 200L);
    Assert.assertEquals((long) result.get(5), 50L);
  }

  @Test
  public void testGetPerPartitionRecordCountsEmpty() {
    Counters counters = new Counters();
    Map<Integer, Long> result = MRJobCounterHelper.getPerPartitionRecordCounts(counters);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testGetPerPartitionRecordCountsWithMultipleIncrements() {
    Counters counters = new Counters();
    Counters.Counter counter = counters.findCounter("Per Partition Record Count", "3");
    counter.increment(10);
    counter.increment(5);

    Map<Integer, Long> result = MRJobCounterHelper.getPerPartitionRecordCounts(counters);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals((long) result.get(3), 15L);
  }

  @Test
  public void testIncrPartitionRecordCountWithNullReporter() {
    // Passing null reporter should not throw
    MRJobCounterHelper.incrPartitionRecordCount(null, 0, 1);
  }
}

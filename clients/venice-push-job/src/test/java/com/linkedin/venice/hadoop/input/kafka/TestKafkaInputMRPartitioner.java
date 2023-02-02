package com.linkedin.venice.hadoop.input.kafka;

import static org.testng.Assert.*;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;


public class TestKafkaInputMRPartitioner {
  private static final KafkaInputMRPartitioner MR_PARTITIONER = new KafkaInputMRPartitioner();
  static {
    MR_PARTITIONER.configure(new JobConf());
  }
  private static final int PARTITION_COUNT = 10000;

  @Test
  public void testWithDifferentKeys() {
    BytesWritable bwForKey1 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 1);
    BytesWritable bwForKey2 = TestKafkaInputKeyComparator.getBytesWritable("223".getBytes(), 2);

    assertNotEquals(
        MR_PARTITIONER.getPartition(bwForKey1, PARTITION_COUNT),
        MR_PARTITIONER.getPartition(bwForKey2, PARTITION_COUNT));
  }

  @Test
  public void testWithSameKeyWithDifferentOffsets() {
    BytesWritable bwForKey1 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 1);
    BytesWritable bwForKey2 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 2);

    assertEquals(
        MR_PARTITIONER.getPartition(bwForKey1, PARTITION_COUNT),
        MR_PARTITIONER.getPartition(bwForKey2, PARTITION_COUNT));
  }
}

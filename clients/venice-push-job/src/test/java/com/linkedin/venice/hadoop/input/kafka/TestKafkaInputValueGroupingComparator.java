package com.linkedin.venice.hadoop.input.kafka;

import static org.testng.Assert.assertTrue;

import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;


public class TestKafkaInputValueGroupingComparator {
  private static final KafkaInputValueGroupingComparator GROUPING_COMPARATOR = new KafkaInputValueGroupingComparator();

  @Test
  public void testDifferentKeys() {
    BytesWritable bwForKey1 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 1);
    BytesWritable bwForKey2 = TestKafkaInputKeyComparator.getBytesWritable("223".getBytes(), 2);

    assertTrue(GROUPING_COMPARATOR.compare(bwForKey1, bwForKey2) < 0);
  }

  @Test
  public void testWithSameKeyWithDifferentOffsets() {
    BytesWritable bwForKey1 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 1);
    BytesWritable bwForKey2 = TestKafkaInputKeyComparator.getBytesWritable("123".getBytes(), 2);

    assertTrue(GROUPING_COMPARATOR.compare(bwForKey1, bwForKey2) == 0);
  }
}

package com.linkedin.venice.spark.datawriter.partition;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionSorterTest {
  @Test
  public void testCompare() {
    PartitionSorter partitionSorter = new PartitionSorter();

    byte[] key1 = new byte[] { 1, 100 };
    byte[] value1 = new byte[] { 1, 10 };
    Row row1 = RowFactory.create(key1, value1);

    byte[] key2 = new byte[] { 1, 10 };
    byte[] value2 = new byte[] { 1, 10 };
    Row row2 = RowFactory.create(key2, value2);

    // key1 > key2
    Assert.assertEquals(partitionSorter.compare(row1, row2), 90);

    // key2 < key1
    Assert.assertEquals(partitionSorter.compare(row2, row1), -90);

    byte[] key3 = new byte[] { 1, 100 };
    byte[] value3 = new byte[] { 1, 100 };
    Row row3 = RowFactory.create(key3, value3);

    // key1 = key3, value1 < value3
    Assert.assertEquals(partitionSorter.compare(row1, row3), -90);

    byte[] key4 = new byte[] { 1, -100 }; // For unsigned comparison, this is 156. For signed comparison, this is -100.
    byte[] value4 = new byte[] { 1, 10 };
    Row row4 = RowFactory.create(key4, value4);

    // key1 < k4 if using unsigned, but key1 > k4 if using signed
    Assert.assertEquals(partitionSorter.compare(row1, row4), -56);
  }
}

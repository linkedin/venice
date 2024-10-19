package com.linkedin.venice.spark.datawriter.partition;

import com.linkedin.venice.spark.datawriter.writer.SparkPartitionWriter;
import com.linkedin.venice.utils.ArrayUtils;
import java.io.Serializable;
import java.util.Comparator;
import org.apache.spark.sql.Row;


/**
 * Sort the rows based on the key and value in ascending order using unsigned byte comparison.
 * <ul>
 *   <li>The sorting on the key is the same as what RocksDB and Shuffle-Sort in MapReduce use.</li>
 *   <li>The sorting on the value is to make {@link SparkPartitionWriter} be able to optimize the de-duping of values.</li>
 * </ul>
 */
public class PartitionSorter implements Comparator<Row>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public int compare(Row r1, Row r2) {
    // For primary sort
    byte[] key1 = (byte[]) r1.get(0);
    byte[] key2 = (byte[]) r2.get(0);

    int keyCompare = ArrayUtils.compareUnsigned(key1, key2);
    if (keyCompare != 0) {
      return keyCompare;
    }

    // For secondary sort
    byte[] value1 = (byte[]) r1.get(1);
    byte[] value2 = (byte[]) r2.get(1);
    return ArrayUtils.compareUnsigned(value1, value2);
  }
}

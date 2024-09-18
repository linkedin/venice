package com.linkedin.venice.spark.utils;

import java.util.Comparator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;


/**
 * Spark partitioning functionality in Dataframe and Dataset APIs is not very flexible. This class provides some
 * functionality by using the underlying RDD implementation.
 */
public final class SparkPartitionUtils {
  private SparkPartitionUtils() {
  }

  /**
   * This function provides the equivalent of {@link JavaPairRDD#repartitionAndSortWithinPartitions} in Dataframe API.
   * 1. Convert to {@link JavaPairRDD}
   * 2. Use {@link JavaPairRDD#repartitionAndSortWithinPartitions} to partition and perform primary and secondary sort
   * 3. Convert {@link JavaPairRDD} to {@link RDD}
   * 4. Convert {@link RDD} back to Dataframe
   */
  public static Dataset<Row> repartitionAndSortWithinPartitions(
      Dataset<Row> df,
      Partitioner partitioner,
      Comparator<Row> comparator) {
    RDD<Row> partitionedRowRDD = df.javaRDD()
        .mapToPair((PairFunction<Row, Row, Row>) row -> new Tuple2<>(row, row))
        .repartitionAndSortWithinPartitions(partitioner, comparator)
        .map(v1 -> v1._1)
        .rdd();
    return df.sparkSession().createDataFrame(partitionedRowRDD, df.schema());
  }
}

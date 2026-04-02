package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.spark.SparkConstants;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StageMetricsTest {
  private SparkSession spark;

  @BeforeClass
  public void setUp() {
    spark =
        SparkSession.builder().appName("StageMetricsTest").master(SparkConstants.DEFAULT_SPARK_CLUSTER).getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateWithCorrectNames() {
    StageMetrics metrics = new StageMetrics(spark.sparkContext(), "test_stage");

    Assert.assertEquals(metrics.getStageName(), "test_stage");
    Assert.assertNotNull(metrics.recordsIn);
    Assert.assertNotNull(metrics.recordsOut);
    Assert.assertNotNull(metrics.bytesIn);
    Assert.assertNotNull(metrics.bytesOut);
    Assert.assertNotNull(metrics.timeNs);
  }

  @Test
  public void testAccumulatorsInitToZero() {
    StageMetrics metrics = new StageMetrics(spark.sparkContext(), "zero_stage");

    Assert.assertEquals((long) metrics.recordsIn.value(), 0L);
    Assert.assertEquals((long) metrics.recordsOut.value(), 0L);
    Assert.assertEquals((long) metrics.bytesIn.value(), 0L);
    Assert.assertEquals((long) metrics.bytesOut.value(), 0L);
    Assert.assertEquals((long) metrics.timeNs.value(), 0L);
  }

  @Test
  public void testRecordCountAccumulation() {
    StageMetrics metrics = new StageMetrics(spark.sparkContext(), "record_stage");

    metrics.recordsIn.add(100);
    metrics.recordsIn.add(200);
    metrics.recordsOut.add(50);

    Assert.assertEquals((long) metrics.recordsIn.value(), 300L);
    Assert.assertEquals((long) metrics.recordsOut.value(), 50L);
  }

  @Test
  public void testByteCountAccumulation() {
    StageMetrics metrics = new StageMetrics(spark.sparkContext(), "byte_stage");

    metrics.bytesIn.add(1024);
    metrics.bytesIn.add(2048);
    metrics.bytesOut.add(512);

    Assert.assertEquals((long) metrics.bytesIn.value(), 3072L);
    Assert.assertEquals((long) metrics.bytesOut.value(), 512L);
  }

  @Test
  public void testTimeTracking() {
    StageMetrics metrics = new StageMetrics(spark.sparkContext(), "time_stage");

    // Simulate some work
    metrics.timeNs.add(1_500_000_000L); // 1.5 seconds in nanoseconds

    Assert.assertEquals((long) metrics.timeNs.value(), 1_500_000_000L);
  }
}

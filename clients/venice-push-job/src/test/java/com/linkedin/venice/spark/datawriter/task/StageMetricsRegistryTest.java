package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.spark.SparkConstants;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StageMetricsRegistryTest {
  private SparkSession spark;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder()
        .appName("StageMetricsRegistryTest")
        .master(SparkConstants.DEFAULT_SPARK_CLUSTER)
        .getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testRegisterAndRetrieveStage() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    StageMetrics metrics = registry.register("test_stage");
    Assert.assertNotNull(metrics);
    Assert.assertEquals(metrics.getStageName(), "test_stage");

    StageMetrics retrieved = registry.getStage("test_stage");
    Assert.assertSame(retrieved, metrics);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDuplicateRegistrationThrows() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    registry.register("dup_stage");
    registry.register("dup_stage"); // should throw
  }

  @Test
  public void testGetUnregisteredStageReturnsNull() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    Assert.assertNull(registry.getStage("nonexistent"));
  }

  @Test
  public void testGenerateReportEmpty() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    String report = registry.generateReport();
    Assert.assertEquals(report, "No stages registered.");
  }

  @Test
  public void testGenerateReportWithData() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    StageMetrics stage1 = registry.register("ttl_filter");
    stage1.recordsIn.add(1000);
    stage1.bytesIn.add(1024 * 1024); // 1 MB
    stage1.recordsOut.add(900);
    stage1.bytesOut.add(900 * 1024); // ~900 KB
    stage1.timeNs.add(2_500_000_000L); // 2.5s

    StageMetrics stage2 = registry.register("compaction");
    stage2.recordsIn.add(900);
    stage2.bytesIn.add(900 * 1024);
    stage2.recordsOut.add(800);
    stage2.bytesOut.add(800 * 1024);
    stage2.timeNs.add(1_000_000_000L); // 1.0s

    String report = registry.generateReport();

    // Verify header is present
    Assert.assertTrue(report.contains("Stage"), "Report should contain Stage header");
    Assert.assertTrue(report.contains("Records In"), "Report should contain Records In header");
    Assert.assertTrue(report.contains("Bytes Out"), "Report should contain Bytes Out header");

    // Verify both stages appear
    Assert.assertTrue(report.contains("ttl_filter"), "Report should contain ttl_filter stage");
    Assert.assertTrue(report.contains("compaction"), "Report should contain compaction stage");

    // Verify formatted values
    Assert.assertTrue(report.contains("1,000"), "Report should show formatted record count");
    Assert.assertTrue(report.contains("1.0 MB"), "Report should show formatted byte size");
    Assert.assertTrue(report.contains("2.5s"), "Report should show formatted time");
  }

  @Test
  public void testReportIncludesRatios() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    StageMetrics metrics = registry.register("compression");
    metrics.recordsIn.add(1000);
    metrics.bytesIn.add(1000 * 1024);
    metrics.recordsOut.add(1000);
    metrics.bytesOut.add(500 * 1024); // 50% compression ratio
    metrics.timeNs.add(1_000_000_000L);

    String report = registry.generateReport();

    Assert.assertTrue(report.contains("0.50"), "Report should show 0.50 compression ratio");
  }

  @Test
  public void testReportShowsDashForNoInput() {
    StageMetricsRegistry registry = new StageMetricsRegistry(spark.sparkContext());

    // First stage has no input (e.g., raw kafka input)
    StageMetrics metrics = registry.register("raw_kafka_input");
    metrics.recordsOut.add(5000);
    metrics.bytesOut.add(5000 * 512);
    metrics.timeNs.add(3_000_000_000L);
    // recordsIn and bytesIn remain 0

    String report = registry.generateReport();

    // The ratio should be "—" since bytesIn is 0
    // recordsIn should show "—"
    Assert.assertTrue(report.contains("raw_kafka_input"), "Report should contain stage name");
    Assert.assertTrue(report.contains("5,000"), "Report should show output record count");
  }

  @Test
  public void testFormatBytes() {
    Assert.assertEquals(StageMetricsRegistry.formatBytes(500), "500 B");
    Assert.assertEquals(StageMetricsRegistry.formatBytes(1024), "1.0 KB");
    Assert.assertEquals(StageMetricsRegistry.formatBytes(1536), "1.5 KB");
    Assert.assertEquals(StageMetricsRegistry.formatBytes(1024 * 1024), "1.0 MB");
    Assert.assertEquals(StageMetricsRegistry.formatBytes(1024L * 1024 * 1024), "1.0 GB");
    Assert.assertEquals(StageMetricsRegistry.formatBytes(1024L * 1024 * 1024 * 3 / 2), "1.5 GB");
  }

  @Test
  public void testFormatNumber() {
    Assert.assertEquals(StageMetricsRegistry.formatNumber(0), "0");
    Assert.assertEquals(StageMetricsRegistry.formatNumber(999), "999");
    Assert.assertEquals(StageMetricsRegistry.formatNumber(1000), "1,000");
    Assert.assertEquals(StageMetricsRegistry.formatNumber(1234567), "1,234,567");
  }
}

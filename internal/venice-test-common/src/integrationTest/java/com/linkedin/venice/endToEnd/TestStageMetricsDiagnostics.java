package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.spark.datawriter.task.StageMetrics;
import com.linkedin.venice.spark.datawriter.task.StageMetricsRegistry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for VPJ Spark pipeline stage diagnostics (StageMetricsRegistry).
 * Verifies that per-stage record counts, byte sizes, and timing are correctly captured
 * for each sub-computation in the pipeline: TTL filtering, compaction, chunk assembly,
 * compression re-encoding, and Kafka write.
 */
public class TestStageMetricsDiagnostics extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestStageMetricsDiagnostics.class);
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int RECORD_COUNT = DEFAULT_USER_DATA_RECORD_COUNT;

  private String[] dcNames;

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  /**
   * Test that a basic batch push + KIF repush produces correct compaction stage metrics.
   * Non-chunked, no TTL, no compression change — exercises the compaction path only.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testCompactionMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-compaction");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create batch-only store with NR + AA
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    // Batch push v1
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // KIF repush — exercises compaction
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-repush-compaction", repushProps)) {
      repushJob.run();

      StageMetricsRegistry registry = repushJob.getStageMetricsRegistry();
      assertNotNull(registry, "StageMetricsRegistry should be available after Spark repush");

      // Compaction should be registered (non-chunked store)
      StageMetrics compaction = registry.getStage("compaction");
      assertNotNull(compaction, "Compaction stage should be registered for non-chunked repush");
      assertTrue((long) compaction.recordsIn.value() > 0, "Compaction should have input records");
      assertTrue((long) compaction.recordsOut.value() > 0, "Compaction should have output records");
      assertTrue(
          (long) compaction.recordsOut.value() <= (long) compaction.recordsIn.value(),
          "Compaction output should be <= input (dedup)");
      assertTrue((long) compaction.bytesIn.value() > 0, "Compaction should track input bytes");
      assertTrue((long) compaction.bytesOut.value() > 0, "Compaction should track output bytes");

      // Kafka write should be registered
      StageMetrics kafkaWrite = registry.getStage("kafka_write");
      assertNotNull(kafkaWrite, "Kafka write stage should be registered");
      assertTrue((long) kafkaWrite.recordsIn.value() > 0, "Kafka write should have input records");
      assertTrue((long) kafkaWrite.timeNs.value() > 0, "Kafka write should track timing");

      LOGGER.info("Compaction diagnostics report:\n{}", registry.generateReport());
    }
  }

  /**
   * Test that TTL repush produces correct TTL filter stage metrics.
   * Hybrid store with TTL enabled — exercises the TTL filter path.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testTTLFilterMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-ttl");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create hybrid store with AA + NR
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])
            .setHybridRewindSeconds(86400L)
            .setHybridOffsetLagThreshold(10L)).close();

    // Batch push v1
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-ttl-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // TTL repush — TTL threshold in the far future so all records pass
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.put(REPUSH_TTL_ENABLE, "true");
    repushProps.put(REPUSH_TTL_START_TIMESTAMP, "0"); // epoch 0 — everything is newer, nothing filtered
    repushProps.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-ttl-repush", repushProps)) {
      repushJob.run();

      StageMetricsRegistry registry = repushJob.getStageMetricsRegistry();
      assertNotNull(registry, "StageMetricsRegistry should be available after TTL repush");

      // TTL filter should be registered
      StageMetrics ttlFilter = registry.getStage("ttl_filter");
      assertNotNull(ttlFilter, "TTL filter stage should be registered for TTL repush");
      assertTrue((long) ttlFilter.recordsIn.value() > 0, "TTL filter should have input records");
      // With TTL start at epoch 0, nothing should be filtered — output == input
      assertEquals(
          (long) ttlFilter.recordsOut.value(),
          (long) ttlFilter.recordsIn.value(),
          "TTL filter with epoch-0 threshold should keep all records");
      assertTrue((long) ttlFilter.timeNs.value() > 0, "TTL filter should track timing");

      // Kafka write should also be present
      StageMetrics kafkaWrite = registry.getStage("kafka_write");
      assertNotNull(kafkaWrite, "Kafka write stage should be registered");
      assertTrue((long) kafkaWrite.recordsIn.value() > 0, "Kafka write should have input records");

      LOGGER.info("TTL filter diagnostics report:\n{}", registry.generateReport());
    }
  }

  /**
   * Test that a chunked store repush produces correct chunk assembly stage metrics.
   * Large values trigger chunking — exercises the chunk assembly path.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testChunkAssemblyMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-chunking");
    File inputDir = getTempDataDirectory();
    // Write large records that will trigger chunking
    int largeValueSize = 500_000; // 500KB per value — will be chunked
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT, largeValueSize);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create batch store with chunking + RMD chunking enabled
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])
            .setChunkingEnabled(true)
            .setRmdChunkingEnabled(true)).close();

    // Batch push v1
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-chunking-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // KIF repush — exercises chunk assembly
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-chunking-repush", repushProps)) {
      repushJob.run();

      StageMetricsRegistry registry = repushJob.getStageMetricsRegistry();
      assertNotNull(registry, "StageMetricsRegistry should be available after chunked repush");

      // Chunk assembly should be registered (chunking enabled)
      StageMetrics chunkAssembly = registry.getStage("chunk_assembly");
      assertNotNull(chunkAssembly, "Chunk assembly stage should be registered for chunked repush");
      assertTrue((long) chunkAssembly.recordsIn.value() > 0, "Chunk assembly should have input records (chunks)");
      assertTrue((long) chunkAssembly.recordsOut.value() > 0, "Chunk assembly should have output records (assembled)");
      // Input > output because multiple chunks assemble into one record
      assertTrue(
          (long) chunkAssembly.recordsIn.value() > (long) chunkAssembly.recordsOut.value(),
          "Chunk assembly input (chunks) should exceed output (assembled keys)");
      assertTrue((long) chunkAssembly.bytesIn.value() > 0, "Chunk assembly should track input bytes");
      // Output bytes should be larger per-record (assembled values) but fewer records
      assertTrue((long) chunkAssembly.bytesOut.value() > 0, "Chunk assembly should track output bytes");

      // Compaction should NOT be registered (chunking enabled skips compaction)
      StageMetrics compaction = registry.getStage("compaction");
      assertTrue(compaction == null, "Compaction should NOT be registered when chunking is enabled");

      // Kafka write should be present
      StageMetrics kafkaWrite = registry.getStage("kafka_write");
      assertNotNull(kafkaWrite, "Kafka write stage should be registered");
      assertTrue((long) kafkaWrite.recordsIn.value() > 0, "Kafka write should have input records");

      LOGGER.info("Chunk assembly diagnostics report:\n{}", registry.generateReport());
    }
  }

  /**
   * Test that compression re-encoding stage metrics are captured when source and destination
   * compression strategies differ.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testCompressionReEncodeMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-compression");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create store with GZIP compression
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])
            .setCompressionStrategy(CompressionStrategy.GZIP)).close();

    // Batch push v1 with GZIP
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-compression-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // Change compression to NO_OP so repush triggers re-encoding (GZIP -> NO_OP)
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.NO_OP));
    }

    // KIF repush — exercises compression re-encoding (GZIP source -> NO_OP destination)
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-compression-repush", repushProps)) {
      repushJob.run();

      StageMetricsRegistry registry = repushJob.getStageMetricsRegistry();
      assertNotNull(registry, "StageMetricsRegistry should be available after compression repush");

      // Compression re-encode should be registered (GZIP -> NO_OP)
      StageMetrics compression = registry.getStage("compression_reencode");
      assertNotNull(compression, "Compression re-encode stage should be registered when strategies differ");
      assertTrue((long) compression.recordsIn.value() > 0, "Compression should have input records");
      assertEquals(
          (long) compression.recordsOut.value(),
          (long) compression.recordsIn.value(),
          "Compression re-encode should not drop records (1:1 transform)");
      // GZIP -> NO_OP means output bytes should be larger than input bytes (decompressed)
      assertTrue(
          (long) compression.bytesOut.value() > (long) compression.bytesIn.value(),
          "GZIP->NO_OP should increase byte size (decompression)");
      assertTrue((long) compression.timeNs.value() > 0, "Compression should track timing");

      LOGGER.info("Compression re-encode diagnostics report:\n{}", registry.generateReport());
    }
  }

  /**
   * Test the complete diagnostics report format — verify all stages appear in the report
   * and the report is parseable.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testFullDiagnosticsReport() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-full-report");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Simple batch store
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    // Batch push v1
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-report-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // Simple repush
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-report-repush", repushProps)) {
      repushJob.run();

      StageMetricsRegistry registry = repushJob.getStageMetricsRegistry();
      assertNotNull(registry, "StageMetricsRegistry should be available");

      String report = registry.generateReport();
      assertNotNull(report, "Report should not be null");
      assertTrue(report.contains("Stage"), "Report should have header");
      assertTrue(report.contains("Records In"), "Report should have Records In column");
      assertTrue(report.contains("kafka_write"), "Report should contain kafka_write stage");
      assertTrue(report.contains("compaction"), "Report should contain compaction stage for non-chunked repush");

      LOGGER.info("Full diagnostics report:\n{}", report);
    }
  }

  private void waitForVersion(String parentControllerUrl, String storeName, int expectedVersion) {
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        for (int version: controllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, expectedVersion, "All DCs should be on v" + expectedVersion);
        }
      });
    }
  }
}

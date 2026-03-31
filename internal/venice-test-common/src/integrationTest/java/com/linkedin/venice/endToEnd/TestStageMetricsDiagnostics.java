package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.spark.datawriter.task.StageMetricsSnapshot;
import com.linkedin.venice.spark.datawriter.task.StageMetricsSnapshot.StageSummary;
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
 * Integration tests for VPJ Spark pipeline stage diagnostics.
 * Two tests exercise different stage combinations:
 * - testAllStageMetrics: non-chunked store — compaction, compression re-encoding (NO_OP -> GZIP), kafka write
 * - testChunkedStageMetrics: chunked store — chunk assembly, kafka write
 */
public class TestStageMetricsDiagnostics extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestStageMetricsDiagnostics.class);
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int RECORD_COUNT = 10;

  private String[] dcNames;

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  /**
   * Single test that exercises stage metrics paths:
   * - Compression re-encode (NO_OP in Batch Push -> GZIP in re-push)
   * - Kafka write
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAllStageMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-all-stages");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create store: batch-only + NO_OP compression + AA + NR
    // Start with NO_OP compression so the repush can exercise compression re-encoding (NO_OP -> GZIP).
    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    batchProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
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
            .setCompressionStrategy(CompressionStrategy.NO_OP)).close();

    // Batch push v1 (non-chunked, NO_OP compression)
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-batch-v1", batchProps)) {
      batchPush.run();

      // --- Verify batch push stage metrics ---
      StageMetricsSnapshot batchSnapshot = batchPush.getStageMetricsSnapshot()
          .orElseThrow(() -> new AssertionError("StageMetricsSnapshot should be available after batch push"));

      // Batch push should have kafka_write stage
      StageSummary batchKafkaWrite = batchSnapshot.getStage("kafka_write");
      assertNotNull(batchKafkaWrite, "Kafka write stage should be registered for batch push");
      long batchKafkaWriteIn = batchKafkaWrite.getRecordsIn();
      assertTrue(
          batchKafkaWriteIn >= RECORD_COUNT,
          "Batch push kafka write recordsIn (" + batchKafkaWriteIn + ") should be at least " + RECORD_COUNT);
      LOGGER.info("Batch push kafka write bytesIn={}", batchKafkaWrite.getBytesIn());
      assertTrue(batchKafkaWrite.getTimeNs() > 0, "Batch push kafka write should track timing");
      // Note: kafka write is a sink — recordsOut and bytesOut are 0 because the partition writer
      // consumes all rows and returns an exhausted iterator.

      // Batch push should NOT have KIF-specific stages
      assertNull(batchSnapshot.getStage("chunk_assembly"), "Chunk assembly should NOT be registered for batch push");
      assertNull(batchSnapshot.getStage("compaction"), "Compaction should NOT be registered for batch push");
      assertNull(batchSnapshot.getStage("ttl_filter"), "TTL filter should NOT be registered for batch push");
      assertNull(
          batchSnapshot.getStage("compression_reencode"),
          "Compression re-encode should NOT be registered for batch push");

      LOGGER.info(
          "Batch push diagnostics: kafka_write recordsIn={}, bytesIn={}, timeNs={}",
          batchKafkaWriteIn,
          batchKafkaWrite.getBytesIn(),
          batchKafkaWrite.getTimeNs());
      LOGGER.info("Batch push diagnostics report:\n{}", batchSnapshot.getFormattedReport());
    }
    waitForVersion(parentControllerUrl, storeName, 1);

    // Change compression to GZIP so the repush triggers re-encoding (NO_OP -> GZIP)
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
    }

    // KIF repush — exercises chunk assembly + compression re-encode (NO_OP -> GZIP) + kafka write
    Properties repushProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-all-stages-repush", repushProps)) {
      repushJob.run();

      StageMetricsSnapshot snapshot = repushJob.getStageMetricsSnapshot()
          .orElseThrow(() -> new AssertionError("StageMetricsSnapshot should be available after Spark repush"));

      // --- Compaction (non-chunked store) ---
      StageSummary compaction = snapshot.getStage("compaction");
      assertNotNull(compaction, "Compaction stage should be registered for non-chunked repush");
      long compactIn = compaction.getRecordsIn();
      long compactOut = compaction.getRecordsOut();
      assertTrue(
          compactIn >= RECORD_COUNT,
          "Compaction recordsIn (" + compactIn + ") should be at least " + RECORD_COUNT);
      assertTrue(compactOut > 0, "Compaction should have output records");
      assertTrue(
          compactOut <= compactIn,
          "Compaction recordsOut (" + compactOut + ") should be <= recordsIn (" + compactIn + ")");
      // Byte size checks: compaction is no-op (no duplicates), so bytesIn == bytesOut
      long compactBytesIn = compaction.getBytesIn();
      long compactBytesOut = compaction.getBytesOut();
      assertTrue(compactBytesIn > 0, "Compaction bytesIn (" + compactBytesIn + ") should be > 0");
      assertEquals(
          compactBytesOut,
          compactBytesIn,
          "Compaction bytesOut (" + compactBytesOut + ") should equal bytesIn (" + compactBytesIn
              + ") because there are no duplicate keys");
      // Each record should be at least a few bytes (key + value + rmd)
      long avgBytesPerRecord = compactBytesIn / compactIn;
      assertTrue(avgBytesPerRecord > 0, "Average bytes per record (" + avgBytesPerRecord + ") should be > 0");

      // Chunk assembly should NOT be registered (chunking disabled)
      assertNull(
          snapshot.getStage("chunk_assembly"),
          "Chunk assembly should NOT be registered when chunking is disabled");

      // TTL filter is NOT registered because REPUSH_TTL_ENABLE is not set.
      assertNull(snapshot.getStage("ttl_filter"), "TTL filter should NOT be registered when TTL repush is disabled");

      // --- Compression re-encode (NO_OP -> GZIP) ---
      StageSummary compression = snapshot.getStage("compression_reencode");
      assertNotNull(compression, "Compression re-encode should be registered when strategies differ");
      long compressIn = compression.getRecordsIn();
      long compressOut = compression.getRecordsOut();
      assertEquals(
          compressIn,
          compactOut,
          "Compression recordsIn (" + compressIn + ") should equal compaction recordsOut (" + compactOut + ")");
      assertEquals(
          compressOut,
          compressIn,
          "Compression recordsOut (" + compressOut + ") should equal recordsIn (" + compressIn + ")");

      // Byte consistency: compression input bytes should match compaction output bytes
      // (same records flow from compaction to compression)
      long compressBytesIn = compression.getBytesIn();
      long compressBytesOut = compression.getBytesOut();
      assertEquals(
          compressBytesIn,
          compactBytesOut,
          "Compression bytesIn (" + compressBytesIn + ") should equal compaction bytesOut (" + compactBytesOut + ")");
      // NO_OP -> GZIP: for small payloads GZIP header overhead can make output larger than input,
      // so we only assert that compression produced output and rely on downstream byte consistency.
      assertTrue(compressBytesOut > 0, "Compression bytesOut (" + compressBytesOut + ") should be > 0");
      assertTrue(compression.getTimeNs() > 0, "Compression should track timing");

      // --- Kafka write (always present, sink — only input tracked) ---
      StageSummary kafkaWrite = snapshot.getStage("kafka_write");
      assertNotNull(kafkaWrite, "Kafka write stage should be registered");
      long kwIn = kafkaWrite.getRecordsIn();
      assertTrue(
          kwIn >= compressOut,
          "Kafka write recordsIn (" + kwIn + ") should be >= compression recordsOut (" + compressOut + ")");
      // Kafka write input bytes should match compression output bytes
      long kwBytesIn = kafkaWrite.getBytesIn();
      assertEquals(
          kwBytesIn,
          compressBytesOut,
          "Kafka write bytesIn (" + kwBytesIn + ") should equal compression bytesOut (" + compressBytesOut + ")");
      assertTrue(kafkaWrite.getTimeNs() > 0, "Kafka write should track timing");

      LOGGER.info(
          "Repush flow: compaction({} records, {} bytes in -> {} records, {} bytes out) "
              + "-> compression({} records, {} bytes in -> {} records, {} bytes out) "
              + "-> kafka_write({} records, {} bytes in)",
          compactIn,
          compactBytesIn,
          compactOut,
          compactBytesOut,
          compressIn,
          compressBytesIn,
          compressOut,
          compressBytesOut,
          kwIn,
          kwBytesIn);

      // --- Full diagnostics report ---
      String report = snapshot.getFormattedReport();
      assertNotNull(report, "Report should not be null");
      assertTrue(report.contains("Stage"), "Report should have header");
      assertTrue(report.contains("kafka_write"), "Report should contain kafka_write stage");
      assertTrue(report.contains("compaction"), "Report should contain compaction stage");
      assertTrue(report.contains("compression_reencode"), "Report should contain compression_reencode stage");

      LOGGER.info("Full diagnostics report (all stages):\n{}", report);
    }
  }

  /**
   * Test chunk assembly stage metrics with a chunked store.
   * Uses large values (1.5MB) that exceed the 950KB chunk threshold with NO_OP compression.
   * Verifies chunk assembly produces fewer output records than input (chunks assembled into records).
   *
   * Note: rmdChunkingEnabled is NOT set because batch push from HDFS produces records without RMD.
   * The chunk assembler would fail trying to decode empty RMD as a ChunkedValueManifest if enabled.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testChunkedStageMetrics() throws Exception {
    String storeName = Utils.getUniqueString("diagnostics-chunked");
    File inputDir = getTempDataDirectory();
    int recordCount = 5;
    int largeValueSize = 1_500_000; // 1.5MB, exceeds 950KB chunk threshold with NO_OP compression
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount, largeValueSize);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    Properties batchProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    batchProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setChunkingEnabled(true)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])
            .setCompressionStrategy(CompressionStrategy.NO_OP)).close();

    // Batch push v1 with chunking
    try (VenicePushJob batchPush = new VenicePushJob("diagnostics-chunked-batch-v1", batchProps)) {
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

    try (VenicePushJob repushJob = new VenicePushJob("diagnostics-chunked-repush", repushProps)) {
      repushJob.run();

      StageMetricsSnapshot snapshot = repushJob.getStageMetricsSnapshot()
          .orElseThrow(() -> new AssertionError("StageMetricsSnapshot should be available after chunked repush"));

      // Chunk assembly: multiple chunks per key assemble into one record
      StageSummary chunkAssembly = snapshot.getStage("chunk_assembly");
      assertNotNull(chunkAssembly, "Chunk assembly stage should be registered for chunked repush");
      assertTrue(
          chunkAssembly.getRecordsIn() > recordCount,
          "Chunk assembly recordsIn (" + chunkAssembly.getRecordsIn() + ") should exceed recordCount (" + recordCount
              + ") because values are chunked");
      assertEquals(
          chunkAssembly.getRecordsOut(),
          (long) recordCount,
          "Chunk assembly should output exactly " + recordCount + " assembled records");
      assertTrue(
          chunkAssembly.getRecordsIn() > chunkAssembly.getRecordsOut(),
          "Chunk assembly recordsIn should exceed recordsOut");
      assertTrue(chunkAssembly.getBytesIn() > 0, "Chunk assembly should track input bytes");
      assertTrue(chunkAssembly.getBytesOut() > 0, "Chunk assembly should track output bytes");

      // Compaction should NOT be registered (chunking skips compaction)
      assertNull(snapshot.getStage("compaction"), "Compaction should NOT be registered when chunking is enabled");

      // Kafka write should be present
      StageSummary kafkaWrite = snapshot.getStage("kafka_write");
      assertNotNull(kafkaWrite, "Kafka write stage should be registered");
      assertTrue(kafkaWrite.getRecordsIn() >= recordCount, "Kafka write recordsIn should be at least " + recordCount);
      assertTrue(kafkaWrite.getTimeNs() > 0, "Kafka write should track timing");

      LOGGER.info("Chunked repush report:\n{}", snapshot.getFormattedReport());
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

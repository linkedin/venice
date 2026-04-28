package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithCustomSize;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_COMBINER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.jobs.StageMetricsSnapshot.StageSummary;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class TestRepushDiagnostics extends AbstractTestRepush {
  /**
   * Baseline KIF repush: batch push then repush twice (combiner=true, combiner=false).
   * Exercises: compaction + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testKafkaInputBatchJob() throws Exception {
    String storeName = Utils.getUniqueString("repush-basic");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
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
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-basic-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Repush twice: combiner=true then combiner=false
    for (String combiner: new String[] { "true", "false" }) {
      Properties repushProps = buildRepushProps(storeName, inputDirPath);
      repushProps.setProperty(KAFKA_INPUT_COMBINER_ENABLED, combiner);
      try (VenicePushJob repushJob = new VenicePushJob("repush-basic-combiner-" + combiner, repushProps)) {
        repushJob.run();
        repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
          assertStage(snapshot, "compaction", 100, 100);
          assertSinkStage(snapshot, "kafka_write", 100);
          assertNull(snapshot.getStage("chunk_assembly"), "chunk_assembly should not be registered");
          assertNull(snapshot.getStage("compression_reencode"), "compression_reencode should not be registered");
          assertNull(snapshot.getStage("ttl_filter"), "ttl_filter should not be registered");
          LOGGER.info("Repush (combiner={}) metrics:\n{}", combiner, snapshot.getFormattedReport());
        });
      }
    }
    verifyBatchData(storeName, 100, 0);
  }

  /**
   * KIF repush with GZIP compression.
   * Exercises: compaction + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testCompressingRecordRepush() throws Exception {
    String storeName = Utils.getUniqueString("repush-gzip");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
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
            .setCompressionStrategy(CompressionStrategy.GZIP)).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-gzip-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Repush
    Properties repushProps = buildRepushProps(storeName, inputDirPath);
    try (VenicePushJob repushJob = new VenicePushJob("repush-gzip-repush", repushProps)) {
      repushJob.run();
      repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
        assertStage(snapshot, "compaction", 100, 100);
        assertSinkStage(snapshot, "kafka_write", 100);
        assertNull(snapshot.getStage("chunk_assembly"));
        assertNull(snapshot.getStage("compression_reencode"));
        assertNull(snapshot.getStage("ttl_filter"));
        LOGGER.info("GZIP repush metrics:\n{}", snapshot.getFormattedReport());
      });
    }
    waitForVersion(storeName, 2);
    verifyBatchData(storeName, 100, 0);
  }

  /**
   * KIF repush with ZSTD_WITH_DICT compression.
   * Exercises: compaction + compression_reencode + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 2)
  public void testZstdCompressionRepush() throws Exception {
    String storeName = Utils.getUniqueString("repush-zstd");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    // Batch push v1 (no compression)
    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    batchProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(3)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-zstd-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Enable ZSTD compression and push v2
    try (ControllerClient controllerClient =
        new ControllerClient(CLUSTER_NAME, multiRegionMultiClusterWrapper.getControllerConnectString())) {
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT));
    }

    try (VenicePushJob batchPush2 = new VenicePushJob("repush-zstd-batch-v2", batchProps)) {
      batchPush2.run();
    }
    waitForVersion(storeName, 2);

    // Repush from v2 (ZSTD)
    Properties repushProps = buildRepushProps(storeName, inputDirPath);
    try (VenicePushJob repushJob = new VenicePushJob("repush-zstd-repush-v3", repushProps)) {
      repushJob.run();
      repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
        assertStage(snapshot, "compaction", 100, 100);
        assertNotNull(snapshot.getStage("compression_reencode"), "compression_reencode should be registered");
        StageSummary compress = snapshot.getStage("compression_reencode");
        assertEquals(compress.getRecordsIn(), 100, "compression_reencode recordsIn");
        assertEquals(compress.getRecordsOut(), 100, "compression_reencode recordsOut");
        assertTrue(compress.getBytesIn() > 0, "compression_reencode bytesIn should be > 0");
        assertTrue(compress.getBytesOut() > 0, "compression_reencode bytesOut should be > 0");
        assertSinkStage(snapshot, "kafka_write", 100);
        assertNull(snapshot.getStage("chunk_assembly"));
        assertNull(snapshot.getStage("ttl_filter"));
        LOGGER.info("ZSTD repush metrics:\n{}", snapshot.getFormattedReport());
      });
    }
    waitForVersion(storeName, 3);
    verifyBatchData(storeName, 100, 0);
  }

  /**
   * KIF repush on AA hybrid store.
   * Exercises: compaction + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testKafkaInputAAStoreRepush() throws Exception {
    String storeName = Utils.getUniqueString("repush-aa");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
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
            .setHybridRewindSeconds(5)
            .setHybridOffsetLagThreshold(2)).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-aa-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Repush
    Properties repushProps = buildRepushProps(storeName, inputDirPath);
    try (VenicePushJob repushJob = new VenicePushJob("repush-aa-repush", repushProps)) {
      repushJob.run();
      repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
        assertStage(snapshot, "compaction", 100, 100);
        assertSinkStage(snapshot, "kafka_write", 100);
        assertNull(snapshot.getStage("chunk_assembly"));
        assertNull(snapshot.getStage("compression_reencode"));
        assertNull(snapshot.getStage("ttl_filter"));
        LOGGER.info("AA repush metrics:\n{}", snapshot.getFormattedReport());
      });
    }
    waitForVersion(storeName, 2);
    verifyBatchData(storeName, 100, 0);
  }

  /**
   * KIF repush with 1 record and 3 partitions. Tests that the first mapper gets only control
   * messages and that maybeSprayAllPartitions is NOT invoked.
   * Exercises: compaction + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testReducerCountValidation() throws Exception {
    String storeName = Utils.getUniqueString("repush-reducer-count");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir, 1);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    batchProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(3)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-reducer-count-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Repush with KAFKA_INPUT_MAX_RECORDS_PER_MAPPER=2 to force empty first mapper
    Properties repushProps = buildRepushProps(storeName, inputDirPath);
    repushProps.put(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "2");
    VeniceClusterWrapper dc0Cluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    repushProps.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, dc0Cluster.getPubSubBrokerWrapper().getAddress());
    try (VenicePushJob repushJob = new VenicePushJob("repush-reducer-count-repush", repushProps)) {
      repushJob.run();
      repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
        assertStage(snapshot, "compaction", 1, 1);
        assertSinkStage(snapshot, "kafka_write", 1);
        assertNull(snapshot.getStage("chunk_assembly"));
        assertNull(snapshot.getStage("compression_reencode"));
        assertNull(snapshot.getStage("ttl_filter"));
        LOGGER.info("Reducer count repush metrics:\n{}", snapshot.getFormattedReport());
      });
    }
    waitForVersion(storeName, 2);
    verifyBatchData(storeName, 1, 0);
  }

  /**
   * KIF repush with large (3MB) chunked values.
   * Exercises: chunk_assembly + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 2, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKafkaInputBatchJobWithLargeValues(boolean sendDirectControlMessage) throws Exception {
    int largeValueSize = 3 * 1024 * 1024; // 3 MB — exceeds 950KB chunk threshold
    int numberOfRecords = 5;
    String storeName = Utils.getUniqueString("repush-large-values");
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithCustomSize(inputDir, numberOfRecords, 0, largeValueSize);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    // Batch push v1 with chunking enabled
    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
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
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    try (VenicePushJob batchPush = new VenicePushJob("repush-large-batch-v1", batchProps)) {
      batchPush.run();
    }
    waitForVersion(storeName, 1);

    // Repush with chunking enabled + direct control messages
    Properties repushProps = buildRepushProps(storeName, inputDirPath);
    repushProps.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, String.valueOf(sendDirectControlMessage));
    try (VenicePushJob repushJob = new VenicePushJob("repush-large-repush", repushProps)) {
      repushJob.run();
      repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
        StageSummary chunkAssembly = snapshot.getStage("chunk_assembly");
        assertNotNull(chunkAssembly, "chunk_assembly should be registered for chunked repush");
        assertTrue(
            chunkAssembly.getRecordsIn() > numberOfRecords,
            "chunk_assembly recordsIn (" + chunkAssembly.getRecordsIn() + ") should exceed " + numberOfRecords);
        assertEquals(chunkAssembly.getRecordsOut(), numberOfRecords, "chunk_assembly recordsOut");
        assertTrue(chunkAssembly.getBytesIn() > 0, "chunk_assembly bytesIn should be > 0");
        assertTrue(chunkAssembly.getBytesOut() > 0, "chunk_assembly bytesOut should be > 0");
        assertSinkStage(snapshot, "kafka_write", numberOfRecords);
        assertNull(snapshot.getStage("compaction"), "compaction should not be registered when chunking is enabled");
        assertNull(snapshot.getStage("compression_reencode"));
        assertNull(snapshot.getStage("ttl_filter"));
        LOGGER.info("Chunked repush metrics:\n{}", snapshot.getFormattedReport());
      });
    }
    waitForVersion(storeName, 2);

    // Verify large values are present and correct
    VeniceClusterWrapper cluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        for (int i = 0; i < numberOfRecords; i++) {
          int expectedSize = largeValueSize / numberOfRecords * (i + 1);
          Object value = client.get(Integer.toString(i)).get();
          assertNotNull(value, "Key " + i + " is null after chunked repush");
          assertEquals(value.toString().length(), expectedSize, "Value size mismatch for key " + i);
        }
      });
    }
  }

  /**
   * Test KIF repush with both value chunking AND RMD chunking.
   * Streams partial updates to 3 keys, each accumulating ~2MB of float array data (value > 950KB threshold)
   * and ~4MB of per-element RMD timestamps (RMD > 950KB threshold). The repush chunk assembler must
   * reassemble both value chunks and RMD chunks correctly.
   * Exercises: chunk_assembly (value + RMD) + kafka_write stages.
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 3)
  public void testRepushWithValueAndRmdChunking() {
    String storeName = Utils.getUniqueString("repush-rmd-chunking");
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams storeParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setNativeReplicationEnabled(true)
              .setNativeReplicationSourceFabric(dcNames[0])
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      assertFalse(parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, storeParams)).isError());

      // Empty push v1 to bootstrap the store
      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Stream partial updates to 3 keys. Each key gets 50 updates x 10,000 floats = 500K floats.
      // Value: 500K floats x 4 bytes = ~2MB (exceeds 950KB chunk threshold)
      // RMD: 500K per-element timestamps x 8 bytes = ~4MB (exceeds 950KB chunk threshold)
      int numKeys = 3;
      int updatesPerKey = 50;
      int floatsPerUpdate = 10_000;
      VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

      try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
        for (int k = 0; k < numKeys; k++) {
          String key = "key_" + k;
          for (int u = 0; u < updatesPerKey; u++) {
            UpdateBuilderImpl updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
            updateBuilder.setNewFieldValue("name", "record_" + k);
            java.util.List<Float> newEntries = new java.util.ArrayList<>(floatsPerUpdate);
            for (int j = 0; j < floatsPerUpdate; j++) {
              newEntries.add((float) (u * floatsPerUpdate + j));
            }
            updateBuilder.setElementsToAddToListField("floatArray", newEntries);
            GenericRecord partialUpdateRecord = updateBuilder.build();
            sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord, (long) (u + 1));
          }
        }
      }

      // Verify data is ingested before repush (120s timeout: 150 partial updates take time to ingest)
      try (AvroGenericStoreClient<String, Object> reader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
          for (int k = 0; k < numKeys; k++) {
            Object value = reader.get("key_" + k).get();
            assertNotNull(value, "key_" + k + " should exist after streaming updates");
          }
        });
      }

      // KIF repush via Spark — exercises chunk_assembly for both value and RMD chunks
      Properties repushProps = buildRepushProps(storeName, "dummyInputPath");
      repushProps.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      try (VenicePushJob repushJob = new VenicePushJob("repush-rmd-chunking", repushProps)) {
        repushJob.run();
        repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
          StageSummary chunkAssembly = snapshot.getStage("chunk_assembly");
          assertNotNull(chunkAssembly, "chunk_assembly should be registered (both value and RMD are chunked)");
          // Input records include value chunks + RMD chunks + manifests, far exceeding numKeys
          assertTrue(
              chunkAssembly.getRecordsIn() > numKeys,
              "chunk_assembly recordsIn (" + chunkAssembly.getRecordsIn() + ") should exceed " + numKeys);
          assertEquals(chunkAssembly.getRecordsOut(), numKeys, "chunk_assembly recordsOut should equal " + numKeys);
          assertTrue(chunkAssembly.getBytesIn() > 0, "chunk_assembly bytesIn should be > 0");
          assertTrue(chunkAssembly.getBytesOut() > 0, "chunk_assembly bytesOut should be > 0");
          assertSinkStage(snapshot, "kafka_write", numKeys);
          assertNull(snapshot.getStage("compaction"));
          LOGGER.info("RMD chunking repush metrics:\n{}", snapshot.getFormattedReport());
        });
      }
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      // Verify data survives the repush
      try (AvroGenericStoreClient<String, Object> reader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int k = 0; k < numKeys; k++) {
            GenericRecord value = (GenericRecord) reader.get("key_" + k).get();
            assertNotNull(value, "key_" + k + " should survive repush");
            assertEquals(value.get("name").toString(), "record_" + k);
          }
        });
      }
    }
  }
}

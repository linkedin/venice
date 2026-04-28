package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRepushCore extends AbstractTestRepush {
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRepushWithChunkingFlagChanged(boolean useSpark) {
    final String storeName = Utils.getUniqueString("reproduce");
    String parentControllerUrl = getParentControllerUrl();
    Schema keySchema = AvroCompatibilityHelper.parse(loadFileAsString("UserKey.avsc"));
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("UserValue.avsc"));
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchema.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setHybridRewindSeconds(86400L)
              .setHybridOffsetLagThreshold(10L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

      GenericRecord keyRecord = new GenericData.Record(keySchema);
      keyRecord.put("learnerUrn", "urn:li:member:682787898");
      keyRecord.put("query", "python");
      GenericRecord checkpointKeyRecord = new GenericData.Record(keySchema);
      checkpointKeyRecord.put("learnerUrn", "urn:li:member:123");
      checkpointKeyRecord.put("query", "python");

      GenericRecord partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
          .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:751323"))
          .build();
      try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
        sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
      }

      // Perform one time repush to make sure repush can handle RMD chunks data correctly.
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, veniceCluster.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      if (useSpark) {
        props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
      }
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
            assertNotNull(value, "key " + keyRecord + " should not be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Enable chunking
        UpdateStoreQueryParams newUpdateStoreParams =
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setChunkingEnabled(true);
        updateStoreResponse =
            parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, newUpdateStoreParams));
        assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

        // Perform one time repush to make sure repush can handle chunks data correctly.
        // intentionally stop re-consuming from RT so stale records don't affect the testing results
        props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
        IntegrationTestPushUtils.runVPJ(props);

        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(storeName, 3),
            parentControllerClient,
            30,
            TimeUnit.SECONDS);

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
            assertNotNull(value, "key " + keyRecord + " should not be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
        try (
            VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
          partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
              .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:1"))
              .build();
          sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
          partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
              .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:2"))
              .build();
          sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
          sendStreamingRecord(veniceProducer, storeName, checkpointKeyRecord, partialUpdateRecord);
        }
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord value = (GenericRecord) storeReader.get(checkpointKeyRecord).get();
            assertNotNull(value, "key " + checkpointKeyRecord + " should not be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
            assertNotNull(value, "key " + keyRecord + " should not be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRepushWithDeleteRecord(boolean useSpark) {
    final String storeName = Utils.getUniqueString("testRepushWithDeleteRecord");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(1L)
              .setHybridOffsetLagThreshold(1L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    String STRING_MAP_FILED = "stringMap";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";

    UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k1", "v1"));
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 10000L);
      sendStreamingDeleteRecord(veniceProducer, storeName, key1, 10001L);
      sendStreamingRecord(veniceProducer, storeName, key2, updateBuilder.build(), 10001L);
    }

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    if (useSpark) {
      props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }
    IntegrationTestPushUtils.runVPJ(props);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
    int numberOfChildDatacenters = childDatacenters.size();
    for (int i = 0; i < numberOfChildDatacenters; i++) {
      VeniceClusterWrapper cluster = childDatacenters.get(i).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            // Key 1 is deleted.
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNull(valueRecord);

            // Key 2 is preserved.
            valueRecord = readValue(storeReader, key2);
            assertNotNull(valueRecord);
            assertNotNull(valueRecord.get(STRING_MAP_FILED));
            Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
            assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false")
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .build();

    MetricsRepository metricsRepository = new VeniceMetricsRepository();
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2ClientDC0,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Object> client =
          factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig().setStorageClass(StorageClass.DISK));
      client.subscribeAll().get();
    } catch (Exception e) {
      throw new VeniceException(e);
    }

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 9999L);
      sendStreamingRecord(veniceProducer, storeName, key3, updateBuilder.build(), 10000L);
    }

    for (int i = 0; i < numberOfChildDatacenters; i++) {
      VeniceClusterWrapper cluster = childDatacenters.get(i).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            // Key 1 is deleted.
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNull(valueRecord);

            // Key 3 is preserved.
            valueRecord = readValue(storeReader, key3);
            assertNotNull(valueRecord);
            assertNotNull(valueRecord.get(STRING_MAP_FILED));
            Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
            assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 12345L);
    }

    for (int i = 0; i < numberOfChildDatacenters; i++) {
      VeniceClusterWrapper cluster = childDatacenters.get(i).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            // Key 1 is preserved.
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertNotNull(valueRecord.get(STRING_MAP_FILED));
            Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
            assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  /**
   * Test that a batch-store repush correctly reads data from a cross-fabric input (dc-1) when
   * the NR source is dc-0. Parameterized for both Spark and Hadoop MR compute engines.
   *
   * <p><b>Setup:</b> A batch-only store with NR enabled, AA enabled, NR source = dc-0.
   * After a v1 batch push, we repush from dc-1 (KAFKA_INPUT_FABRIC = dc-1).
   *
   * <p><b>Key verification:</b>
   * <ul>
   *   <li>The VenicePushJob's repushSourcePubsubBroker should resolve to dc-1's broker, confirming
   *       the consume fabric was correctly set.</li>
   *   <li>All records should be present in both DCs after repush, confirming that data was
   *       correctly produced to the NR source (dc-0) and replicated.</li>
   * </ul>
   *
   * <p><b>Expected outcome:</b> Both MR and Spark variants should pass.
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBatchStoreRepushWithCrossFabricInput(boolean useSpark) throws Exception {
    String engine = useSpark ? "Spark" : "MR";
    String storeName = Utils.getUniqueString("batch-repush-xfabric-" + engine.toLowerCase());
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();

    /*
     * Step 1: Write 50 simple avro records to a temp directory.
     * Keys are "1".."50", values are "test_name_1".."test_name_50".
     */
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 50);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    VeniceMultiClusterWrapper dc0 = childDatacenters.get(0);
    VeniceMultiClusterWrapper dc1 = childDatacenters.get(1);
    String dc0KafkaUrl = dc0.getPubSubBrokerWrapper().getAddress();
    String dc1KafkaUrl = dc1.getPubSubBrokerWrapper().getAddress();
    assertNotEquals(dc0KafkaUrl, dc1KafkaUrl, "DCs must have different Kafka brokers for this test");

    /*
     * Step 2: Create a batch-only store with NR enabled, AA enabled, NR source = dc-0.
     * No hybrid config is set — this is a pure batch store.
     */
    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
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

    /*
     * Step 3: Batch push v1 and verify data is present in both DCs.
     */
    try (VenicePushJob batchPush = new VenicePushJob("batch-push-v1-" + engine, batchProps)) {
      batchPush.run();
    }
    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentControllerUrls)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 1, "All DCs should be on v1 after batch push");
        }
      });
    }
    verifyBatchData(storeName, 50, 0);
    verifyBatchData(storeName, 50, 1);

    /*
     * Step 4: Repush from dc-1 (cross-fabric). The controller should resolve KAFKA_INPUT_FABRIC
     * to dc-1's broker URL. When useSpark=true, DataWriterSparkJob is used.
     *
     * We use a direct VenicePushJob (not runVPJ) so we can inspect getRepushSourcePubsubBroker()
     * to verify the consume-fabric was correctly resolved to dc-1.
     */
    Properties repushProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[1]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    if (useSpark) {
      repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }

    try (VenicePushJob repushJob = new VenicePushJob(engine + "-repush-from-dc1", repushProps)) {
      repushJob.run();

      /*
       * Step 5: Verify consume fabric. The repushSourcePubsubBroker should point to dc-1's
       * broker, confirming that the repush reads data from dc-1 (not dc-0).
       */
      assertEquals(
          repushJob.getRepushSourcePubsubBroker(),
          dc1KafkaUrl,
          "Repush should consume from dc-1 (KAFKA_INPUT_FABRIC=" + dcNames[1] + "), "
              + "but repushSourcePubsubBroker points elsewhere");
      if (useSpark) {
        repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
          assertStage(snapshot, "compaction", 50, 50);
          assertSinkStage(snapshot, "kafka_write", 50);
          assertNull(snapshot.getStage("chunk_assembly"));
          assertNull(snapshot.getStage("compression_reencode"));
          assertNull(snapshot.getStage("ttl_filter"));
          LOGGER.info("Cross-fabric batch repush metrics:\n{}", snapshot.getFormattedReport());
        });
      }
    }

    /*
     * Step 6: Wait for v2 to become current in all DCs and verify data.
     * If the produce fabric is correct (NR source = dc-0), all records should be
     * present in both DCs after replication.
     */
    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentControllerUrls)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 2, "All DCs should be on v2 after repush");
        }
      });
    }
    verifyBatchData(storeName, 50, 0);
    verifyBatchData(storeName, 50, 1);
    LOGGER.info("[{}] testBatchStoreRepushWithCrossFabricInput passed", engine);
  }

  /**
   * Test that a hybrid-store repush correctly reads data from a cross-fabric input (dc-1)
   * by verifying at the data level that only dc-1's data appears in the repushed version.
   * Parameterized for both Spark and Hadoop MR compute engines.
   *
   * <p><b>Test design:</b> The store is created with NR enabled but AA disabled. After batch
   * push v1, we send new streaming records ONLY to dc-0. Without AA, these records do NOT
   * replicate to dc-1. We then repush from dc-1 with REWIND_TIME_IN_SECONDS_OVERRIDE=0
   * (no RT consumption). If the repush correctly reads from dc-1, the dc-0-only streaming
   * records should be ABSENT from v2 — proving the data was sourced from dc-1.
   *
   * <p><b>Expected outcome:</b> Both MR and Spark variants should pass.
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHybridStoreRepushWithCrossFabricInput(boolean useSpark) throws Exception {
    String engine = useSpark ? "Spark" : "MR";
    String storeName = Utils.getUniqueString("hybrid-repush-xfabric-" + engine.toLowerCase());
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();

    /*
     * Step 1: Write 50 simple avro records to a temp directory.
     * Keys are "1".."50", values are "test_name_1".."test_name_50".
     */
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 50);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    VeniceMultiClusterWrapper dc0 = childDatacenters.get(0);
    VeniceMultiClusterWrapper dc1 = childDatacenters.get(1);
    String dc0KafkaUrl = dc0.getPubSubBrokerWrapper().getAddress();
    String dc1KafkaUrl = dc1.getPubSubBrokerWrapper().getAddress();
    assertNotEquals(dc0KafkaUrl, dc1KafkaUrl, "DCs must have different Kafka brokers for this test");

    /*
     * Step 2: Create a hybrid store with NR enabled but AA DISABLED, NR source = dc-0.
     * AA is intentionally off so that streaming records sent to dc-0 do NOT replicate
     * to dc-1, creating the data asymmetry needed to verify consume-fabric correctness.
     * hybridRewindSeconds and hybridOffsetLagThreshold are set to make it a hybrid store.
     */
    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    createStoreForJob(
        CLUSTER_NAME,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(2)
            .setNativeReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])
            .setHybridRewindSeconds(86400L)
            .setHybridOffsetLagThreshold(10L)).close();

    /*
     * Step 3: Batch push v1 and verify data is present in both DCs.
     */
    try (VenicePushJob batchPush = new VenicePushJob("hybrid-batch-push-v1-" + engine, batchProps)) {
      batchPush.run();
    }
    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentControllerUrls)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 1, "All DCs should be on v1 after batch push");
        }
      });
    }
    verifyBatchData(storeName, 50, 0);
    verifyBatchData(storeName, 50, 1);

    /*
     * Step 4: Send new streaming records ONLY to dc-0. These records use keys "51".."60"
     * which are NOT in the original batch data (keys "1".."50"). Since AA is disabled
     * (the store was created without it), these records will exist only in dc-0's
     * real-time topic and will be consumed only by dc-0's storage nodes.
     */
    VeniceClusterWrapper dc0Cluster = dc0.getClusters().get(CLUSTER_NAME);
    try (VeniceSystemProducer dc0Producer = getSamzaProducer(dc0Cluster, storeName, Version.PushType.STREAM)) {
      for (int i = 51; i <= 60; i++) {
        sendStreamingRecord(dc0Producer, storeName, Integer.toString(i), "stream_value_" + i);
      }
    }

    /*
     * Step 5: Wait until dc-0 has the new streaming records. Query dc-0's router to confirm.
     */
    VeniceClusterWrapper dc0ClusterForRead = dc0.getClusters().get(CLUSTER_NAME);
    try (AvroGenericStoreClient<String, Object> dc0Reader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0ClusterForRead.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Object val = dc0Reader.get("55").get();
        assertNotNull(val, "dc-0 should have streaming record for key 55");
      });
    }

    /*
     * Step 6: Confirm dc-1 does NOT have the new streaming records.
     * Since AA is disabled, the records sent to dc-0 should not have replicated to dc-1.
     */
    VeniceClusterWrapper dc1Cluster = dc1.getClusters().get(CLUSTER_NAME);
    try (AvroGenericStoreClient<String, Object> dc1Reader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc1Cluster.getRandomRouterURL()))) {
      for (int i = 51; i <= 60; i++) {
        Object val = dc1Reader.get(Integer.toString(i)).get();
        assertNull(val, "dc-1 should NOT have streaming record for key " + i + " (AA is disabled)");
      }
    }

    /*
     * Step 7: Repush from dc-1 — the colo WITHOUT the new streaming records.
     * REWIND_TIME_IN_SECONDS_OVERRIDE=0 ensures no RT consumption, so the repush only gets
     * VT (Version Topic) data from dc-1. If the repush correctly reads from dc-1, the
     * dc-0-only streaming records (keys 51-60) should NOT appear in v2.
     */
    Properties repushProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[1]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    repushProps.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    if (useSpark) {
      repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }

    try (VenicePushJob repushJob = new VenicePushJob(engine + "-hybrid-repush-from-dc1", repushProps)) {
      repushJob.run();

      /*
       * Step 8: Verify consume fabric. The repushSourcePubsubBroker should point to dc-1's
       * broker, confirming that the repush reads data from dc-1.
       */
      assertEquals(
          repushJob.getRepushSourcePubsubBroker(),
          dc1KafkaUrl,
          "Repush should consume from dc-1 (KAFKA_INPUT_FABRIC=" + dcNames[1] + "), "
              + "but repushSourcePubsubBroker points elsewhere");
      if (useSpark) {
        repushJob.getStageMetricsSnapshot().ifPresent(snapshot -> {
          assertStage(snapshot, "compaction", 50, 50);
          assertSinkStage(snapshot, "kafka_write", 50);
          assertNull(snapshot.getStage("chunk_assembly"));
          assertNull(snapshot.getStage("compression_reencode"));
          assertNull(snapshot.getStage("ttl_filter"));
          LOGGER.info("Cross-fabric hybrid repush metrics:\n{}", snapshot.getFormattedReport());
        });
      }
    }

    /*
     * Step 9: Wait for v2 to become current in all DCs.
     */
    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentControllerUrls)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 2, "All DCs should be on v2 after repush");
        }
      });
    }

    /*
     * Step 10: Verify the dc-0-only streaming records (keys 51-60) are ABSENT from v2.
     * This proves the repush read from dc-1 (which doesn't have them), not from dc-0.
     * We check both DCs to be thorough.
     */
    for (int dcIndex = 0; dcIndex < childDatacenters.size(); dcIndex++) {
      VeniceClusterWrapper cluster = childDatacenters.get(dcIndex).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<String, Object> reader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        int dc = dcIndex;
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          for (int i = 51; i <= 60; i++) {
            Object val = reader.get(Integer.toString(i)).get();
            assertNull(
                val,
                "Key " + i + " (dc-0-only streaming record) should be ABSENT in dc-" + dc
                    + " after repush from dc-1. Its presence would mean the repush read from "
                    + "dc-0 instead of dc-1.");
          }
        });
      }
    }

    /*
     * Step 11: Verify the original batch records (keys 1-50) ARE present in both DCs.
     * This confirms the repush successfully transferred the batch data.
     */
    verifyBatchData(storeName, 50, 0);
    verifyBatchData(storeName, 50, 1);
    LOGGER.info("[{}] testHybridStoreRepushWithCrossFabricInput passed", engine);
  }
}

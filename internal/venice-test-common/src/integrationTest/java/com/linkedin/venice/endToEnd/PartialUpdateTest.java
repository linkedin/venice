package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.stats.HostLevelIngestionStats.ASSEMBLED_RMD_SIZE_IN_BYTES;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.IntegrationTestChunkingUtils.getChunkValueManifest;
import static com.linkedin.venice.utils.IntegrationTestChunkingUtils.validateChunksFromManifests;
import static com.linkedin.venice.utils.IntegrationTestChunkingUtils.validateRmdData;
import static com.linkedin.venice.utils.IntegrationTestChunkingUtils.validateValueChunks;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithoutFlush;
import static com.linkedin.venice.utils.IntegrationTestReadUtils.readValue;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToUserWithStringMapSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class includes tests on A/A partial update core functionality with batch data and compression.
 */
public class PartialUpdateTest extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Override
  protected Properties getExtraServerProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, "true");
    return properties;
  }

  /**
   * This integration test verifies that in A/A + partial update enabled store, UPDATE on a key that was written in the
   * batch push should not throw exception, as the update logic should initialize a new RMD record for the original value
   * and apply updates on top of them.
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class)
  public void testPartialUpdateOnBatchPushedKeys(CompressionStrategy compressionStrategy) throws IOException {
    final String storeName = Utils.getUniqueString("updateBatch");
    String parentControllerUrl = getParentControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);

    Schema valueSchema = AvroCompatibilityHelper.parse(valueSchemaStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    VeniceClusterWrapper veniceClusterWrapper = getClusterDC0();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(compressionStrategy)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        IntegrationTestPushUtils.runVPJ(vpjProperties, 1, childControllerClient);
      }
      veniceClusterWrapper.waitVersion(storeName, 1);
      // Produce partial updates on batch pushed keys
      try (VeniceSystemProducer veniceProducer =
          getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM)) {
        for (int i = 1; i < 100; i++) {
          GenericRecord partialUpdateRecord =
              new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("firstName", "new_name_" + i).build();
          sendStreamingRecord(veniceProducer, storeName, String.valueOf(i), partialUpdateRecord);
        }
      }

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "new_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testActiveActivePartialUpdateOnBatchPushedChunkKeys() throws IOException {
    final String storeName = Utils.getUniqueString("updateBatch");
    String parentControllerUrl = getParentControllerUrl();
    File inputDir = getTempDataDirectory();
    int mapItemPerRecord = 1000;
    Schema recordSchema = writeSimpleAvroFileWithStringToUserWithStringMapSchema(inputDir, mapItemPerRecord);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);

    Schema valueSchema = AvroCompatibilityHelper.parse(valueSchemaStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    VeniceClusterWrapper veniceClusterWrapper = getClusterDC0();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        IntegrationTestPushUtils.runVPJ(vpjProperties, 1, childControllerClient);
      }
      veniceClusterWrapper.waitVersion(storeName, 1);
      // Produce partial updates on batch pushed keys
      try (VeniceSystemProducer veniceProducer =
          getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM)) {
        for (int i = 1; i < 100; i++) {
          GenericRecord partialUpdateRecord =
              new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("key", "new_name_" + i).build();
          sendStreamingRecord(veniceProducer, storeName, String.valueOf(i), partialUpdateRecord);
        }
      }

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("key").toString(), "new_name_" + key);

              assertEquals(((Map) value.get("value")).size(), mapItemPerRecord);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  /**
   * This integration test performs a few actions to test RMD chunking logic:
   * (1) Send a bunch of large UPDATE messages to make sure eventually the key's value + RMD size greater than 1MB and
   * thus trigger chunking / RMD chunking.
   * (2) Run a KIF re-push to make sure it handles RMD chunks correctly.
   * (3) Send a DELETE message to partially delete some items in the map field.
   * (4) Send a DELETE message to fully delete the record.
   */
  @Test(timeOut = TEST_TIMEOUT_MS
      * 3, dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class)
  public void testActiveActivePartialUpdateWithCompression(CompressionStrategy compressionStrategy) throws Exception {
    final String storeName = Utils.getUniqueString("rmdChunking");
    String parentControllerUrl = getParentControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(compressionStrategy)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      assertTrue(parentControllerClient.getStore(storeName).getStore().isRmdChunkingEnabled());
      assertTrue(parentControllerClient.getStore(storeName).getStore().getVersion(1).get().isRmdChunkingEnabled());
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();

    String key = "key1";
    String primitiveFieldName = "name";
    String listFieldName = "floatArray";

    int totalUpdateCount = 40;
    // Insert large amount of Map entries to trigger RMD chunking.
    int singleUpdateEntryCount = 10000;
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < (totalUpdateCount - 1); i++) {
        producePartialUpdateToArray(
            storeName,
            veniceProducer,
            partialUpdateSchema,
            key,
            primitiveFieldName,
            listFieldName,
            singleUpdateEntryCount,
            i);
      }
      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          assertEquals(
              ((List<Float>) (valueRecord.get(listFieldName))).size(),
              (totalUpdateCount - 1) * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
      validateValueChunks(multiRegionMultiClusterWrapper, CLUSTER_NAME, kafkaTopic_v1, key, Assert::assertNotNull);
      VeniceServerWrapper serverWrapper = multiRegionMultiClusterWrapper.getChildRegions()
          .get(0)
          .getClusters()
          .get(CLUSTER_NAME)
          .getVeniceServers()
          .get(0);
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic_v1);
      ChunkedValueManifest valueManifest = getChunkValueManifest(storageEngine, 0, key, false);
      ChunkedValueManifest rmdManifest = getChunkValueManifest(storageEngine, 0, key, true);

      producePartialUpdateToArray(
          storeName,
          veniceProducer,
          partialUpdateSchema,
          key,
          primitiveFieldName,
          listFieldName,
          singleUpdateEntryCount,
          totalUpdateCount - 1);

      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          assertEquals(
              ((List<Float>) (valueRecord.get(listFieldName))).size(),
              totalUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
      // Validate RMD bytes after PUT requests.
      // Use waitForNonDeterministicAssertion because RMD is read directly from storage engine
      // which may not be in sync with the router-served value read above.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        validateRmdData(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            rmdSerDe,
            kafkaTopic_v1,
            key,
            rmdWithValueSchemaId -> {
              GenericRecord timestampRecord =
                  (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
              GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
              List<Long> activeElementsTimestamps =
                  (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
              assertEquals(activeElementsTimestamps.size(), totalUpdateCount * singleUpdateEntryCount);
            });
      });
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        Assert.assertNotNull(valueManifest);
        Assert.assertNotNull(rmdManifest);
        validateChunksFromManifests(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            kafkaTopic_v1,
            0,
            valueManifest,
            rmdManifest,
            (valueChunkBytes, rmdChunkBytes) -> {
              Assert.assertNull(valueChunkBytes);
              Assert.assertNotNull(rmdChunkBytes);
              // Assert.assertEquals(rmdChunkBytes.length, 4);
            },
            true);
      });

      // For now, repush with large ZSTD dictionary will fail as the size exceeds max request size.
      if (compressionStrategy.equals(CompressionStrategy.ZSTD_WITH_DICT)) {
        return;
      }

      // <!--- Perform one time repush to make sure repush can handle RMD chunks data correctly -->
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      IntegrationTestPushUtils.runVPJ(props);

      ControllerClient controllerClient =
          new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));
      veniceCluster.refreshAllRouterMetaData();

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          assertEquals(
              ((List<Float>) (valueRecord.get(listFieldName))).size(),
              totalUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
      // Validate RMD bytes after PUT requests.
      String kafkaTopic_v2 = Version.composeKafkaTopic(storeName, 2);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        validateRmdData(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            rmdSerDe,
            kafkaTopic_v2,
            key,
            rmdWithValueSchemaId -> {
              GenericRecord timestampRecord =
                  (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
              GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
              List<Long> activeElementsTimestamps =
                  (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
              assertEquals(activeElementsTimestamps.size(), totalUpdateCount * singleUpdateEntryCount);
            });
      });

      // Send DELETE record that partially removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, (totalUpdateCount - 1) * 10L);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertFalse(nullRecord);
        assertEquals(((List<Float>) (valueRecord.get(listFieldName))).size(), singleUpdateEntryCount);
      });

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        validateRmdData(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            rmdSerDe,
            kafkaTopic_v2,
            key,
            rmdWithValueSchemaId -> {
              GenericRecord timestampRecord =
                  (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
              GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
              List<Long> activeElementsTimestamps =
                  (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
              assertEquals(activeElementsTimestamps.size(), singleUpdateEntryCount);
              List<Long> deletedElementsTimestamps =
                  (List<Long>) collectionFieldTimestampRecord.get(DELETED_ELEM_TS_FIELD_NAME);
              assertEquals(deletedElementsTimestamps.size(), 0);
            });
      });

      // Send DELETE record that fully removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, totalUpdateCount * 10L);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertTrue(nullRecord);
      });
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        validateRmdData(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            rmdSerDe,
            kafkaTopic_v2,
            key,
            rmdWithValueSchemaId -> {
              Assert.assertTrue(rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
              GenericRecord timestampRecord =
                  (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
              GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
              assertEquals(collectionFieldTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME), (long) (totalUpdateCount) * 10);
            });
      });
    }

    String metricName = AbstractVeniceStats.getSensorFullName(storeName, ASSEMBLED_RMD_SIZE_IN_BYTES) + ".Max";
    double assembledRmdSize = MetricsUtils.getMax(metricName, veniceCluster.getVeniceServers());
    assertTrue(assembledRmdSize >= 290000 && assembledRmdSize <= 740000);
  }

  /**
   * Verifies that orphan chunks from previous updates are properly deleted when multiple same-key
   * updates arrive in the same consumer poll and are processed by IngestionBatchProcessor.
   *
   * The fix under test is {@code linkBackManifestFromTransientRecord}: when the 1st batch update
   * produces a new chunked value (M2), the 2nd batch update must know about M2's chunks so it can
   * delete them. Without the fix, only M1's chunks (from before the batch) would be deleted; with
   * the fix, M2's intermediate chunks are also deleted.
   *
   * We verify this by consuming the version topic and counting chunk DELETE records produced during
   * the batch window. With the fix, the total chunk DELETEs must exceed M1's chunk count (because
   * M2's chunks are also deleted by the 2nd update).
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 3)
  public void testBatchProcessorOrphanChunkDeletion() throws Exception {
    final String storeName = Utils.getUniqueString("orphanChunk");
    String parentControllerUrl = getParentControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();

    String key = "key1";
    String primitiveFieldName = "name";
    String listFieldName = "floatArray";
    // Each update adds 10000 floats (~40KB). We need enough updates so the assembled value exceeds
    // the 950KB chunking threshold. 30 updates * 10000 floats * 4 bytes = ~1.2MB, safely above threshold.
    int singleUpdateEntryCount = 10000;
    int initialUpdateCount = 30;

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Step 1: Send enough flushed updates to build a chunked value.
      for (int i = 0; i < initialUpdateCount; i++) {
        producePartialUpdateToArray(
            storeName,
            veniceProducer,
            partialUpdateSchema,
            key,
            primitiveFieldName,
            listFieldName,
            singleUpdateEntryCount,
            i);
      }

      // Verify all initial updates are visible.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          assertNotNull(valueRecord, "Value should not be null after initial updates");
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham");
          assertEquals(
              ((List<Float>) (valueRecord.get(listFieldName))).size(),
              initialUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 2: Capture old manifests (M1) — value should now be chunked.
      String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
      VeniceServerWrapper serverWrapper = multiRegionMultiClusterWrapper.getChildRegions()
          .get(0)
          .getClusters()
          .get(CLUSTER_NAME)
          .getVeniceServers()
          .get(0);
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic_v1);
      ChunkedValueManifest oldValueManifest = getChunkValueManifest(storageEngine, 0, key, false);
      ChunkedValueManifest oldRmdManifest = getChunkValueManifest(storageEngine, 0, key, true);
      assertNotNull(oldValueManifest, "Value should be chunked after initial updates exceed chunk threshold");
      int m1ChunkCount = oldValueManifest.keysWithChunkIdSuffix.size()
          + (oldRmdManifest == null ? 0 : oldRmdManifest.keysWithChunkIdSuffix.size());

      // Step 3: Record the VT end position before the batch so we can isolate batch-produced records.
      PubSubBrokerWrapper pubSubBrokerWrapper = veniceCluster.getPubSubBrokerWrapper();
      PubSubTopicPartition vtPartition =
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(kafkaTopic_v1), 0);
      Properties consumerProps = new Properties();
      consumerProps.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
      PubSubPosition preBatchVtEndPosition;
      try (PubSubConsumerAdapter offsetConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
          .getConsumerAdapterFactory()
          .create(
              new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(consumerProps))
                  .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                  .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                  .setConsumerName("offsetProbe")
                  .build())) {
        preBatchVtEndPosition =
            offsetConsumer.endPositions(Collections.singletonList(vtPartition), Duration.ofSeconds(10))
                .get(vtPartition);
        assertNotNull(preBatchVtEndPosition, "VT should have records after initial updates");
      }

      // Step 4: Send 2 updates WITHOUT flush — they buffer in the Samza producer.
      producePartialUpdateToArrayWithoutFlush(
          storeName,
          veniceProducer,
          partialUpdateSchema,
          key,
          primitiveFieldName,
          listFieldName,
          singleUpdateEntryCount,
          initialUpdateCount);
      producePartialUpdateToArrayWithoutFlush(
          storeName,
          veniceProducer,
          partialUpdateSchema,
          key,
          primitiveFieldName,
          listFieldName,
          singleUpdateEntryCount,
          initialUpdateCount + 1);

      // Step 5: Flush — both records hit Kafka RT together, enter IngestionBatchProcessor path.
      veniceProducer.flush(storeName);

      // Step 6: Verify client sees the cumulative result of all updates.
      int totalUpdateCount = initialUpdateCount + 2;
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          assertNotNull(valueRecord, "Value should not be null after all updates");
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham");
          assertEquals(
              ((List<Float>) (valueRecord.get(listFieldName))).size(),
              totalUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 7: Consume VT from the pre-batch offset and count chunk DELETE records.
      // VeniceWriter produces a DELETE to the VT for each old chunk key it cleans up.
      // With the fix: both batch updates delete old chunks (M1 + intermediate M2).
      // Without the fix: only the first batch update deletes old chunks (M1 only).
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        int chunkDeleteCount = 0;
        try (PubSubConsumerAdapter vtConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
            .getConsumerAdapterFactory()
            .create(
                new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(consumerProps))
                    .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                    .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                    .setConsumerName("chunkDeleteCounter")
                    .build())) {
          vtConsumer.subscribe(vtPartition, preBatchVtEndPosition);
          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = vtConsumer.poll(30 * Time.MS_PER_SECOND);
          for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
            for (DefaultPubSubMessage msg: entry.getValue()) {
              if (msg.getKey().isControlMessage()) {
                continue;
              }
              KafkaMessageEnvelope envelope = msg.getValue();
              if (MessageType.valueOf(envelope) == MessageType.DELETE) {
                chunkDeleteCount++;
              }
            }
          }
        }
        // With the fix, there must be MORE chunk DELETEs than just M1's chunks, because the 2nd
        // batch update also deletes the intermediate M2's chunks via linkBackManifestFromTransientRecord.
        assertTrue(
            chunkDeleteCount > m1ChunkCount,
            "Expected chunk DELETE count (" + chunkDeleteCount + ") to exceed M1 chunk count (" + m1ChunkCount
                + "). The 2nd batch update should also delete intermediate chunks from the 1st batch update.");
      });
    }
  }

  private void producePartialUpdateToArrayWithoutFlush(
      String storeName,
      SystemProducer veniceProducer,
      Schema partialUpdateSchema,
      String key,
      String primitiveFieldName,
      String arrayField,
      int singleUpdateEntryCount,
      int updateCount) {
    UpdateBuilderImpl updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(primitiveFieldName, "Tottenham");
    List<Float> newEntries = new ArrayList<>();
    for (int j = 0; j < singleUpdateEntryCount; j++) {
      float value = (float) (updateCount * singleUpdateEntryCount + j);
      newEntries.add(value);
    }
    updateBuilder.setElementsToAddToListField(arrayField, newEntries);
    GenericRecord partialUpdateRecord = updateBuilder.build();
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, partialUpdateRecord, updateCount * 10L + 1);
  }

  private void producePartialUpdateToArray(
      String storeName,
      SystemProducer veniceProducer,
      Schema partialUpdateSchema,
      String key,
      String primitiveFieldName,
      String arrayField,
      int singleUpdateEntryCount,
      int updateCount) {
    UpdateBuilderImpl updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(primitiveFieldName, "Tottenham");
    List<Float> newEntries = new ArrayList<>();
    for (int j = 0; j < singleUpdateEntryCount; j++) {
      float value = (float) (updateCount * singleUpdateEntryCount + j);
      newEntries.add(value);
    }
    updateBuilder.setElementsToAddToListField(arrayField, newEntries);
    GenericRecord partialUpdateRecord = updateBuilder.build();
    sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord, updateCount * 10L + 1);
  }
}

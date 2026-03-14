package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.metrics.MetricsRepository;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRepush extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int ASSERTION_TIMEOUT_MS = 30_000;

  @Override
  protected boolean shouldCreateD2Client() {
    return true;
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testRepushWithChunkingFlagChanged() {
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
          30,
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
      props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
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

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class)
  public void testRepushWithTTLWithActiveActivePartialUpdateStore(CompressionStrategy compressionStrategy) {
    final String storeName = Utils.getUniqueString("ttlRepushAAWC");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(compressionStrategy)
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
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    long STALE_TS = 99999L;
    long FRESH_TS = 100000L;
    String STRING_MAP_FILED = "stringMap";
    String REGULAR_FIELD = "name";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String key4 = "key4";
    String key5 = "key5";

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      /**
       * Case 1: The record is partially stale, TTL repush should only keep the part that's fresh.
       */

      // This update is expected to be carried into TTL repush.
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k1", "v1"));
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), FRESH_TS);
      // This update is expected to be WIPED OUT after TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name");
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k2", "v2"));
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), STALE_TS);

      /**
       * Case 2: The record is fully stale, TTL repush should drop the record.
       */

      // This update is expected to be WIPED OUT after TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_2");
      sendStreamingRecord(veniceProducer, storeName, key2, updateBuilder.build(), STALE_TS);

      /**
       * Case 3: The record is fully fresh, TTL repush should keep the record.
       */

      // This update is expected to be carried into TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_3");
      sendStreamingRecord(veniceProducer, storeName, key3, updateBuilder.build(), FRESH_TS);

      /**
       * Case 4: The delete record is fully fresh, TTL repush should NOT FAIL due to this record.
       */

      sendStreamingRecord(veniceProducer, storeName, key4, updateBuilder.build(), FRESH_TS);
      sendStreamingDeleteRecord(veniceProducer, storeName, key4, FRESH_TS + 1);

      /**
       * Case 5: The delete record is stale, TTL repush should keep new update.
       */

      sendStreamingDeleteRecord(veniceProducer, storeName, key5, STALE_TS);
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_5");
      sendStreamingRecord(veniceProducer, storeName, key5, updateBuilder.build(), FRESH_TS);

    }

    /**
     * Validate the data is ready in storage before TTL repush.
     */
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key1);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name"));
          assertNotNull(valueRecord.get(STRING_MAP_FILED));
          Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
          assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          assertEquals(stringMapValue.get(new Utf8("k2")), new Utf8("v2"));

          valueRecord = readValue(storeReader, key2);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_2"));

          valueRecord = readValue(storeReader, key3);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_3"));

          valueRecord = readValue(storeReader, key4);
          assertNull(valueRecord);

          valueRecord = readValue(storeReader, key5);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_5"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
    // Validate RMD bytes after initial update requests.
    validateRmdData(rmdSerDe, kafkaTopic_v1, key1, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FILED);
      Assert.assertEquals(stringMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 0L);
      Assert.assertEquals(stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(STALE_TS, FRESH_TS));
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key2, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key3, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), FRESH_TS);
    });

    // Perform one time repush to make sure repush can handle RMD chunks data correctly.
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    props.setProperty(REPUSH_TTL_ENABLE, "true");
    // Override the TTL repush start TS to work with logical TS setup.
    props.setProperty(REPUSH_TTL_START_TIMESTAMP, String.valueOf(FRESH_TS));
    // Override the rewind time to make sure not to consume 24hrs data from RT topic.
    props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    IntegrationTestPushUtils.runVPJ(props);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    /**
     * Validate the data is ready in storage after TTL repush.
     */
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          // Key 1 is partially preserved.
          GenericRecord valueRecord = readValue(storeReader, key1);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("default_name"));
          assertNotNull(valueRecord.get(STRING_MAP_FILED));
          Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
          assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          assertNull(stringMapValue.get(new Utf8("k2")));

          // Key 2 is fully removed.
          valueRecord = readValue(storeReader, key2);
          assertNull(valueRecord);

          // Key 3 is fully preserved.
          valueRecord = readValue(storeReader, key3);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_3"));

          valueRecord = readValue(storeReader, key4);
          assertNull(valueRecord);

          valueRecord = readValue(storeReader, key5);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_5"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    String kafkaTopic_v2 = Version.composeKafkaTopic(storeName, 2);
    // Validate RMD bytes after TTL repush.
    validateRmdData(rmdSerDe, kafkaTopic_v2, key1, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FILED);
      Assert.assertEquals(stringMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), STALE_TS);
      Assert.assertEquals(stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(FRESH_TS));
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });
    validateRmdData(rmdSerDe, kafkaTopic_v1, key2, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key3, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), FRESH_TS);
    });
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testRepushWithDeleteRecord() {
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
          30,
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
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
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
   * Test that TTL repush correctly handles RMD-chunked data in VT.
   *
   * This test creates an AA store with chunking and RMD chunking enabled, then:
   * 1. Writes streaming data to RT so the server produces RMD-bearing records to VT v1
   *    (with rmdChunkingEnabled=true, the server chunks the RMD separately)
   * 2. Runs a non-TTL repush (v1 -> v2) to guarantee VT v2 has assembled data with RMD chunks
   * 3. Writes fresh/stale streaming data for TTL testing
   * 4. Runs a TTL repush (v2 -> v3) via the Spark VPJ
   * 5. Validates the Spark chunk assembly + TTL filtering pipeline produced correct results
   *
   * Test keys written before TTL repush:
   * - keyFresh: Multiple fresh map entries (all timestamps >= FRESH_TS) -> preserved after TTL repush
   * - keyStale: Stale record (timestamp < FRESH_TS) -> filtered out after TTL repush
   * - keyMixed: Fresh map entry + stale regular field -> partially preserved (map kept, field reverted to default)
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testTTLRepushWithRmdChunkedData() {
    final String storeName = Utils.getUniqueString("ttlRmdChunk");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

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

      // Step 1: Empty push to create v1. The server will consume RT data and produce to VT v1 with RMD.
      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    long STALE_TS = 99999L;
    long FRESH_TS = 100000L;
    String STRING_MAP_FIELD = "stringMap";
    String REGULAR_FIELD = "name";
    String keyFresh = "keyFresh";
    String keyStale = "keyStale";
    String keyMixed = "keyMixed";

    // Step 2: Write initial streaming data to RT. The server leader processes these and produces
    // to VT v1 with RMD. With rmdChunkingEnabled=true, the server stores RMD in separate chunks.
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      for (int i = 0; i < 20; i++) {
        UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
        updateBuilder.setEntriesToAddToMapField(STRING_MAP_FIELD, Collections.singletonMap("k" + i, "v" + i));
        sendStreamingRecord(veniceProducer, storeName, keyFresh, updateBuilder.build(), FRESH_TS);
      }
    }

    // Wait for initial data to be ingested into storage (and VT v1).
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, keyFresh);
          assertNotNull(valueRecord, "keyFresh should exist after initial streaming writes");
          Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FIELD);
          assertNotNull(mapValue);
          assertEquals(mapValue.size(), 20, "keyFresh should have 20 map entries");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    // Step 3: Non-TTL repush (v1 -> v2). This reads from VT v1 which contains server-produced
    // records with RMD chunks (because server chunk size = 200 bytes). The repush assembles the
    // chunks and writes them to VT v2. This guarantees VT v2 has real data with RMD.
    Properties repushProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    repushProps.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    IntegrationTestPushUtils.runVPJ(repushProps);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    // Verify v2 data is correct after non-TTL repush.
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, keyFresh);
          assertNotNull(valueRecord, "keyFresh should exist in v2 after non-TTL repush");
          Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FIELD);
          assertEquals(mapValue.size(), 20, "keyFresh should have 20 map entries in v2");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    // Step 4: Write fresh/stale/mixed streaming data to RT. The server produces these to VT v2 with RMD.
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      // keyStale: Fully stale record - should be filtered out by TTL repush.
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "stale_name");
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FIELD, Collections.singletonMap("sk1", "sv1"));
      sendStreamingRecord(veniceProducer, storeName, keyStale, updateBuilder.build(), STALE_TS);

      // keyMixed: Fresh map entry + stale regular field -> partially preserved after TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FIELD, Collections.singletonMap("mk1", "mv1"));
      sendStreamingRecord(veniceProducer, storeName, keyMixed, updateBuilder.build(), FRESH_TS);
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "mixed_name");
      sendStreamingRecord(veniceProducer, storeName, keyMixed, updateBuilder.build(), STALE_TS);
    }

    // Wait for fresh/stale data to be ingested into VT v2.
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, keyStale);
          assertNotNull(valueRecord, "keyStale should exist before TTL repush");
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("stale_name"));

          valueRecord = readValue(storeReader, keyMixed);
          assertNotNull(valueRecord, "keyMixed should exist before TTL repush");
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("mixed_name"));
          Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FIELD);
          assertEquals(mapValue.get(new Utf8("mk1")), new Utf8("mv1"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    // Step 5: TTL repush (v2 -> v3). VT v2 now contains RMD-chunked data from both the non-TTL
    // repush (keyFresh) and the server-produced streaming records (keyStale, keyMixed).
    // The Spark VPJ must: read chunked records from VT v2 -> assemble chunks -> apply TTL filter -> output.
    Properties ttlProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    ttlProps.setProperty(SOURCE_KAFKA, "true");
    ttlProps.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    ttlProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    ttlProps.setProperty(REPUSH_TTL_ENABLE, "true");
    ttlProps.setProperty(REPUSH_TTL_START_TIMESTAMP, String.valueOf(FRESH_TS));
    // Don't consume from RT so stale records don't re-enter.
    ttlProps.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    IntegrationTestPushUtils.runVPJ(ttlProps);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    // Step 6: Validate post-TTL-repush data.
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          // keyFresh: all entries were written with FRESH_TS, so all should be preserved.
          GenericRecord valueRecord = readValue(storeReader, keyFresh);
          assertNotNull(valueRecord, "keyFresh should be preserved after TTL repush");
          Map<Utf8, Utf8> mapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FIELD);
          assertNotNull(mapValue, "keyFresh stringMap should not be null after repush");
          assertEquals(mapValue.size(), 20, "keyFresh should still have 20 map entries after TTL repush");
          for (int i = 0; i < 20; i++) {
            assertEquals(
                mapValue.get(new Utf8("k" + i)),
                new Utf8("v" + i),
                "keyFresh map entry k" + i + " should be preserved");
          }

          // keyStale: fully stale, should be filtered out.
          valueRecord = readValue(storeReader, keyStale);
          assertNull(valueRecord, "keyStale should be filtered out by TTL repush");

          // keyMixed: map entry with FRESH_TS kept, regular field with STALE_TS reverted to default.
          valueRecord = readValue(storeReader, keyMixed);
          assertNotNull(valueRecord, "keyMixed should be partially preserved after TTL repush");
          assertEquals(
              valueRecord.get(REGULAR_FIELD),
              new Utf8("default_name"),
              "keyMixed stale regular field should revert to default");
          mapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FIELD);
          assertNotNull(mapValue, "keyMixed stringMap should not be null");
          assertEquals(mapValue.get(new Utf8("mk1")), new Utf8("mv1"), "keyMixed fresh map entry should be preserved");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    // Step 7: Validate RMD on v3.
    String kafkaTopic_v3 = Version.composeKafkaTopic(storeName, 3);
    // keyFresh: should have per-element timestamps for all 20 entries.
    validateRmdData(rmdSerDe, kafkaTopic_v3, keyFresh, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD);
      assertNotNull(stringMapTsRecord, "RMD for keyFresh should have stringMap timestamps");
      java.util.List<Long> activeElemTs = (List<Long>) stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
      assertNotNull(activeElemTs, "Active element timestamps should exist");
      assertEquals(activeElemTs.size(), 20, "Should have 20 active element timestamps");
      for (Long ts: activeElemTs) {
        assertEquals(ts.longValue(), FRESH_TS, "Each element timestamp should be FRESH_TS");
      }
    });

    // keyMixed: should have fresh map entry preserved, stale regular field timestamp in RMD.
    validateRmdData(rmdSerDe, kafkaTopic_v3, keyMixed, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD);
      assertNotNull(stringMapTsRecord, "RMD for keyMixed should have stringMap timestamps");
      java.util.List<Long> activeElemTs = (List<Long>) stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
      assertNotNull(activeElemTs, "Active element timestamps should exist for keyMixed");
      assertEquals(activeElemTs.size(), 1, "keyMixed should have 1 active element timestamp");
      assertEquals(activeElemTs.get(0).longValue(), FRESH_TS, "keyMixed map element timestamp should be FRESH_TS");
      assertEquals(
          timestampRecord.get(REGULAR_FIELD),
          STALE_TS,
          "keyMixed regular field RMD timestamp should reflect the stale write");
    });
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  private void validateRmdData(
      RmdSerDe rmdSerDe,
      String kafkaTopic,
      String key,
      Consumer<RmdWithValueSchemaId> rmdDataValidationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionMultiClusterWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get("venice-cluster0")
        .getVeniceServers()) {
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);
      // RMD may not be immediately available after repush (e.g., with ZSTD_WITH_DICT compression where
      // dictionary-based decompression/recompression can delay RMD persistence). Retry until available.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        ValueRecord result = SingleGetChunkingAdapter
            .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null);
        // Avoid assertion failure logging massive RMD record.
        assertNotNull(result, "RMD should exist for key: " + key);
        byte[] rmdBytes = result.serialize();
        RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
        rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(rmdBytes, rmdWithValueSchemaId);
        rmdDataValidationFlow.accept(rmdWithValueSchemaId);
      });
    }
  }

  private byte[] serializeStringKeyToByteArray(String key) {
    Utf8 utf8Key = new Utf8(key);
    DatumWriter<Utf8> writer = new GenericDatumWriter<>(Schema.create(Schema.Type.STRING));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(out);
    try {
      writer.write(utf8Key, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write input: " + utf8Key + " to binary encoder", e);
    }
    return out.toByteArray();
  }
}

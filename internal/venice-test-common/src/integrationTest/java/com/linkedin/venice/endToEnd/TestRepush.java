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
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRepush {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int ASSERTION_TIMEOUT_MS = 30_000;

  private static final int REPLICATION_FACTOR = 2;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private D2Client d2ClientDC0;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, true);
    serverProperties.put(
        ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
        KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION.name());
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, true);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(REPLICATION_FACTOR)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    this.multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
    this.d2ClientDC0 = new D2ClientBuilder()
        .setZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2ClientDC0);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testRepushWithChunkingFlagChanged() {
    final String storeName = Utils.getUniqueString("reproduce");
    String parentControllerUrl = parentController.getControllerUrl();
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
    String parentControllerUrl = parentController.getControllerUrl();
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
    String parentControllerUrl = parentController.getControllerUrl();
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
    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
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

    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
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

    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
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

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2ClientDC0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
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
      ValueRecord result = SingleGetChunkingAdapter
          .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null);
      // Avoid assertion failure logging massive RMD record.
      boolean nullRmd = (result == null);
      assertFalse(nullRmd);
      byte[] value = result.serialize();
      RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
      rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(value, rmdWithValueSchemaId);
      rmdDataValidationFlow.accept(rmdWithValueSchemaId);
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

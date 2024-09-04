package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.stats.HostLevelIngestionStats.ASSEMBLED_RMD_SIZE_IN_BYTES;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToUserWithStringMapSchema;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.ConsumerPoolType;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskBackdoor;
import com.linkedin.davinci.kafka.consumer.TopicPartitionIngestionInfo;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update (Write Compute) with a setup that has both the parent and child controllers.
 */
public class PartialUpdateTest {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int ASSERTION_TIMEOUT_MS = 30_000;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  private static VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> VENICE_JSON_SERIALIZER =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
      });

  private static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, true);
    serverProperties.put(
        ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
        KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION.name());
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(controllerProps),
        Optional.of(controllerProps),
        Optional.of(serverProperties),
        false);
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
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
      SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
      GenericRecord keyRecord = new GenericData.Record(keySchema);
      keyRecord.put("learnerUrn", "urn:li:member:682787898");
      keyRecord.put("query", "python");
      GenericRecord checkpointKeyRecord = new GenericData.Record(keySchema);
      checkpointKeyRecord.put("learnerUrn", "urn:li:member:123");
      checkpointKeyRecord.put("query", "python");

      GenericRecord partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
          .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:751323"))
          .build();
      sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);

      // Perform one time repush to make sure repush can handle RMD chunks data correctly.
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      // props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      TestWriteUtils.runPushJob("Run repush job 1", props);
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
        TestWriteUtils.runPushJob("Run repush job 2", props);

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
        partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
            .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:1"))
            .build();
        sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
        partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
            .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:2"))
            .build();
        sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
        sendStreamingRecord(veniceProducer, storeName, checkpointKeyRecord, partialUpdateRecord);
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

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPushPartialUpdateClassicFormat() throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_classic_format");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
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

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIncrementalPushPartialUpdateNewFormat(boolean useSparkCompute) throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_new_format");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);
    if (useSparkCompute) {
      vpjProperties.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, NAME_RECORD_V2_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
              assertEquals(value.get("age"), -1);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  /**
   * This integration test verifies that in A/A + partial update enabled store, UPDATE on a key that was written in the
   * batch push should not throw exception, as the update logic should initialize a new RMD record for the original value
   * and apply updates on top of them.
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class)
  public void testPartialUpdateOnBatchPushedKeys(CompressionStrategy compressionStrategy) throws IOException {
    final String storeName = Utils.getUniqueString("updateBatch");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);

    Schema valueSchema = AvroCompatibilityHelper.parse(valueSchemaStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

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
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      veniceClusterWrapper.waitVersion(storeName, 1);
      // Produce partial updates on batch pushed keys
      SystemProducer veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
      for (int i = 1; i < 100; i++) {
        GenericRecord partialUpdateRecord =
            new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("firstName", "new_name_" + i).build();
        sendStreamingRecord(veniceProducer, storeName, String.valueOf(i), partialUpdateRecord);
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
  public void testPartialUpdateOnBatchPushedChunkKeys() throws IOException {
    final String storeName = Utils.getUniqueString("updateBatch");
    String parentControllerUrl = parentController.getControllerUrl();
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

    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

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
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      veniceClusterWrapper.waitVersion(storeName, 1);
      // Produce partial updates on batch pushed keys
      SystemProducer veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
      for (int i = 1; i < 100; i++) {
        GenericRecord partialUpdateRecord =
            new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("key", "new_name_" + i).build();
        sendStreamingRecord(veniceProducer, storeName, String.valueOf(i), partialUpdateRecord);
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

  @Test(timeOut = TEST_TIMEOUT_MS * 3)
  public void testNonAAPartialUpdateChunkDeletion() {
    final String storeName = Utils.getUniqueString("partialUpdateChunking");
    String parentControllerUrl = parentController.getControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(false)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(false)
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

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);

    String key = "key1";
    String primitiveFieldName = "name";
    String mapFieldName = "stringMap";

    // Insert large amount of Map entries to trigger RMD chunking.
    int oldUpdateCount = 29;
    int singleUpdateEntryCount = 10000;
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < oldUpdateCount; i++) {
        producePartialUpdate(
            storeName,
            veniceProducer,
            partialUpdateSchema,
            key,
            primitiveFieldName,
            mapFieldName,
            singleUpdateEntryCount,
            i);
      }
      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          Map<String, String> mapFieldResult = new HashMap<>();
          ((Map<Utf8, Utf8>) valueRecord.get(mapFieldName))
              .forEach((x, y) -> mapFieldResult.put(x.toString(), y.toString()));
          assertEquals(mapFieldResult.size(), oldUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
      validateValueChunks(kafkaTopic_v1, key, Assert::assertNotNull);
      VeniceServerWrapper serverWrapper = multiRegionMultiClusterWrapper.getChildRegions()
          .get(0)
          .getClusters()
          .get("venice-cluster0")
          .getVeniceServers()
          .get(0);
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic_v1);
      ChunkedValueManifest valueManifest = getChunkValueManifest(storageEngine, 0, key, false);

      int updateCount = 30;
      producePartialUpdate(
          storeName,
          veniceProducer,
          partialUpdateSchema,
          key,
          primitiveFieldName,
          mapFieldName,
          singleUpdateEntryCount,
          updateCount - 1);

      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          Map<String, String> mapFieldResult = new HashMap<>();
          ((Map<Utf8, Utf8>) valueRecord.get(mapFieldName))
              .forEach((x, y) -> mapFieldResult.put(x.toString(), y.toString()));
          assertEquals(mapFieldResult.size(), updateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        Assert.assertNotNull(valueManifest);
        validateChunksFromManifests(kafkaTopic_v1, 0, valueManifest, null, (valueChunkBytes, rmdChunkBytes) -> {
          Assert.assertNull(valueChunkBytes);
        }, false);
      });
    } finally {
      veniceProducer.stop();
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
    String parentControllerUrl = parentController.getControllerUrl();
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

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);

    String key = "key1";
    String primitiveFieldName = "name";
    String listFieldName = "floatArray";

    int totalUpdateCount = 40;
    // Insert large amount of Map entries to trigger RMD chunking.
    int singleUpdateEntryCount = 10000;
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
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
      validateValueChunks(kafkaTopic_v1, key, Assert::assertNotNull);
      VeniceServerWrapper serverWrapper = multiRegionMultiClusterWrapper.getChildRegions()
          .get(0)
          .getClusters()
          .get("venice-cluster0")
          .getVeniceServers()
          .get(0);
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic_v1);
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
      validateRmdData(rmdSerDe, kafkaTopic_v1, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
        GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
        List<Long> activeElementsTimestamps =
            (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
        assertEquals(activeElementsTimestamps.size(), totalUpdateCount * singleUpdateEntryCount);
      });
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        Assert.assertNotNull(valueManifest);
        Assert.assertNotNull(rmdManifest);
        validateChunksFromManifests(kafkaTopic_v1, 0, valueManifest, rmdManifest, (valueChunkBytes, rmdChunkBytes) -> {
          Assert.assertNull(valueChunkBytes);
          Assert.assertNotNull(rmdChunkBytes);
          Assert.assertEquals(rmdChunkBytes.length, 4);
        }, true);
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
      TestWriteUtils.runPushJob("Run repush job", props);

      ControllerClient controllerClient =
          new ControllerClient("venice-cluster0", childDatacenters.get(0).getControllerConnectString());
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
      validateRmdData(rmdSerDe, kafkaTopic_v2, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
        GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
        List<Long> activeElementsTimestamps =
            (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
        assertEquals(activeElementsTimestamps.size(), totalUpdateCount * singleUpdateEntryCount);
      });

      // Send DELETE record that partially removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, (totalUpdateCount - 1) * 10L);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertFalse(nullRecord);
        assertEquals(((List<Float>) (valueRecord.get(listFieldName))).size(), singleUpdateEntryCount);
      });

      validateRmdData(rmdSerDe, kafkaTopic_v2, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
        GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
        List<Long> activeElementsTimestamps =
            (List<Long>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
        assertEquals(activeElementsTimestamps.size(), singleUpdateEntryCount);
        List<Long> deletedElementsTimestamps =
            (List<Long>) collectionFieldTimestampRecord.get(DELETED_ELEM_TS_FIELD_NAME);
        assertEquals(deletedElementsTimestamps.size(), 0);
      });

      // Send DELETE record that fully removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, totalUpdateCount * 10L);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertTrue(nullRecord);
      });
      validateRmdData(rmdSerDe, kafkaTopic_v2, key, rmdWithValueSchemaId -> {
        Assert.assertTrue(rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
        GenericRecord collectionFieldTimestampRecord = (GenericRecord) timestampRecord.get(listFieldName);
        assertEquals(collectionFieldTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME), (long) (totalUpdateCount) * 10);
      });
    } finally {
      veniceProducer.stop();
    }

    String baseMetricName = AbstractVeniceStats.getSensorFullName(storeName, ASSEMBLED_RMD_SIZE_IN_BYTES);
    List<Double> assembledRmdSizes = MetricsUtils.getAvgMax(baseMetricName, veniceCluster.getVeniceServers());
    MetricsUtils.validateMetricRange(assembledRmdSizes, 290000, 740000);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testRepushWithTTLWithActiveActivePartialUpdateStore() {
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
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    long STALE_TS = 99999L;
    long FRESH_TS = 100000L;
    String STRING_MAP_FILED = "stringMap";
    String REGULAR_FIELD = "name";
    /**
     * Case 1: The record is partially stale, TTL repush should only keep the part that's fresh.
     */
    String key1 = "key1";
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
    String key2 = "key2";
    // This update is expected to be WIPED OUT after TTL repush.
    updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_2");
    sendStreamingRecord(veniceProducer, storeName, key2, updateBuilder.build(), STALE_TS);

    /**
     * Case 3: The record is fully fresh, TTL repush should keep the record.
     */
    String key3 = "key3";
    // This update is expected to be carried into TTL repush.
    updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_3");
    sendStreamingRecord(veniceProducer, storeName, key3, updateBuilder.build(), FRESH_TS);

    /**
     * Case 4: The delete record is fully fresh, TTL repush should NOT FAIL due to this record.
     */
    String key4 = "key4";
    sendStreamingRecord(veniceProducer, storeName, key4, updateBuilder.build(), FRESH_TS);
    sendStreamingDeleteRecord(veniceProducer, storeName, key4, FRESH_TS + 1);

    /**
     * Case 5: The delete record is stale, TTL repush should keep new update.
     */
    String key5 = "key5";
    sendStreamingDeleteRecord(veniceProducer, storeName, key5, STALE_TS);
    updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_5");
    sendStreamingRecord(veniceProducer, storeName, key5, updateBuilder.build(), FRESH_TS);

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
    TestWriteUtils.runPushJob("Run repush job 1", props);
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
  public void testEnablePartialUpdateOnActiveActiveStore() {
    final String storeName = Utils.getUniqueString("testRepushWithDeleteRecord");
    String parentControllerUrl = parentController.getControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    String realTimeTopicName = Version.composeRealTimeTopic(storeName);
    PubSubTopic realTimeTopic = PUB_SUB_TOPIC_REPOSITORY.getTopic(realTimeTopicName);
    PubSubTopic storeVersionTopicV1 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1));
    PubSubTopic storeVersionTopicV2 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 2));
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(1L)
              .setHybridOffsetLagThreshold(1L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // Generate v1 without AA and Write-compute:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV1.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      verifyConsumerThreadPoolFor(
          storeVersionTopicV1,
          new PubSubTopicPartitionImpl(realTimeTopic, 0),
          ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1);
      verifyConsumerThreadPoolFor(
          storeVersionTopicV1,
          new PubSubTopicPartitionImpl(storeVersionTopicV1, 0),
          ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1);
      // Enable write-compute for v1:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER => CURRENT_VERSION_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsEnableWriteCompute =
          new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      updateStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsEnableWriteCompute));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            storeVersionTopicV1,
            new PubSubTopicPartitionImpl(realTimeTopic, 0),
            ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
            1);
        verifyConsumerThreadPoolFor(
            storeVersionTopicV1,
            new PubSubTopicPartitionImpl(storeVersionTopicV1, 0),
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1);
      });
      // Disable write-compute for v1:
      // leader: CURRENT_VERSION_AAWC_LEADER => CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsDisableWriteCompute =
          new UpdateStoreQueryParams().setWriteComputationEnabled(false);
      updateStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsDisableWriteCompute));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            storeVersionTopicV1,
            new PubSubTopicPartitionImpl(realTimeTopic, 0),
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1);
        verifyConsumerThreadPoolFor(
            storeVersionTopicV1,
            new PubSubTopicPartitionImpl(storeVersionTopicV1, 0),
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1);
      });
      // Enable AA and push a new version to get v2, to verify pool change
      // For v1 without AA:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER => NON_CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER => CURRENT_VERSION_AAWC_LEADER
      // For v2 with AA:
      // leader : CURRENT_VERSION_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsForEnablingAA =
          new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
      updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsForEnablingAA));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      response = parentControllerClient.emptyPush(storeName, "test_push_id for 2", 1000);

      assertEquals(response.getVersion(), 2);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV2.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      // Version 1 has active-active and write compute disabled, so each server host will ingest from local data center
      // for leader.
      verifyConsumerThreadPoolFor(
          storeVersionTopicV1,
          new PubSubTopicPartitionImpl(realTimeTopic, 0),
          ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1);
      verifyConsumerThreadPoolFor(
          storeVersionTopicV1,
          new PubSubTopicPartitionImpl(storeVersionTopicV1, 0),
          ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1);
      // Version 2 has active-active enabled, so each server host will ingest from two data centers for leader.
      verifyConsumerThreadPoolFor(
          storeVersionTopicV2,
          new PubSubTopicPartitionImpl(realTimeTopic, 0),
          ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
          NUMBER_OF_CHILD_DATACENTERS);
      verifyConsumerThreadPoolFor(
          storeVersionTopicV2,
          new PubSubTopicPartitionImpl(storeVersionTopicV2, 0),
          ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    String key1 = "key1";

    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("name", "value-level TS");
    record.put("floatArray", Collections.emptyList());
    record.put("stringMap", Collections.emptyMap());

    sendStreamingRecord(veniceProducer, storeName, key1, record, 10000L);

    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
      VeniceClusterWrapper cluster = childDatacenters.get(i).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("value-level TS"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
    }

    // Enable AA and push a new version to get v3, to verify pool change
    // For v2 with AA:
    // leader : CURRENT_VERSION_AAWC_LEADER => NON_CURRENT_VERSION_AAWC_LEADER
    // follower: CURRENT_VERSION_NON_AAWC_LEADER => NON_CURRENT_VERSION_NON_AAWC_LEADER
    // For v3 with AA:
    // leader : CURRENT_VERSION_AAWC_LEADER
    // follower: CURRENT_VERSION_NON_AAWC_LEADER
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    TestWriteUtils.runPushJob("Run repush job 1", props);
    PubSubTopic storeVersionTopicV3 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 3));
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV3.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
    // Version 2 become backup version.
    verifyConsumerThreadPoolFor(
        storeVersionTopicV2,
        new PubSubTopicPartitionImpl(realTimeTopic, 0),
        ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
        NUMBER_OF_CHILD_DATACENTERS);
    verifyConsumerThreadPoolFor(
        storeVersionTopicV2,
        new PubSubTopicPartitionImpl(storeVersionTopicV2, 0),
        ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        1);
    verifyConsumerThreadPoolFor(
        storeVersionTopicV3,
        new PubSubTopicPartitionImpl(realTimeTopic, 0),
        ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
        NUMBER_OF_CHILD_DATACENTERS);
    verifyConsumerThreadPoolFor(
        storeVersionTopicV3,
        new PubSubTopicPartitionImpl(storeVersionTopicV3, 0),
        ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        1);
    // Need to create a new producer to update partial update config from store response.
    veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    UpdateBuilder builder = new UpdateBuilderImpl(partialUpdateSchema);
    builder.setNewFieldValue("name", "field-level TS");
    sendStreamingRecord(veniceProducer, storeName, key1, builder.build(), 10001L);
    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
      VeniceClusterWrapper cluster = childDatacenters.get(i).getClusters().get(CLUSTER_NAME);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("field-level TS"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  private void verifyConsumerThreadPoolFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      ConsumerPoolType consumerPoolType,
      int expectedRegionNum) {
    for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
      for (VeniceServerWrapper serverWrapper: veniceMultiClusterWrapper.getClusters()
          .get(CLUSTER_NAME)
          .getVeniceServers()) {
        KafkaStoreIngestionService kafkaStoreIngestionService =
            serverWrapper.getVeniceServer().getKafkaStoreIngestionService();
        TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse =
            kafkaStoreIngestionService.getTopicPartitionIngestionContext(
                versionTopic.getName(),
                pubSubTopicPartition.getTopicName(),
                pubSubTopicPartition.getPartitionNumber());
        try {
          Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContexts = VENICE_JSON_SERIALIZER
              .deserialize(topicPartitionIngestionContextResponse.getTopicPartitionIngestionContext(), "");
          if (!topicPartitionIngestionContexts.isEmpty()) {
            int regionCount = 0;
            for (Map.Entry<String, Map<String, TopicPartitionIngestionInfo>> entry: topicPartitionIngestionContexts
                .entrySet()) {
              Map<String, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap = entry.getValue();
              Assert.assertTrue(topicPartitionIngestionInfoMap.size() <= 1);
              for (Map.Entry<String, TopicPartitionIngestionInfo> topicPartitionIngestionInfoEntry: topicPartitionIngestionInfoMap
                  .entrySet()) {
                String topicPartitionStr = topicPartitionIngestionInfoEntry.getKey();
                TopicPartitionIngestionInfo topicPartitionIngestionInfo = topicPartitionIngestionInfoEntry.getValue();
                assertEquals(pubSubTopicPartition.toString(), topicPartitionStr);
                assertTrue(topicPartitionIngestionInfo.getConsumerIdStr().contains(consumerPoolType.getStatSuffix()));
                regionCount += 1;
              }
            }
            Assert.assertEquals(regionCount, expectedRegionNum);
          }
        } catch (IOException e) {
          throw new VeniceException("Got IO Exception during consumer pool check.", e);
        }
      }
    }
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
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    String STRING_MAP_FILED = "stringMap";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";

    UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k1", "v1"));
    sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 10000L);
    sendStreamingDeleteRecord(veniceProducer, storeName, key1, 10001L);
    sendStreamingRecord(veniceProducer, storeName, key2, updateBuilder.build(), 10001L);

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    TestWriteUtils.runPushJob("Run repush job 1", props);
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
    sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 9999L);
    sendStreamingRecord(veniceProducer, storeName, key3, updateBuilder.build(), 10000L);
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
    sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), 12345L);

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
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);
      ValueRecord result = SingleGetChunkingAdapter
          .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null, null);
      // Avoid assertion failure logging massive RMD record.
      boolean nullRmd = (result == null);
      assertFalse(nullRmd);
      byte[] value = result.serialize();
      RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
      rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(value, rmdWithValueSchemaId);
      rmdDataValidationFlow.accept(rmdWithValueSchemaId);
    }
  }

  /**
   * This test simulates a situation where the stored value schema mismatches with the value schema used by a partial update
   * request. In other words, the partial update request tries to update a field that does not exist in the stored value
   * record due to schema mismatch.
   *
   * In this case, we expect a superset schema that contains fields from all value schema to be used to store the partially
   * updated value record. The partially updated value record should contain original fields as well as the partially updated
   * field.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testUpdateWithSupersetSchema() {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = parentController.getControllerUrl();
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));
    Schema valueSchemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV2.avsc"));
    String valueFieldName = "name";

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchemaV1.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
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

      assertCommand(parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString()));
    }

    SystemProducer veniceProducer = null;
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Step 1. Put a value record.
      veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
      String key = "key1";
      GenericRecord value = new GenericData.Record(valueSchemaV1);
      value.put(valueFieldName, "Lebron");
      value.put("age", 37);
      sendStreamingRecord(veniceProducer, storeName, key, value);

      // Verify the Put has been persisted
      TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron");
          assertEquals(retrievedValue.get("age").toString(), "37");

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 2: Partially update a field that exists in V2 schema (and it does not exist in V1 schema).
      Schema writeComputeSchemaV2 =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV2);
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchemaV2);
      updateBuilder.setNewFieldValue(valueFieldName, "Lebron James");
      updateBuilder.setNewFieldValue("hometown", "Akron");
      GenericRecord partialUpdateRecord = updateBuilder.build();
      sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);

      // Verify the value record has been partially updated and it uses V3 superset value schema now.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron James"); // Updated field
          assertEquals(retrievedValue.get("age").toString(), "37");
          assertEquals(retrievedValue.get("hometown").toString(), "Akron"); // Updated field

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testWriteComputeWithHybridLeaderFollowerLargeRecord(
      boolean writeComputeFromCache,
      CompressionStrategy compressionStrategy) throws Exception {

    SystemProducer veniceProducer = null;

    try {
      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("write-compute-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String parentControllerURL = parentController.getControllerUrl();
      // Records 1-100, id string to name record
      Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      Properties vpjProperties =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL)) {

        String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
        String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
        assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setChunkingEnabled(true)
                .setCompressionStrategy(compressionStrategy)
                .setWriteComputationEnabled(true)
                .setHybridRewindSeconds(10L)
                .setHybridOffsetLagThreshold(2L));

        assertFalse(response.isError());

        // Add a new value schema v2 to store
        SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NAME_RECORD_V2_SCHEMA.toString());
        assertFalse(schemaResponse.isError());

        // Add partial update schema associated to v2.
        // Note that partial update schema needs to be registered manually here because the integration test harness
        // does not create any parent controller. In production, when a value schema is added to a partial update
        // enabled
        // store via a parent controller, it will automatically generate and register its WC schema.
        Schema writeComputeSchema =
            WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
        schemaResponse =
            controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
        assertFalse(schemaResponse.isError());

        // VPJ push
        String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
        try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
          runVPJ(vpjProperties, 1, childControllerClient);
        }
        veniceClusterWrapper.waitVersion(storeName, 1);
        try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
          // Verify records (note, records 1-100 have been pushed)
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
            try {
              for (int i = 1; i < 100; i++) {
                String key = String.valueOf(i);
                GenericRecord value = readValue(storeReader, key);
                assertNotNull(value, "Key " + key + " should not be missing!");
                assertEquals(value.get("firstName").toString(), "first_name_" + key);
                assertEquals(value.get("lastName").toString(), "last_name_" + key);
                assertEquals(value.get("age"), -1);
              }
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // disable the purging of transientRecord buffer using reflection.
          if (writeComputeFromCache) {
            String topicName = Version.composeKafkaTopic(storeName, 1);
            for (VeniceServerWrapper veniceServerWrapper: veniceClusterWrapper.getVeniceServers()) {
              StoreIngestionTaskBackdoor.setPurgeTransientRecordBuffer(veniceServerWrapper, topicName, false);
            }
          }

          // Do not send large record to RT; RT doesn't support chunking
          veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
          String key = String.valueOf(101);
          GenericRecord value = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
          char[] chars = new char[100];
          Arrays.fill(chars, 'f');
          String firstName = new String(chars);
          Arrays.fill(chars, 'l');
          String lastName = new String(chars);
          value.put("firstName", firstName);
          value.put("lastName", lastName);
          sendStreamingRecord(veniceProducer, storeName, key, value);

          // Verify the streaming record
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), firstName);
              assertEquals(retrievedValue.get("lastName").toString(), lastName);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // Update the record
          Arrays.fill(chars, 'u');
          String updatedFirstName = new String(chars);
          final int updatedAge = 1;
          UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
          updateBuilder.setNewFieldValue("firstName", updatedFirstName);
          updateBuilder.setNewFieldValue("age", updatedAge);
          GenericRecord partialUpdateRecord = updateBuilder.build();

          sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
          // Verify the update
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName);
              assertEquals(retrievedValue.get("lastName").toString(), lastName);
              assertEquals(retrievedValue.get("age"), updatedAge);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // Update the record again
          Arrays.fill(chars, 'v');
          String updatedFirstName1 = new String(chars);

          updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
          updateBuilder.setNewFieldValue("firstName", updatedFirstName1);
          GenericRecord partialUpdateRecord1 = updateBuilder.build();
          sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord1);
          // Verify the update
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName1);
              assertEquals(retrievedValue.get("lastName").toString(), lastName);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // Delete the record
          sendStreamingRecord(veniceProducer, storeName, key, null);
          // Verify the delete
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNull(retrievedValue, "Key " + key + " should be missing!");
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // Update the record again
          Arrays.fill(chars, 'w');
          String updatedFirstName2 = new String(chars);
          Arrays.fill(chars, 'g');
          String updatedLastName = new String(chars);

          updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
          updateBuilder.setNewFieldValue("firstName", updatedFirstName2);
          updateBuilder.setNewFieldValue("lastName", updatedLastName);
          updateBuilder.setNewFieldValue("age", 2);
          GenericRecord partialUpdateRecord2 = updateBuilder.build();

          sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord2);
          // Verify the update
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName2);
              assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
              assertEquals(retrievedValue.get("age"), 2);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

          // Update the record again
          Arrays.fill(chars, 'x');
          String updatedFirstName3 = new String(chars);

          updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
          updateBuilder.setNewFieldValue("firstName", updatedFirstName3);
          GenericRecord partialUpdateRecord3 = updateBuilder.build();
          sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord3);
          // Verify the update
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
            try {
              GenericRecord retrievedValue = readValue(storeReader, key);
              assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName3);
              assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });

        }
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testWriteComputeWithSamzaBatchJob() throws Exception {

    SystemProducer veniceProducer = null;
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;

    String storeName = Utils.getUniqueString("write-compute-store");
    File inputDir = getTempDataDirectory();
    String parentControllerURL = parentController.getControllerUrl();
    // Records 1-100, id string to name record
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL)) {

      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
      assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L));

      assertFalse(response.isError());

      // Add a new value schema v2 to store
      SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NAME_RECORD_V2_SCHEMA.toString());
      assertFalse(schemaResponse.isError());

      // Add WC (Write Compute) schema associated to v2.
      // (this is a test environment only needed step since theres no parent)
      Schema writeComputeSchema =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
      schemaResponse =
          controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
      assertFalse(schemaResponse.isError());

      // Run empty push to create a version and get everything created
      controllerClient.sendEmptyPushAndWait(storeName, "foopush", 10000, 60 * Time.MS_PER_SECOND);

      VeniceSystemFactory factory = new VeniceSystemFactory();
      Version.PushType pushType = Version.PushType.BATCH;
      Map<String, String> samzaConfig = getSamzaProducerConfig(veniceClusterWrapper, storeName, pushType);
      // final boolean veniceAggregate = config.getBoolean(prefix + VENICE_AGGREGATE, false);
      samzaConfig.put("systems.venice." + VENICE_AGGREGATE, "true");
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
      samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
      veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();

      // build partial update
      char[] chars = new char[5];
      Arrays.fill(chars, 'f');
      String firstName = new String(chars);
      Arrays.fill(chars, 'l');
      String lastName = new String(chars);

      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
      updateBuilder.setNewFieldValue("firstName", firstName);
      updateBuilder.setNewFieldValue("lastName", lastName);
      GenericRecord partialUpdateRecord = updateBuilder.build();

      for (int i = 0; i < 10; i++) {
        String key = String.valueOf(i);
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
      }

      // send end of push
      controllerClient.writeEndOfPush(storeName, 2);

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        // Verify everything made it
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 0; i < 10; i++) {
              GenericRecord retrievedValue = readValue(storeReader, Integer.toString(i));
              assertNotNull(retrievedValue, "Key " + i + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), firstName);
              assertEquals(retrievedValue.get("lastName").toString(), lastName);
              assertEquals(retrievedValue.get("age").toString(), "-1");
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  /**
   * Blocking, waits for new version to go online
   */
  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    String jobName = Utils.getUniqueString("write-compute-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          60,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
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

  private void producePartialUpdate(
      String storeName,
      SystemProducer veniceProducer,
      Schema partialUpdateSchema,
      String key,
      String primitiveFieldName,
      String mapFieldName,
      int singleUpdateEntryCount,
      int updateCount) {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(primitiveFieldName, "Tottenham");
    Map<String, String> newEntries = new HashMap<>();
    for (int j = 0; j < singleUpdateEntryCount; j++) {
      String idx = String.valueOf(updateCount * singleUpdateEntryCount + j);
      newEntries.put("key_" + idx, "value_" + idx);
    }
    updateBuilder.setEntriesToAddToMapField(mapFieldName, newEntries);
    GenericRecord partialUpdateRecord = updateBuilder.build();
    sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord, updateCount * 10L + 1);
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
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
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

  private void validateValueChunks(String kafkaTopic, String key, Consumer<byte[]> validationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionMultiClusterWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get("venice-cluster0")
        .getVeniceServers()) {
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);

      ChunkedValueManifest manifest = getChunkValueManifest(storageEngine, 0, key, false);
      Assert.assertNotNull(manifest);

      for (ByteBuffer chunkedKey: manifest.keysWithChunkIdSuffix) {
        byte[] chunkValueBytes = storageEngine.get(0, chunkedKey.array());
        validationFlow.accept(chunkValueBytes);
      }
    }
  }

  private void validateChunksFromManifests(
      String kafkaTopic,
      int partition,
      ChunkedValueManifest valueManifest,
      ChunkedValueManifest rmdManifest,
      BiConsumer<byte[], byte[]> validationFlow,
      boolean isAAEnabled) {
    for (VeniceServerWrapper serverWrapper: multiRegionMultiClusterWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get("venice-cluster0")
        .getVeniceServers()) {
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);

      validateChunkDataFromManifest(storageEngine, partition, valueManifest, validationFlow, isAAEnabled);
      validateChunkDataFromManifest(storageEngine, partition, rmdManifest, validationFlow, isAAEnabled);
    }
  }

  private ChunkedValueManifest getChunkValueManifest(
      AbstractStorageEngine storageEngine,
      int partition,
      String key,
      boolean isRmd) {
    byte[] serializedKeyBytes =
        ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializeStringKeyToByteArray(key));
    byte[] manifestValueBytes = isRmd
        ? storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(serializedKeyBytes))
        : storageEngine.get(partition, serializedKeyBytes);
    if (manifestValueBytes == null) {
      return null;
    }
    int schemaId = ValueRecord.parseSchemaId(manifestValueBytes);
    Assert.assertEquals(schemaId, AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    return CHUNKED_VALUE_MANIFEST_SERIALIZER.deserialize(manifestValueBytes, schemaId);
  }

  private void validateChunkDataFromManifest(
      AbstractStorageEngine storageEngine,
      int partition,
      ChunkedValueManifest manifest,
      BiConsumer<byte[], byte[]> validationFlow,
      boolean isAAEnabled) {
    if (manifest == null) {
      return;
    }
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      byte[] chunkKeyBytes = manifest.keysWithChunkIdSuffix.get(i).array();
      byte[] valueBytes = storageEngine.get(partition, chunkKeyBytes);
      byte[] rmdBytes =
          isAAEnabled ? storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(chunkKeyBytes)) : null;
      validationFlow.accept(valueBytes, rmdBytes);
    }
  }

}

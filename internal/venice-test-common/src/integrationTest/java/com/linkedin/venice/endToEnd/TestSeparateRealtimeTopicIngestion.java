package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.verifyConsumerThreadPoolFor;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_TO_SEPARATE_REALTIME_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.ConsumerPoolType;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
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
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
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
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestSeparateRealtimeTopicIngestion {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final long TEST_TIMEOUT_MS = 60_000;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final int REPLICATION_FACTOR = 2;
  private static final String CLUSTER_NAME = "venice-cluster0";
  private static final int ASSERTION_TIMEOUT_MS = 30_000;

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
    serverProperties.put(ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, Boolean.toString(false));
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        REPLICATION_FACTOR,
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

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT_MS * 2)
  public void testIncrementalPushPartialUpdate() throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_classic_format");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(PUSH_TO_SEPARATE_REALTIME_TOPIC, true);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);
    // Set source region to dc-1.
    vpjProperties.put(SOURCE_GRID_FABRIC, "dc-1");

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setActiveActiveReplicationEnabled(true)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setPartitionCount(1)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setSeparateRealTimeTopicEnabled(true)
              .setHybridRewindSeconds(10000L)
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

      Schema valueSchema = TestWriteUtils.NAME_RECORD_V1_SCHEMA;
      Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
      Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
      ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
      when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
      when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
      when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
      StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
          new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
      RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

      // Run one time Incremental Push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      validateData(storeName, veniceClusterWrapper);

      parentControllerClient.emptyPush(storeName, "test_push_id_v2", 1000);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      validateData(storeName, veniceClusterWrapper);
      validateRmdData(rmdSerDe, Version.composeKafkaTopic(storeName, 2), String.valueOf(99), rmdWithValueSchemaId -> {
        GenericRecord rmdRecord = rmdWithValueSchemaId.getRmdRecord();
        List<Long> offsetVector = RmdUtils.extractOffsetVectorFromRmd(rmdRecord);
        Assert.assertEquals(offsetVector.size(), 4);
        // No msg is written to RT regions.
        Assert.assertEquals(offsetVector.get(0).longValue(), 0L);
        Assert.assertEquals(offsetVector.get(1).longValue(), 0L);
        // Since we only have 1 partition and push to 1 region, the last key's offset should be greater or equal to the
        // total key count.
        Assert.assertTrue(offsetVector.get(3) >= 100);
      });
      PubSubTopic realTimeTopic = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeRealTimeTopic(storeName));
      PubSubTopic separateRealtimeTopic =
          PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeSeparateRealTimeTopic(storeName));
      PubSubTopic versionTopicV1 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1));
      PubSubTopic versionTopicV2 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 2));
      PubSubTopicPartition versionV1TopicPartition = new PubSubTopicPartitionImpl(versionTopicV1, 0);
      PubSubTopicPartition versionV2TopicPartition = new PubSubTopicPartitionImpl(versionTopicV2, 0);
      PubSubTopicPartition realtimeTopicPartition = new PubSubTopicPartitionImpl(realTimeTopic, 0);
      PubSubTopicPartition separateRealtimeTopicPartition = new PubSubTopicPartitionImpl(separateRealtimeTopic, 0);

      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV2,
            versionV2TopicPartition,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);

        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV2,
            realtimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
            NUMBER_OF_CHILD_DATACENTERS,
            1);

        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV2,
            separateRealtimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_SEP_RT_LEADER_POOL,
            NUMBER_OF_CHILD_DATACENTERS,
            1);

        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV1,
            realtimeTopicPartition,
            ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
            NUMBER_OF_CHILD_DATACENTERS,
            1);

        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV1,
            versionV1TopicPartition,
            ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);

        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            versionTopicV1,
            separateRealtimeTopicPartition,
            ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
            NUMBER_OF_CHILD_DATACENTERS,
            1);
      });
    }
  }

  private void validateData(String storeName, VeniceClusterWrapper veniceClusterWrapper) {
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

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

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
}

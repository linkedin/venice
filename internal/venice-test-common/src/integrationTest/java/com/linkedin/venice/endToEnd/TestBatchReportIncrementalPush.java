package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.kafka.consumer.TopicPartitionIngestionInfo;
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
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBatchReportIncrementalPush {
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

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testBatchReportIncrementalPush() throws IOException {
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

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

}

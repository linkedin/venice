package com.linkedin.venice.ingestionHeartbeat;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.message.KafkaKey.HEART_BEAT;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToRecordSchema;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_COMPLETED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.LeaderCompleteState;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update (Write Compute) with a setup that has both the parent and child controllers.
 */
public class sendIngestionHeartBeatTest {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final String CLUSTER_NAME = "venice-cluster0";
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        3,
        1,
        3,
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

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT_MS)
  public void testSendIngestionHeartBeat(boolean isNativeReplicationEnabled, boolean isActiveActiveEnabled)
      throws IOException {
    final String storeName = Utils.getUniqueString("ingestionHeartBeatTest");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(INCREMENTAL_PUSH, true);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, NAME_RECORD_V1_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(500L)
              .setHybridOffsetLagThreshold(0L)
              .setPartitionCount(2)
              .setReplicationFactor(2)
              .setNativeReplicationEnabled(isNativeReplicationEnabled)
              .setActiveActiveReplicationEnabled(isActiveActiveEnabled);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      if (!isNativeReplicationEnabled && isActiveActiveEnabled) {
        assertTrue(
            updateStoreResponse.isError(),
            "Update store should fail when both native replication is disabled but active-active replication is enabled");
        return;
      }

      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // VPJ incremental push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);

      // Verify data pushed via incremental push using client
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

      // create consumer to consume from RT/VT to verify HB and Leader completed header
      for (int dc = 0; dc < NUMBER_OF_CHILD_DATACENTERS; dc++) {
        PubSubBrokerWrapper pubSubBrokerWrapper =
            childDatacenters.get(dc).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper();

        Properties properties = new Properties();
        properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
        PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
            .getConsumerAdapterFactory()
            .create(new VeniceProperties(properties), false, PubSubMessageDeserializer.getInstance(), "testConsumer");

        for (int partition = 0; partition < response.getPartitions(); partition++) {
          // 1. subscribe to RT partition to verify HB and leader completed header from HB
          pubSubConsumer.subscribe(
              new PubSubTopicPartitionImpl(
                  new PubSubTopicRepository().getTopic(Version.composeRealTimeTopic(storeName)),
                  partition),
              0);

          AtomicBoolean isHBFound = new AtomicBoolean(false);
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
                pubSubConsumer.poll(10 * Time.MS_PER_SECOND);
            for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> entry: messages
                .entrySet()) {
              List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessages = entry.getValue();
              for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: pubSubMessages) {
                if (Arrays.equals(message.getKey().getKey(), HEART_BEAT.getKey())) {
                  isHBFound.set(true);
                }
              }
              if (isHBFound.get()) {
                break;
              }
            }
            assertTrue(isHBFound.get(), "Heartbeat should be found in RT topic");
          });

          // 2. unsubscribe from RT topic and subscribe to VT to verify leader topic partition
          // receives the HB from RT, adds leader completed header and forwards it to VT
          pubSubConsumer.unSubscribe(
              new PubSubTopicPartitionImpl(
                  new PubSubTopicRepository().getTopic(Version.composeRealTimeTopic(storeName)),
                  partition));

          pubSubConsumer.subscribe(
              new PubSubTopicPartitionImpl(
                  new PubSubTopicRepository().getTopic(Version.composeKafkaTopic(storeName, 1)),
                  partition),
              0);

          isHBFound.set(false);
          AtomicBoolean isLeaderCompletionHeaderFound = new AtomicBoolean(false);
          AtomicBoolean isLeaderCompleted = new AtomicBoolean(false);
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
                pubSubConsumer.poll(10 * Time.MS_PER_SECOND);
            for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> entry: messages
                .entrySet()) {
              List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessages = entry.getValue();
              for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: pubSubMessages) {
                if (Arrays.equals(message.getKey().getKey(), HEART_BEAT.getKey())) {
                  isHBFound.set(true);
                }
                PubSubMessageHeaders pubSubMessageHeaders = message.getPubSubMessageHeaders();
                for (PubSubMessageHeader header: pubSubMessageHeaders.toList()) {
                  if (header.key().equals(VENICE_LEADER_COMPLETION_STATE_HEADER)) {
                    isLeaderCompletionHeaderFound.set(true);
                    if (LeaderCompleteState.valueOf(header.value()[0]) == LEADER_COMPLETED) {
                      isLeaderCompleted.set(true);
                      break;
                    }
                  }
                }
              }
              if (isLeaderCompleted.get()) {
                break;
              }
            }
            assertTrue(isHBFound.get(), "Heartbeat should be found in VT");
            assertTrue(isLeaderCompletionHeaderFound.get(), "Leader completed header should be found in VT");
            assertTrue(isLeaderCompleted.get(), "Leader completed header should be set to completed in VT");
          });
        }
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
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
    }
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }
}

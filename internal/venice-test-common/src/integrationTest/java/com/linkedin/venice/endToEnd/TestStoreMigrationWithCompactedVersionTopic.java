package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreMigrationTestUtil;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestStoreMigrationWithCompactedVersionTopic {
  private static final int TEST_TIMEOUT = 180 * Time.MS_PER_SECOND;
  private static final int PRODUCED_RECORD_COUNT = 200;
  private static final int UNIQUE_KEY_COUNT = 10;
  private static final int VALUE_PAYLOAD_SIZE = 12 * 1024;
  private static final String FABRIC = "dc-0";
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private static final PubSubTopicRepository PUBSUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper;
  private VeniceMultiClusterWrapper childRegion;
  private String sourceClusterName;
  private String destinationClusterName;
  private String parentControllerUrl;
  private String childControllerUrl;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "300000");

    Map<String, String> brokerConfiguration = new HashMap<>();
    brokerConfiguration.put("log.cleaner.enable", "true");
    brokerConfiguration.put("log.cleaner.backoff.ms", "100");
    brokerConfiguration.put("log.cleaner.min.cleanable.ratio", "0.01");
    brokerConfiguration.put("log.segment.bytes", String.valueOf(256 * 1024));
    brokerConfiguration.put("log.roll.ms", "1000");

    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(2)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(1)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .forkServer(false)
            .parentControllerProperties(parentControllerProperties)
            .additionalBrokerConfiguration(brokerConfiguration)
            .build();
    multiRegionWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);
    childRegion = multiRegionWrapper.getChildRegions().get(0);
    String[] clusterNames = childRegion.getClusterNames();
    Arrays.sort(clusterNames);
    sourceClusterName = clusterNames[0];
    destinationClusterName = clusterNames[1];
    parentControllerUrl = multiRegionWrapper.getControllerConnectString();
    childControllerUrl = childRegion.getControllerConnectString();
    IntegrationTestUtils.waitForParticipantStorePush(clusterNames, childControllerUrl);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(multiRegionWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchStoreMigrationReplaysCompactedVersionTopic() throws Exception {
    testStoreMigrationReplaysCompactedVersionTopic(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStoreMigrationReplaysCompactedVersionTopic() throws Exception {
    testStoreMigrationReplaysCompactedVersionTopic(true);
  }

  private void testStoreMigrationReplaysCompactedVersionTopic(boolean hybrid) throws Exception {
    String storeName = Utils.getUniqueString(hybrid ? "compactedHybridMigration" : "compactedBatchMigration");
    File inputDir = getTempDataDirectory();
    Properties pushJobProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionWrapper, "file:" + inputDir.getAbsolutePath(), storeName);
    UpdateStoreQueryParams storeParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(1)
            .setBlobTransferEnabled(false);
    if (hybrid) {
      storeParams.setHybridRewindSeconds(60).setHybridOffsetLagThreshold(1);
    }

    PubSubBrokerWrapper sourceBroker = childRegion.getPubSubBrokerWrapper();
    try (ControllerClient sourceParentController = IntegrationTestPushUtils
        .createStoreForJob(sourceClusterName, KEY_SCHEMA, VALUE_SCHEMA, pushJobProperties, storeParams)) {
      VersionCreationResponse versionCreationResponse = TestUtils.assertCommand(
          sourceParentController.requestTopicForWrites(
              storeName,
              1024,
              Version.PushType.BATCH,
              Version.guidBasedDummyPushId(),
              true,
              false,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1));
      assertEquals(versionCreationResponse.getVersion(), 1);
      try (VeniceWriter<String, String, byte[]> writer =
          childRegion.getClusters().get(sourceClusterName).getVeniceWriter(versionCreationResponse.getKafkaTopic())) {
        writer.broadcastStartOfPush(Collections.emptyMap());
        for (int index = 0; index < PRODUCED_RECORD_COUNT; index++) {
          writer
              .put(
                  "key_" + index % UNIQUE_KEY_COUNT,
                  getIncompressibleValue(index),
                  HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID)
              .get();
        }
        writer.broadcastEndOfPush(Collections.emptyMap(), Collections.singletonMap(0, (long) PRODUCED_RECORD_COUNT));
      }
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponse.getKafkaTopic(),
          sourceParentController,
          60,
          TimeUnit.SECONDS);
    }

    Map<Integer, Long> eopRecordCounts =
        IntegrationTestPushUtils.getEopPartitionRecordCounts(sourceBroker, storeName, 1, 1);
    assertEquals(eopRecordCounts.get(0).longValue(), PRODUCED_RECORD_COUNT);

    PubSubTopic versionTopic = PUBSUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1));
    TopicManager topicManager = childRegion.getLeaderController(sourceClusterName).getVeniceAdmin().getTopicManager();
    topicManager.updateTopicCompactionPolicy(versionTopic, true, 1, Optional.of(TimeUnit.SECONDS.toMillis(1)));
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertTrue(topicManager.isTopicCompactionEnabled(versionTopic)));

    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, () -> {
      int replayedRecordCount = getUserRecordCountBeforeEop(sourceBroker, versionTopic);
      assertTrue(
          replayedRecordCount < PRODUCED_RECORD_COUNT,
          "Expected Kafka compaction to reduce replay records below stale EOP prc " + PRODUCED_RECORD_COUNT
              + ", but got " + replayedRecordCount);
    });

    StoreMigrationTestUtil.startMigration(parentControllerUrl, storeName, sourceClusterName, destinationClusterName);
    try (ControllerClient destinationChildController =
        new ControllerClient(destinationClusterName, childControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        StoreInfo destinationStore = TestUtils.assertCommand(destinationChildController.getStore(storeName)).getStore();
        Assert.assertTrue(destinationStore.isMigrationDuplicateStore());
        Assert.assertEquals(destinationStore.getCurrentVersion(), 1);
        Assert.assertEquals(destinationStore.getVersion(1).get().getStatus(), VersionStatus.ONLINE);
      });
    }

    IntegrationTestPushUtils.assertBatchPushRecordCountSensors(
        childRegion.getClusters().get(destinationClusterName).getVeniceServers(),
        storeName,
        false,
        true);

    StoreMigrationTestUtil
        .completeMigration(parentControllerUrl, storeName, sourceClusterName, destinationClusterName, FABRIC);
    try (ControllerClient destinationChildController =
        new ControllerClient(destinationClusterName, childControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo destinationStore = TestUtils.assertCommand(destinationChildController.getStore(storeName)).getStore();
        Assert.assertEquals(destinationStore.getCurrentVersion(), 1);
        Assert.assertEquals(destinationStore.getVersion(1).get().getStatus(), VersionStatus.ONLINE);
      });
    }
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(childRegion.getClusters().get(destinationClusterName).getRandomRouterURL()))) {
      for (int keyIndex = 0; keyIndex < UNIQUE_KEY_COUNT; keyIndex++) {
        int lastRecordIndex = PRODUCED_RECORD_COUNT - UNIQUE_KEY_COUNT + keyIndex;
        assertEquals(
            client.get("key_" + keyIndex).get().toString(),
            getIncompressibleValue(lastRecordIndex),
            "Unexpected value after migrating compacted topic for key_" + keyIndex);
      }
    }
  }

  private static String getIncompressibleValue(int index) {
    // Deterministic high-entropy values force Kafka log segments to roll even when producer compression is enabled.
    byte[] payload = new byte[VALUE_PAYLOAD_SIZE];
    new Random(index).nextBytes(payload);
    return "value_" + index + "_" + Base64.getEncoder().encodeToString(payload);
  }

  private int getUserRecordCountBeforeEop(PubSubBrokerWrapper broker, PubSubTopic topic) {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, broker.getAddress());
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    try (PubSubConsumerAdapter consumer = broker.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(consumerProperties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(broker.getPubSubPositionTypeRegistry())
                .setConsumerName("compactedMigrationReplayCounter")
                .build())) {
      consumer.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST, false);
      int userRecordCount = 0;
      long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
      while (System.currentTimeMillis() < deadline) {
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledRecords = consumer.poll(1000);
        for (DefaultPubSubMessage message: polledRecords.getOrDefault(topicPartition, Collections.emptyList())) {
          if (!message.getKey().isControlMessage()) {
            userRecordCount++;
            continue;
          }
          KafkaMessageEnvelope envelope = message.getValue();
          ControlMessage controlMessage = (ControlMessage) envelope.payloadUnion;
          if (controlMessage.getControlMessageType() == ControlMessageType.END_OF_PUSH.getValue()) {
            return userRecordCount;
          }
        }
      }
      throw new AssertionError("Did not observe EOP while replaying compacted topic " + topic);
    }
  }
}

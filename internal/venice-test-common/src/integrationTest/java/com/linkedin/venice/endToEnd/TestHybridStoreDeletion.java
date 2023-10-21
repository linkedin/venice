package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHybridStoreDeletion {
  private static final Logger LOGGER = LogManager.getLogger(TestHybridStoreDeletion.class);
  public static final int STREAMING_RECORD_SIZE = 1024;
  public static final int NUMBER_OF_SERVERS = 1;

  private VeniceClusterWrapper veniceCluster;
  ZkServerWrapper parentZk = null;
  VeniceControllerWrapper parentController = null;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = setUpCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    parentController.close();
    parentZk.close();
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  private static VeniceClusterWrapper setUpCluster() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 1, 1, 1000000, false, false, extraProperties);

    // Add Venice Router
    Properties routerProperties = new Properties();
    cluster.addVeniceRouter(routerProperties);

    // Add Venice Server
    Properties serverProperties = new Properties();
    serverProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    serverProperties.setProperty(SSL_TO_KAFKA_LEGACY, "false");

    // Limit shared consumer thread pool size to 1.
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");
    serverProperties.setProperty(MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER, "1");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");

    // Set the max number of partitions to 1 so that we can test the partition-wise shared consumer assignment strategy.
    serverProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "1");
    serverProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      cluster.addVeniceServer(new Properties(), serverProperties);
    }

    return cluster;
  }

  /**
   * testHybridStoreRTDeletionWhileIngesting does the following:
   *
   * 1. Set up a Venice cluster with 1 controller, 1 router, and 1 server.
   * 2. Limit the shared consumer thread pool size to 1 on the server.
   * 3. Create two hybrid stores.
   * 4. Produce to the rt topic of the first store and allow the thread to produce some amount of data.
   * 5. Delete the rt topic of the first store and wait for the rt topic to be fully deleted.
   * 6. Produce to the rt topic of the second store with 10 key-value pairs.
   * 7. Check that the second store has all the records.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testHybridStoreRTDeletionWhileIngesting() {
    parentZk = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(
            veniceCluster.getClusterName(),
            parentZk,
            veniceCluster.getPubSubBrokerWrapper())
                .childControllers(new VeniceControllerWrapper[] { veniceCluster.getLeaderVeniceController() })
                .build());

    long streamingRewindSeconds = 25;
    long streamingMessageLag = 2;
    final String storeNameFirst = Utils.getUniqueString("hybrid-store-test-first");
    final String storeNameSecond = Utils.getUniqueString("hybrid-store-test-second");
    final String[] storeNames = new String[] { storeNameFirst, storeNameSecond };

    try (
        ControllerClient controllerClient =
            new ControllerClient(veniceCluster.getClusterName(), parentController.getControllerUrl());
        AvroGenericStoreClient<Object, Object> clientToSecondStore = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeNameSecond).setVeniceURL(veniceCluster.getRandomRouterURL()));
        TopicManager topicManager =
            IntegrationTestPushUtils
                .getTopicManagerRepo(
                    PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                    100,
                    0l,
                    veniceCluster.getPubSubBrokerWrapper(),
                    veniceCluster.getPubSubTopicRepository())
                .getLocalTopicManager()) {

      createStoresAndVersions(controllerClient, storeNames, streamingRewindSeconds, streamingMessageLag);

      // Produce to the rt topic of the first store and allow the thread to produce some amount of data.
      produceToStoreRTTopic(storeNameFirst, 200);

      // Delete the rt topic of the first store.
      controllerClient.deleteKafkaTopic(Version.composeRealTimeTopic(storeNameFirst));

      // Wait until the rt topic of the first store is fully deleted.
      PubSubTopic rtTopicFirst =
          veniceCluster.getPubSubTopicRepository().getTopic(Version.composeRealTimeTopic(storeNameFirst));
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        Assert.assertFalse(topicManager.containsTopic(rtTopicFirst));
      });

      // Produce to the rt topic of the second store with 10 key-value pairs.
      produceToStoreRTTopic(storeNameSecond, 10);

      // Check that the second store has all the records.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        try {
          for (int i = 1; i <= 10; i++) {
            checkLargeRecord(clientToSecondStore, i);
            LOGGER.info("Checked record {}", i);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    }
  }

  private void produceToStoreRTTopic(String storeName, int numOfRecords) {
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    for (int i = 1; i <= numOfRecords; i++) {
      sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
    }
    veniceProducer.stop();
  }

  private void checkLargeRecord(AvroGenericStoreClient client, int index)
      throws ExecutionException, InterruptedException {
    String key = Integer.toString(index);
    assert client != null;
    Object obj = client.get(key).get();
    String value = obj.toString();
    assertEquals(
        value.length(),
        STREAMING_RECORD_SIZE,
        "Expected a large record for key '" + key + "' but instead got: '" + value + "'.");

    String expectedChar = Integer.toString(index).substring(0, 1);
    for (int i = 0; i < value.length(); i++) {
      assertEquals(value.substring(i, i + 1), expectedChar);
    }
  }

  private void createStoresAndVersions(
      ControllerClient controllerClient,
      String[] storeNames,
      long streamingRewindSeconds,
      long streamingMessageLag) {
    for (String storeName: storeNames) {
      // Create store at parent, make it a hybrid store.
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setPartitionCount(1)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      // There should be no version on the store yet.
      assertEquals(
          controllerClient.getStore(storeName).getStore().getCurrentVersion(),
          0,
          "The newly created store must have a current version of 0");

      // Create a new version, and do an empty push for that version.
      VersionCreationResponse vcr =
          controllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      int versionNumber = vcr.getVersion();
      assertNotEquals(versionNumber, 0, "requesting a topic for a push should provide a non zero version number");
    }
  }
}

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
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
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

  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = setUpCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  private static VeniceClusterWrapper setUpCluster() {
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

    return ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .extraProperties(serverProperties)
            .build());
  }

  /**
   * testHybridStoreRTDeletionWhileIngesting does the following:
   *
   * 1. Set up a Venice cluster with 1 parent controller, 1 child controller, 1 router, and 1 server.
   * 2. Limit the shared consumer thread pool size to 1 on the server.
   * 3. Create two hybrid stores.
   * 4. Produce to the rt topic of the first store and allow the thread to produce some amount of data.
   * 5. Delete the rt topic of the first store and wait for the rt topic to be fully deleted.
   * 6. Produce to the rt topic of the second store with 10 key-value pairs.
   * 7. Check that the second store has all the records.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testHybridStoreRTDeletionWhileIngesting() {
    long streamingRewindSeconds = 25;
    long streamingMessageLag = 2;
    final String storeNameFirst = Utils.getUniqueString("hybrid-store-test-first");
    final String storeNameSecond = Utils.getUniqueString("hybrid-store-test-second");
    final String[] storeNames = new String[] { storeNameFirst, storeNameSecond };
    String[] realTimeTopicNames = new String[storeNames.length];

    try (TopicManager topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                100,
                0l,
                veniceCluster.getPubSubBrokerWrapper(),
                veniceCluster.getPubSubTopicRepository())
            .getLocalTopicManager()) {

      createStoresAndVersions(storeNames, streamingRewindSeconds, streamingMessageLag);

      veniceCluster.useControllerClient(controllerClient -> {
        realTimeTopicNames[0] =
            Utils.getRealTimeTopicName(TestUtils.assertCommand(controllerClient.getStore(storeNameFirst)).getStore());
        realTimeTopicNames[1] =
            Utils.getRealTimeTopicName(TestUtils.assertCommand(controllerClient.getStore(storeNameSecond)).getStore());
      });

      // Wait until the rt topic of the first store is fully deleted.
      PubSubTopic rtTopicFirst1 = veniceCluster.getPubSubTopicRepository().getTopic(realTimeTopicNames[0]);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        Assert.assertTrue(topicManager.containsTopic(rtTopicFirst1));
      });

      // Produce to the rt topic of the first store and allow the thread to produce some amount of data.
      produceToStoreRTTopic(storeNameFirst, 200);

      // Delete the rt topic of the first store.
      topicManager
          .ensureTopicIsDeletedAndBlock(veniceCluster.getPubSubTopicRepository().getTopic(realTimeTopicNames[0]));

      // Wait until the rt topic of the first store is fully deleted.
      PubSubTopic rtTopicFirst = veniceCluster.getPubSubTopicRepository().getTopic(realTimeTopicNames[0]);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        Assert.assertFalse(topicManager.containsTopic(rtTopicFirst));
      });

      // Produce to the rt topic of the second store with 10 key-value pairs.
      produceToStoreRTTopic(storeNameSecond, 10);

      try (AvroGenericStoreClient<Object, Object> clientToSecondStore = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeNameSecond).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
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

  private void createStoresAndVersions(String[] storeNames, long streamingRewindSeconds, long streamingMessageLag) {
    veniceCluster.useControllerClient(controllerClient -> {
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
        controllerClient
            .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, 60L * Time.MS_PER_SECOND);
      }
    });
  }
}

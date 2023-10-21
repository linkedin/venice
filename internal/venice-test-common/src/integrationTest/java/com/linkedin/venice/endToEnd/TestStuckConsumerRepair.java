package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_EXISTING_TOPIC_CHECK_RETRY_INTERNAL_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_NON_EXISTING_TOPIC_INGESTION_TASK_KILL_THRESHOLD_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_INTERVAL_SECOND;
import static com.linkedin.venice.ConfigKeys.SERVER_STUCK_CONSUMER_REPAIR_THRESHOLD_SECOND;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStuckConsumerRepair {
  private static final Logger LOGGER = LogManager.getLogger(TestStuckConsumerRepair.class);
  public static final int STREAMING_RECORD_SIZE = 1024;

  private VeniceClusterWrapper sharedVenice;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    sharedVenice = setUpCluster();
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
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");
    serverProperties.setProperty(SERVER_STUCK_CONSUMER_REPAIR_ENABLED, "true");
    serverProperties.setProperty(SERVER_STUCK_CONSUMER_REPAIR_INTERVAL_SECOND, "1");
    serverProperties.setProperty(SERVER_STUCK_CONSUMER_REPAIR_THRESHOLD_SECOND, "2");
    serverProperties.setProperty(SERVER_NON_EXISTING_TOPIC_INGESTION_TASK_KILL_THRESHOLD_SECOND, "5");
    serverProperties.setProperty(SERVER_NON_EXISTING_TOPIC_CHECK_RETRY_INTERNAL_SECOND, "1");

    serverProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4");
    serverProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    cluster.addVeniceServer(new Properties(), serverProperties);

    return cluster;
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
  }

  private void checkLargeRecord(AvroGenericStoreClient client, int index)
      throws ExecutionException, InterruptedException {
    String key = Integer.toString(index);
    String value = client.get(key).get().toString();
    assertEquals(
        value.length(),
        STREAMING_RECORD_SIZE,
        "Expected a large record for key '" + key + "' but instead got: '" + value + "'.");

    String expectedChar = Integer.toString(index).substring(0, 1);
    for (int i = 0; i < value.length(); i++) {
      assertEquals(value.substring(i, i + 1), expectedChar);
    }
  }

  /**
   * This test verifies that the stuck consumer repair logic kicks in when the consumer is stuck. It does the following
   * steps:
   * 1. Set up a Venice cluster with 1 controller, 1 router, and 1 server.
   * 2. Create a hybrid store with rewind seconds set to 120 seconds and offset lag threshold set to 2.
   * 3. Run a VPJ job to push 100 records to the store.
   * 4. Verify that the records are pushed successfully.
   * 5. Write 10 streaming records to the store's rt topic.
   * 6. Verify that the streaming records are pushed successfully by reading and verifying all of them.
   * 7. Delete the v1 topic to simulate the producer stuck issue.
   * 8. Write 80 more streaming records to the store's rt topic. Consumer thread will be blocked when processing them.
   * 9. Verify that the stuck consumer repair logic kicks in and the stuck consumer is repaired.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testStuckConsumerRepair() throws Exception {
    SystemProducer veniceProducer = null;

    VeniceClusterWrapper venice = sharedVenice;
    try {
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("hybrid-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
      Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

      try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
          TopicManager topicManager =
              IntegrationTestPushUtils
                  .getTopicManagerRepo(
                      PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                      100,
                      0l,
                      venice.getPubSubBrokerWrapper(),
                      sharedVenice.getPubSubTopicRepository())
                  .getLocalTopicManager()) {

        Cache cacheNothingCache = Mockito.mock(Cache.class);
        Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
        topicManager.setTopicConfigCache(cacheNothingCache);

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(120).setHybridOffsetLagThreshold(streamingMessageLag));

        Assert.assertFalse(response.isError());

        // Do a VPJ push
        runVPJ(vpjProperties, 1, controllerClient);

        // Verify some records (note, records 1-100 have been pushed)
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              assertNotNull(value, "Key " + i + " should not be missing!");
              assertEquals(value.toString(), "test_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // write streaming records
        veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
        for (int i = 1; i <= 10; i++) {
          // The batch values are small, but the streaming records are "big" (i.e.: not that big, but bigger than
          // the server's max configured chunk size). In the scenario where chunking is disabled, the server's
          // max chunk size is not altered, and thus this will be under threshold.
          sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
        }

        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          try {
            for (int i = 1; i <= 10; ++i) {
              checkLargeRecord(client, i);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Delete v1 topic to simulate producer stuck issue
        String topicForV1 = Version.composeKafkaTopic(storeName, 1);
        topicManager.ensureTopicIsDeletedAndBlock(sharedVenice.getPubSubTopicRepository().getTopic(topicForV1));
        LOGGER.info("Topic: {} has been deleted", topicForV1);

        // Sending more streaming records, as it will block the consumer thread when processing them.
        for (int i = 20; i <= 100; i++) {
          sendCustomSizeStreamingRecord(veniceProducer, storeName, i, 1024);
        }

        // Verify that the stuck consumer repair logic does kick in
        List<MetricsRepository> serverMetricRepos = new ArrayList<>();
        for (VeniceServerWrapper server: venice.getVeniceServers()) {
          serverMetricRepos.add(server.getMetricsRepository());
        }
        List<Boolean> serversHasStuckConsumerRepair = new ArrayList<>(serverMetricRepos.size());
        List<Boolean> serversHasIngestionTaskRepair = new ArrayList<>(serverMetricRepos.size());
        List<Boolean> serversHasRepairFailure = new ArrayList<>(serverMetricRepos.size());
        for (int i = 0; i < serverMetricRepos.size(); i++) {
          serversHasStuckConsumerRepair.add(false);
          serversHasIngestionTaskRepair.add(false);
          serversHasRepairFailure.add(false);
        }
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          for (int i = 0; i < serverMetricRepos.size(); i++) {
            if (serverMetricRepos.get(i)
                .metrics()
                .get(".StuckConsumerRepair--stuck_consumer_found.OccurrenceRate")
                .value() > 0f) {
              serversHasStuckConsumerRepair.set(i, true);
            }
            if (serverMetricRepos.get(i)
                .metrics()
                .get(".StuckConsumerRepair--ingestion_task_repair.OccurrenceRate")
                .value() > 0f) {
              serversHasIngestionTaskRepair.set(i, true);
            }
            if (serverMetricRepos.get(i)
                .metrics()
                .get(".StuckConsumerRepair--repair_failure.OccurrenceRate")
                .value() > 0f) {
              serversHasRepairFailure.set(i, true);
            }
            assertTrue(serversHasStuckConsumerRepair.get(i), "Server " + i + " do not have stuck consumer");
            assertTrue(serversHasIngestionTaskRepair.get(i), "Server " + i + " did not repair ingestion task");
            assertFalse(serversHasRepairFailure.get(i), "Server " + i + " failed during ingestion task repair");
          }
        });
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

}

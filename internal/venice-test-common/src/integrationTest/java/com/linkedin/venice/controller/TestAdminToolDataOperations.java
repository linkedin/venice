package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.IS_DARK_CLUSTER;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.endToEnd.TestHybrid;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.AbstractMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminToolDataOperations {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final Logger LOGGER = LogManager.getLogger(TestAdminToolDataOperations.class);

  // Constants for repush store test
  private static final long TEST_LOG_COMPACTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10); // ms

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    String regionName = "dc-0";
    properties.setProperty(LOCAL_REGION_NAME, regionName);
    properties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    properties.setProperty(TOPIC_CLEANUP_DELAY_FACTOR, "0");

    // repushStore() configs
    properties.setProperty(REPUSH_ORCHESTRATOR_CLASS_NAME, TestHybrid.TestRepushOrchestratorImpl.class.getName());
    properties.setProperty(LOG_COMPACTION_ENABLED, "true");

    // dark cluster configs
    properties.setProperty(IS_DARK_CLUSTER, "true");

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .regionName(regionName)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(100000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(properties)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT * 4)
  public void testWipeClusterCommand() throws Exception {
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, venice.getLeaderVeniceController().getControllerUrl())) {
      // Create 2 stores. Store 1 has 2 versions
      String testStoreName1 = Utils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse =
          controllerClient.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      VersionCreationResponse versionCreationResponse =
          controllerClient.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      versionCreationResponse = controllerClient.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-2"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());

      String testStoreName2 = Utils.getUniqueString("test-store");
      newStoreResponse = controllerClient.createNewStore(testStoreName2, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());

      // Delete a version
      String[] wipeClusterArgs1 = { "--wipe-cluster", "--url", venice.getLeaderVeniceController().getControllerUrl(),
          "--cluster", clusterName, "--fabric", "dc-0", "--store", testStoreName1, "--version", "1" };
      AdminTool.main(wipeClusterArgs1);
      StoreResponse storeResponse = controllerClient.getStore(testStoreName1);
      Assert.assertNotNull(storeResponse.getStore());
      Assert.assertFalse(storeResponse.getStore().getVersion(1).isPresent());
      Assert.assertTrue(storeResponse.getStore().getVersion(2).isPresent());

      // Delete a store
      String[] wipeClusterArgs2 = { "--wipe-cluster", "--url", venice.getLeaderVeniceController().getControllerUrl(),
          "--cluster", clusterName, "--fabric", "dc-0", "--store", testStoreName1 };
      AdminTool.main(wipeClusterArgs2);
      storeResponse = controllerClient.getStore(testStoreName1);
      Assert.assertNull(storeResponse.getStore());

      // Wipe a cluster
      String[] wipeClusterArgs3 = { "--wipe-cluster", "--url", venice.getLeaderVeniceController().getControllerUrl(),
          "--cluster", clusterName, "--fabric", "dc-0" };
      AdminTool.main(wipeClusterArgs3);
      MultiStoreResponse multiStoreResponse = controllerClient.queryStoreList(false);
      Assert.assertEquals(multiStoreResponse.getStores().length, 0);

      // Wait until all topics are indeed removed from Kafka service.
      PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

      PubSubTopic testStoreTopic1 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(testStoreName1, 1));
      PubSubTopic testStoreTopic2 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(testStoreName1, 2));
      PubSubTopic testStoreTopic3 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(testStoreName2, 1));

      TopicManager topicManager = venice.getLeaderVeniceController().getVeniceAdmin().getTopicManager();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        assertFalse(topicManager.containsTopicWithExpectationAndRetry(testStoreTopic1, 3, false));
        assertFalse(topicManager.containsTopicWithExpectationAndRetry(testStoreTopic2, 3, false));
        assertFalse(topicManager.containsTopicWithExpectationAndRetry(testStoreTopic3, 3, false));
      });

      // Redo fabric buildup. Create the store and version again.
      newStoreResponse = controllerClient.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      versionCreationResponse = controllerClient.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      Assert.assertEquals(versionCreationResponse.getVersion(), 1);
    }
  }

  /** similar test logic to {@link TestHybrid#testHybridStoreLogCompaction()} & shares {@link com.linkedin.venice.endToEnd.TestHybrid.TestRepushOrchestratorImpl} */
  @Test(timeOut = TEST_TIMEOUT)
  public void testRepushStoreCommand() throws Exception {
    // create test store
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
        .setHybridRewindSeconds(2000000)
        .setHybridOffsetLagThreshold(0)
        .setPartitionCount(2);
    String storeName = Utils.getUniqueString("repush-test-store");
    venice.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });
    venice.createVersion(
        storeName,
        DEFAULT_KEY_SCHEMA,
        DEFAULT_VALUE_SCHEMA,
        IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));

    // Test: send admin command
    String[] repushStoreArgs =
        { "--repush-store", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--store", storeName };
    AdminTool.main(repushStoreArgs);

    // Validate repush triggered
    try {
      if (TestHybrid.TestRepushOrchestratorImpl.getLatch().await(TEST_LOG_COMPACTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
        LOGGER.info("Log compaction job triggered");
      }
    } catch (InterruptedException e) {
      LOGGER.error("Log compaction job failed");
      throw new RuntimeException(e);
    }
  }
}

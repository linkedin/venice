package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.Arg;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminToolEndToEnd {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(LOCAL_REGION_NAME, "dc-0");
    properties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    properties.setProperty(TOPIC_CLEANUP_DELAY_FACTOR, "0");
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 100000, false, false, properties);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateClusterConfig() throws Exception {
    ZkClient zkClient = ZkClientFactory.newZkClient(venice.getZk().getAddress());
    HelixAdapterSerializer adapterSerializer = new HelixAdapterSerializer();
    HelixReadOnlyLiveClusterConfigRepository liveClusterConfigRepository =
        new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapterSerializer, clusterName);

    String regionName = "dc-0";
    int kafkaFetchQuota = 1000;

    Assert.assertNotEquals(
        liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
        kafkaFetchQuota);

    String[] adminToolArgs = { "--update-cluster-config", "--url",
        venice.getLeaderVeniceController().getControllerUrl(), "--cluster", clusterName, "--fabric", regionName,
        "--" + Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND.getArgName(), String.valueOf(kafkaFetchQuota) };
    AdminTool.main(adminToolArgs);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertEquals(
          liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
          kafkaFetchQuota);
      Assert.assertTrue(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    String[] disallowStoreMigrationArg =
        { "--update-cluster-config", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
            clusterName, "--" + Arg.ALLOW_STORE_MIGRATION.getArgName(), String.valueOf(false) };
    AdminTool.main(disallowStoreMigrationArg);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertFalse(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    try {
      String[] startMigrationArgs = { "--migrate-store", "--url", venice.getLeaderVeniceController().getControllerUrl(),
          "--store", "anyStore", "--cluster-src", clusterName, "--cluster-dest", "anyCluster" };
      AdminTool.main(startMigrationArgs);
      Assert.fail("Store migration should be denied");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("does not allow store migration"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
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
        assertFalse(topicManager.containsTopic(testStoreTopic1));
        assertFalse(topicManager.containsTopic(testStoreTopic2));
        assertFalse(topicManager.containsTopic(testStoreTopic3));
      });

      // Redo fabric buildup. Create the store and version again.
      newStoreResponse = controllerClient.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      versionCreationResponse = controllerClient.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      Assert.assertEquals(versionCreationResponse.getVersion(), 1);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNodeReplicasReadinessCommand() throws Exception {
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String[] nodeReplicasReadinessArgs =
        { "--node-replicas-readiness", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
            clusterName, "--storage-node", Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort()) };
    AdminTool.main(nodeReplicasReadinessArgs);
  }
}

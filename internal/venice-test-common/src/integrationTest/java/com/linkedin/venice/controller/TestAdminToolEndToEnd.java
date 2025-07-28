package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.Arg;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.endToEnd.TestHybrid;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.AbstractMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminToolEndToEnd {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final Logger LOGGER = LogManager.getLogger(TestAdminToolEndToEnd.class);

  String clusterName;
  VeniceClusterWrapper venice;

  // Constants for repush store test
  private static final long TEST_LOG_COMPACTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10); // ms

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

  @Test(timeOut = TEST_TIMEOUT)
  public void testNodeReplicasReadinessCommand() throws Exception {
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String[] nodeReplicasReadinessArgs =
        { "--node-replicas-readiness", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
            clusterName, "--storage-node", Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort()) };
    AdminTool.main(nodeReplicasReadinessArgs);
  }

  @Test(timeOut = 4 * TEST_TIMEOUT)
  public void testUpdateAdminOperationVersion() throws Exception {
    Long defaultVersion = -1L;
    Long newVersion = 80L;
    String storeName = Utils.getUniqueString("test-store");
    try (VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                .numberOfClusters(1)
                .numberOfParentControllers(1)
                .numberOfChildControllers(1)
                .numberOfServers(1)
                .numberOfRouters(1)
                .replicationFactor(1)
                .build());) {
      String clusterName = venice.getClusterNames()[0];

      // Get the parent con†roller
      VeniceControllerWrapper parentController = venice.getParentControllers().get(0);
      ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

      // Verify the original metadata - default value
      AdminTopicMetadataResponse originalMetadata = parentControllerClient.getAdminTopicMetadata(Optional.empty());
      Assert.assertEquals(originalMetadata.getAdminOperationProtocolVersion(), (long) defaultVersion);
      Assert.assertEquals(originalMetadata.getExecutionId(), (long) defaultVersion);
      Assert.assertEquals(originalMetadata.getOffset(), (long) defaultVersion);
      Assert.assertEquals(originalMetadata.getUpstreamOffset(), (long) defaultVersion);

      // Create store
      NewStoreResponse newStoreResponse =
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());

      // Update store config
      ControllerResponse updateStore =
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
      Assert.assertFalse(updateStore.isError());

      // Check the baseline metadata
      AdminTopicMetadataResponse metdataAfterStoreCreation =
          parentControllerClient.getAdminTopicMetadata(Optional.empty());
      long baselineExecutionId = metdataAfterStoreCreation.getExecutionId();
      long baselineOffset = metdataAfterStoreCreation.getOffset();
      long baselineUpstreamOffset = metdataAfterStoreCreation.getUpstreamOffset();
      long baselineAdminVersion = metdataAfterStoreCreation.getAdminOperationProtocolVersion();

      // Execution id and offset should be positive now since we have created a store and updated the store config
      Assert.assertEquals(baselineAdminVersion, (long) defaultVersion);
      Assert.assertTrue(baselineExecutionId > 0);
      Assert.assertTrue(baselineOffset > 0);
      Assert.assertEquals(baselineUpstreamOffset, (long) defaultVersion);

      // Update the admin operation version to newVersion - 80
      String[] updateAdminOperationVersionArgs =
          { "--update-admin-operation-protocol-version", "--url", parentController.getControllerUrl(), "--cluster",
              clusterName, "--admin-operation-protocol-version", newVersion.toString() };

      AdminTool.main(updateAdminOperationVersionArgs);

      // Verify the admin operation metadata version is updated and the remaining data is unchanged
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        AdminTopicMetadataResponse updatedMetadata = parentControllerClient.getAdminTopicMetadata(Optional.empty());
        Assert.assertEquals(updatedMetadata.getAdminOperationProtocolVersion(), (long) newVersion);
        Assert.assertEquals(updatedMetadata.getExecutionId(), baselineExecutionId);
        Assert.assertEquals(updatedMetadata.getOffset(), baselineOffset);
        Assert.assertEquals(updatedMetadata.getUpstreamOffset(), baselineUpstreamOffset);
      });
    }
  }
}

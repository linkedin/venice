package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PUBSUB_HEALTH_MONITOR_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PUBSUB_HEALTH_PROBE_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_PUBSUB_PARTITION_PAUSE_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerForStream;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithKeyPrefix;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.PubSubHealthMonitor;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Multi-region, multi-replica integration tests for PubSub health-based partition pause/resume.
 *
 * <p>Uses a 2-region (dc-0, dc-1) setup with active-active replication, RF=2, and 2 servers
 * per region — so each partition has a leader and a follower replica in each region. Tests:
 * <ul>
 *   <li>Region-isolated failure with leader/follower: all replicas in one region pause</li>
 *   <li>Leader-only pause within a region while follower and other region continue</li>
 *   <li>Follower-only pause within a region while leader and other region continue</li>
 *   <li>Cross-region data flow during regional outage with catch-up on resume</li>
 *   <li>Both regions fail and recover independently</li>
 * </ul>
 */
public class TestPubSubHealthPartitionPauseResumeMultiRegion {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int SERVERS_PER_REGION = 2;
  private static final long PUSH_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0ControllerClient;
  private ControllerClient dc1ControllerClient;
  private String clusterName;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    clusterName = CLUSTER_NAMES[0];

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, "1");
    serverProperties.put(SERVER_PUBSUB_HEALTH_MONITOR_ENABLED, "true");
    serverProperties.put(SERVER_PUBSUB_PARTITION_PAUSE_ENABLED, "true");
    serverProperties.put(SERVER_PUBSUB_HEALTH_PROBE_INTERVAL_SECONDS, "2");

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(SERVERS_PER_REGION)
            .numberOfRouters(1)
            .replicationFactor(SERVERS_PER_REGION)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);

    multiRegionWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionWrapper.getChildRegions();

    String parentControllerURLs = multiRegionWrapper.getParentControllers()
        .stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dc0ControllerClient = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    dc1ControllerClient = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(dc0ControllerClient);
    Utils.closeQuietlyWithErrorLogged(dc1ControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionWrapper);
  }

  /**
   * Creates a hybrid AA store, does an empty push, waits for the version to be online in both
   * regions, configures health monitor probe topics, and identifies leader/follower in each region.
   */
  private RegionTestContext setupHybridAAStore(String storeName) throws Exception {
    assertCommand(
        parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString()));
    updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false));

    ControllerResponse response = assertCommand(
        parentControllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, PUSH_TIMEOUT));
    assertTrue(response instanceof JobStatusQueryResponse, "Should get JobStatusQueryResponse");
    int versionNumber = ((JobStatusQueryResponse) response).getVersion();
    String versionTopic = Version.composeKafkaTopic(storeName, versionNumber);

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      StoreResponse dc0Response = assertCommand(dc0ControllerClient.getStore(storeName));
      assertEquals(dc0Response.getStore().getCurrentVersion(), versionNumber, "dc-0 should have version online");
      StoreResponse dc1Response = assertCommand(dc1ControllerClient.getStore(storeName));
      assertEquals(dc1Response.getStore().getCurrentVersion(), versionNumber, "dc-1 should have version online");
    });

    // Wait for all SITs to be running and configure probe topics
    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    VeniceClusterWrapper[] regionClusters = new VeniceClusterWrapper[NUMBER_OF_CHILD_DATACENTERS];

    for (int regionIdx = 0; regionIdx < NUMBER_OF_CHILD_DATACENTERS; regionIdx++) {
      final int idx = regionIdx;
      VeniceClusterWrapper cluster = childDatacenters.get(idx).getClusters().get(clusterName);
      regionClusters[idx] = cluster;

      for (VeniceServerWrapper serverWrapper: cluster.getVeniceServers()) {
        KafkaStoreIngestionService ksis = serverWrapper.getVeniceServer().getKafkaStoreIngestionService();

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          StoreIngestionTask sit = ksis.getStoreIngestionTask(versionTopic);
          assertNotNull(sit, "SIT should exist in dc-" + idx + " server:" + serverWrapper.getPort());
          assertTrue(sit.isRunning(), "SIT should be running in dc-" + idx + " server:" + serverWrapper.getPort());
        });

        PubSubHealthMonitor healthMonitor = ksis.getPubSubHealthMonitor();
        assertNotNull(healthMonitor, "Health monitor should be configured in dc-" + idx);
        healthMonitor.setProbeTopic(topicRepo.getTopic(versionTopic));
      }
    }

    // Identify leader/follower in each region
    StoreIngestionTask[] leaderSits = new StoreIngestionTask[NUMBER_OF_CHILD_DATACENTERS];
    StoreIngestionTask[] followerSits = new StoreIngestionTask[NUMBER_OF_CHILD_DATACENTERS];

    for (int regionIdx = 0; regionIdx < NUMBER_OF_CHILD_DATACENTERS; regionIdx++) {
      final int idx = regionIdx;
      VeniceClusterWrapper cluster = regionClusters[idx];

      // Wait for leader assignment in this region
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        HelixExternalViewRepository routingDataRepo = cluster.getLeaderVeniceController()
            .getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getRoutingDataRepository();
        Instance leader = routingDataRepo.getLeaderInstance(versionTopic, 0);
        assertNotNull(leader, "Leader should be assigned in dc-" + idx);
      });

      HelixExternalViewRepository routingDataRepo = cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository();
      Instance leaderInstance = routingDataRepo.getLeaderInstance(versionTopic, 0);

      for (VeniceServerWrapper serverWrapper: cluster.getVeniceServers()) {
        StoreIngestionTask sit =
            serverWrapper.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(versionTopic);
        if (serverWrapper.getPort() == leaderInstance.getPort()) {
          leaderSits[idx] = sit;
        } else {
          followerSits[idx] = sit;
        }
      }
      assertNotNull(leaderSits[idx], "Should find leader in dc-" + idx);
      assertNotNull(followerSits[idx], "Should find follower in dc-" + idx);
    }

    // Verify leader/follower PCS states
    for (int regionIdx = 0; regionIdx < NUMBER_OF_CHILD_DATACENTERS; regionIdx++) {
      final int idx = regionIdx;
      final StoreIngestionTask leaderSit = leaderSits[idx];
      final StoreIngestionTask followerSit = followerSits[idx];
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, true, () -> {
        PartitionConsumptionState leaderPcs = leaderSit.getPartitionConsumptionState(0);
        assertNotNull(leaderPcs, "Leader PCS should exist in dc-" + idx);
        assertEquals(
            leaderPcs.getLeaderFollowerState(),
            LeaderFollowerStateType.LEADER,
            "dc-" + idx + " leader should be in LEADER state");

        PartitionConsumptionState followerPcs = followerSit.getPartitionConsumptionState(0);
        assertNotNull(followerPcs, "Follower PCS should exist in dc-" + idx);
        assertEquals(
            followerPcs.getLeaderFollowerState(),
            LeaderFollowerStateType.STANDBY,
            "dc-" + idx + " follower should be in STANDBY state");
      });
    }

    return new RegionTestContext(
        leaderSits[0],
        followerSits[0],
        leaderSits[1],
        followerSits[1],
        regionClusters[0],
        regionClusters[1],
        storeName,
        versionTopic);
  }

  private void simulatePubSubFailureAndWaitForPause(StoreIngestionTask sit, String label) {
    sit.setLastConsumerException(new PubSubClientException("Simulated broker failure: " + label));
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, true, () -> {
      Set<Integer> pausedPartitions = sit.getPubSubHealthPausedPartitions();
      assertFalse(pausedPartitions.isEmpty(), label + " should have paused partitions");
      assertTrue(sit.isRunning(), label + " SIT should still be running (not killed)");
    });
  }

  private void waitForResume(StoreIngestionTask sit, String label) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Set<Integer> pausedPartitions = sit.getPubSubHealthPausedPartitions();
      assertTrue(pausedPartitions.isEmpty(), label + " should have all partitions resumed");
      assertTrue(sit.isRunning(), label + " SIT should still be running after recovery");
    });
  }

  /**
   * Pauses both leader and follower in a region (simulating a region-wide Kafka outage).
   */
  private void pauseEntireRegion(RegionTestContext ctx, int regionIdx) {
    if (regionIdx == 0) {
      simulatePubSubFailureAndWaitForPause(ctx.dc0LeaderSit, "dc-0 leader");
      simulatePubSubFailureAndWaitForPause(ctx.dc0FollowerSit, "dc-0 follower");
    } else {
      simulatePubSubFailureAndWaitForPause(ctx.dc1LeaderSit, "dc-1 leader");
      simulatePubSubFailureAndWaitForPause(ctx.dc1FollowerSit, "dc-1 follower");
    }
  }

  /**
   * Waits for both leader and follower in a region to resume.
   */
  private void waitForRegionResume(RegionTestContext ctx, int regionIdx) {
    if (regionIdx == 0) {
      waitForResume(ctx.dc0LeaderSit, "dc-0 leader");
      waitForResume(ctx.dc0FollowerSit, "dc-0 follower");
    } else {
      waitForResume(ctx.dc1LeaderSit, "dc-1 leader");
      waitForResume(ctx.dc1FollowerSit, "dc-1 follower");
    }
  }

  private void assertRegionNotPaused(RegionTestContext ctx, int regionIdx) {
    StoreIngestionTask leaderSit = regionIdx == 0 ? ctx.dc0LeaderSit : ctx.dc1LeaderSit;
    StoreIngestionTask followerSit = regionIdx == 0 ? ctx.dc0FollowerSit : ctx.dc1FollowerSit;
    String dc = "dc-" + regionIdx;
    assertTrue(leaderSit.getPubSubHealthPausedPartitions().isEmpty(), dc + " leader should not be paused");
    assertTrue(followerSit.getPubSubHealthPausedPartitions().isEmpty(), dc + " follower should not be paused");
    assertTrue(leaderSit.isRunning(), dc + " leader SIT should be running");
    assertTrue(followerSit.isRunning(), dc + " follower SIT should be running");
  }

  /**
   * Test 1: Region-isolated failure pauses both leader and follower in that region.
   * Pause all replicas in dc-0, verify dc-1's leader and follower are unaffected,
   * data is still readable from dc-1, then dc-0 recovers.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testRegionIsolatedPauseAndResume() throws Exception {
    String storeName = Utils.getUniqueString("region-isolated-test");
    RegionTestContext ctx = setupHybridAAStore(storeName);

    VeniceSystemProducer dc0Producer = getSamzaProducerForStream(multiRegionWrapper, 0, storeName);
    try {
      for (int i = 0; i < 10; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "data-", i);
      }

      String dc1RouterUrl = ctx.dc1Cluster.getRandomRouterURL();
      ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc1RouterUrl);
      try (
          AvroGenericStoreClient<String, Object> dc1Client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc1Client.get("data-" + i).get();
            assertNotNull(value, "Record 'data-" + i + "' should be readable from dc-1");
            assertEquals(value.toString(), "stream_" + i);
          }
        });

        // Pause entire dc-0 (both leader and follower)
        pauseEntireRegion(ctx, 0);

        // dc-1 leader and follower should NOT be affected
        assertRegionNotPaused(ctx, 1);

        // dc-1 should still serve data
        for (int i = 0; i < 10; i++) {
          Object value = dc1Client.get("data-" + i).get();
          assertNotNull(value, "dc-1 should still serve data while dc-0 is paused");
        }

        // Wait for dc-0 to resume (both leader and follower)
        waitForRegionResume(ctx, 0);

        String dc0RouterUrl = ctx.dc0Cluster.getRandomRouterURL();
        ClientConfig dc0ClientConfig = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl);
        try (AvroGenericStoreClient<String, Object> dc0Client =
            ClientFactory.getAndStartGenericAvroClient(dc0ClientConfig)) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
            for (int i = 0; i < 10; i++) {
              Object value = dc0Client.get("data-" + i).get();
              assertNotNull(value, "dc-0 should serve data after recovery");
              assertEquals(value.toString(), "stream_" + i);
            }
          });
        }
      }
    } finally {
      dc0Producer.stop();
    }
  }

  /**
   * Test 2: Cross-region data flow during regional outage.
   * Pause entire dc-0, write data to dc-1's RT, resume dc-0,
   * verify dc-0 catches up on the replicated data.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testCrossRegionDataFlowDuringPause() throws Exception {
    String storeName = Utils.getUniqueString("cross-region-test");
    RegionTestContext ctx = setupHybridAAStore(storeName);

    VeniceSystemProducer dc0Producer = getSamzaProducerForStream(multiRegionWrapper, 0, storeName);
    VeniceSystemProducer dc1Producer = getSamzaProducerForStream(multiRegionWrapper, 1, storeName);
    try {
      for (int i = 0; i < 5; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "dc0-initial-", i);
      }

      String dc1RouterUrl = ctx.dc1Cluster.getRandomRouterURL();
      ClientConfig dc1Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc1RouterUrl);
      try (AvroGenericStoreClient<String, Object> dc1Client = ClientFactory.getAndStartGenericAvroClient(dc1Config)) {

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 5; i++) {
            Object value = dc1Client.get("dc0-initial-" + i).get();
            assertNotNull(value, "dc-0 initial data should reach dc-1");
          }
        });

        pauseEntireRegion(ctx, 0);

        for (int i = 0; i < 10; i++) {
          sendStreamingRecordWithKeyPrefix(dc1Producer, storeName, "dc1-during-pause-", i);
        }

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc1Client.get("dc1-during-pause-" + i).get();
            assertNotNull(value, "dc-1 should have data written during dc-0 pause");
            assertEquals(value.toString(), "stream_" + i);
          }
        });

        waitForRegionResume(ctx, 0);

        String dc0RouterUrl = ctx.dc0Cluster.getRandomRouterURL();
        ClientConfig dc0Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl);
        try (AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(dc0Config)) {
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
            for (int i = 0; i < 5; i++) {
              Object value = dc0Client.get("dc0-initial-" + i).get();
              assertNotNull(value, "dc-0 should still have initial data after recovery");
            }
            for (int i = 0; i < 10; i++) {
              Object value = dc0Client.get("dc1-during-pause-" + i).get();
              assertNotNull(value, "dc-0 should catch up on dc-1 data after recovery");
              assertEquals(value.toString(), "stream_" + i);
            }
          });
        }
      }
    } finally {
      dc0Producer.stop();
      dc1Producer.stop();
    }
  }

  /**
   * Test 3: Both regions fail and recover independently.
   * Pause all replicas in both regions, verify recovery via independent health monitors.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testBothRegionsPauseAndRecoverIndependently() throws Exception {
    String storeName = Utils.getUniqueString("both-regions-test");
    RegionTestContext ctx = setupHybridAAStore(storeName);

    VeniceSystemProducer dc0Producer = getSamzaProducerForStream(multiRegionWrapper, 0, storeName);
    try {
      for (int i = 0; i < 5; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "initial-", i);
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        assertTrue(ctx.dc0LeaderSit.isRunning());
        assertTrue(ctx.dc0FollowerSit.isRunning());
        assertTrue(ctx.dc1LeaderSit.isRunning());
        assertTrue(ctx.dc1FollowerSit.isRunning());
      });

      // Pause all 4 replicas (2 per region).
      // Each call verifies the pause took effect before returning.
      pauseEntireRegion(ctx, 0);
      pauseEntireRegion(ctx, 1);

      // Wait for all to resume (health monitors on each server recover independently)
      waitForRegionResume(ctx, 0);
      waitForRegionResume(ctx, 1);

      for (int i = 0; i < 5; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "post-recovery-", i);
      }

      String dc0RouterUrl = ctx.dc0Cluster.getRandomRouterURL();
      String dc1RouterUrl = ctx.dc1Cluster.getRandomRouterURL();
      ClientConfig dc0Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl);
      ClientConfig dc1Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc1RouterUrl);

      try (AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(dc0Config);
          AvroGenericStoreClient<String, Object> dc1Client = ClientFactory.getAndStartGenericAvroClient(dc1Config)) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 5; i++) {
            Object dc0Value = dc0Client.get("post-recovery-" + i).get();
            assertNotNull(dc0Value, "dc-0 should have post-recovery data");
            assertEquals(dc0Value.toString(), "stream_" + i);

            Object dc1Value = dc1Client.get("post-recovery-" + i).get();
            assertNotNull(dc1Value, "dc-1 should have post-recovery data");
            assertEquals(dc1Value.toString(), "stream_" + i);
          }
        });
      }
    } finally {
      dc0Producer.stop();
    }
  }

  /**
   * Test 4: Leader-only pause in one region.
   * Pause only dc-0's leader while dc-0's follower, and all of dc-1, continue.
   * Data should still be readable from dc-0 (served by follower) and dc-1.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testLeaderOnlyPauseInOneRegion() throws Exception {
    String storeName = Utils.getUniqueString("leader-only-pause-test");
    RegionTestContext ctx = setupHybridAAStore(storeName);

    VeniceSystemProducer dc0Producer = getSamzaProducerForStream(multiRegionWrapper, 0, storeName);
    try {
      for (int i = 0; i < 10; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "data-", i);
      }

      // Wait for data in both regions
      String dc0RouterUrl = ctx.dc0Cluster.getRandomRouterURL();
      ClientConfig dc0Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl);
      try (AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(dc0Config)) {

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc0Client.get("data-" + i).get();
            assertNotNull(value, "Record should be readable from dc-0");
          }
        });

        // Pause ONLY dc-0's leader
        simulatePubSubFailureAndWaitForPause(ctx.dc0LeaderSit, "dc-0 leader");

        // dc-0's follower should NOT be paused
        assertTrue(
            ctx.dc0FollowerSit.getPubSubHealthPausedPartitions().isEmpty(),
            "dc-0 follower should not be paused");
        assertTrue(ctx.dc0FollowerSit.isRunning(), "dc-0 follower should be running");

        // dc-1 should be completely unaffected
        assertRegionNotPaused(ctx, 1);

        // dc-0 should still serve data (via follower)
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc0Client.get("data-" + i).get();
            assertNotNull(value, "dc-0 should serve data via follower while leader is paused");
          }
        });

        // Resume dc-0 leader
        waitForResume(ctx.dc0LeaderSit, "dc-0 leader");
      }
    } finally {
      dc0Producer.stop();
    }
  }

  /**
   * Test 5: Follower-only pause in one region.
   * Pause only dc-0's follower while dc-0's leader, and all of dc-1, continue.
   * Write new records, verify they are ingested by dc-0's leader and replicated to dc-1.
   * Resume follower and verify it catches up.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testFollowerOnlyPauseInOneRegion() throws Exception {
    String storeName = Utils.getUniqueString("follower-only-pause-test");
    RegionTestContext ctx = setupHybridAAStore(storeName);

    VeniceSystemProducer dc0Producer = getSamzaProducerForStream(multiRegionWrapper, 0, storeName);
    try {
      for (int i = 0; i < 5; i++) {
        sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "initial-", i);
      }

      String dc0RouterUrl = ctx.dc0Cluster.getRandomRouterURL();
      String dc1RouterUrl = ctx.dc1Cluster.getRandomRouterURL();
      ClientConfig dc0Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl);
      ClientConfig dc1Config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc1RouterUrl);

      try (AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(dc0Config);
          AvroGenericStoreClient<String, Object> dc1Client = ClientFactory.getAndStartGenericAvroClient(dc1Config)) {

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 5; i++) {
            assertNotNull(dc0Client.get("initial-" + i).get(), "Initial data should be in dc-0");
            assertNotNull(dc1Client.get("initial-" + i).get(), "Initial data should be in dc-1");
          }
        });

        // Pause ONLY dc-0's follower
        simulatePubSubFailureAndWaitForPause(ctx.dc0FollowerSit, "dc-0 follower");

        // dc-0's leader should NOT be paused
        assertTrue(ctx.dc0LeaderSit.getPubSubHealthPausedPartitions().isEmpty(), "dc-0 leader should not be paused");
        assertTrue(ctx.dc0LeaderSit.isRunning(), "dc-0 leader should be running");

        // dc-1 should be completely unaffected
        assertRegionNotPaused(ctx, 1);

        // Write records while dc-0 follower is paused — leader ingests, replicates to dc-1
        for (int i = 0; i < 10; i++) {
          sendStreamingRecordWithKeyPrefix(dc0Producer, storeName, "during-follower-pause-", i);
        }

        // Both dc-0 (via leader) and dc-1 should have the new records
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object dc0Value = dc0Client.get("during-follower-pause-" + i).get();
            assertNotNull(dc0Value, "dc-0 should serve new records via leader");
            Object dc1Value = dc1Client.get("during-follower-pause-" + i).get();
            assertNotNull(dc1Value, "dc-1 should have replicated records");
          }
        });

        // Resume dc-0 follower
        waitForResume(ctx.dc0FollowerSit, "dc-0 follower");

        // Verify all replicas are healthy
        assertRegionNotPaused(ctx, 0);
        assertRegionNotPaused(ctx, 1);
      }
    } finally {
      dc0Producer.stop();
    }
  }

  private static class RegionTestContext {
    final StoreIngestionTask dc0LeaderSit;
    final StoreIngestionTask dc0FollowerSit;
    final StoreIngestionTask dc1LeaderSit;
    final StoreIngestionTask dc1FollowerSit;
    final VeniceClusterWrapper dc0Cluster;
    final VeniceClusterWrapper dc1Cluster;
    final String storeName;
    final String versionTopic;

    RegionTestContext(
        StoreIngestionTask dc0LeaderSit,
        StoreIngestionTask dc0FollowerSit,
        StoreIngestionTask dc1LeaderSit,
        StoreIngestionTask dc1FollowerSit,
        VeniceClusterWrapper dc0Cluster,
        VeniceClusterWrapper dc1Cluster,
        String storeName,
        String versionTopic) {
      this.dc0LeaderSit = dc0LeaderSit;
      this.dc0FollowerSit = dc0FollowerSit;
      this.dc1LeaderSit = dc1LeaderSit;
      this.dc1FollowerSit = dc1FollowerSit;
      this.dc0Cluster = dc0Cluster;
      this.dc1Cluster = dc1Cluster;
      this.storeName = storeName;
      this.versionTopic = versionTopic;
    }
  }
}

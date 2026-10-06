package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.stats.DeadStoreStats;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.AggPushStatusCleanUpStats;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.LeakedPushStatusCleanUpService;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Venice Helix Admin tests that run in isolated cluster. This suite is pretty time-consuming.
 * Please consider adding cases to {@link TestVeniceHelixAdminWithSharedEnvironment}.
 */
public class TestVeniceHelixAdminWithIsolatedEnvironment extends AbstractTestVeniceHelixAdmin {
  private static final Logger LOGGER = LogManager.getLogger(TestVeniceHelixAdminWithIsolatedEnvironment.class);

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(new MetricsRepository());
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    super.cleanUp();
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testControllerFailOver() throws Exception {
    String storeName = Utils.getUniqueString("test");
    veniceAdmin.createStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    int newAdminPort = controllerConfig.getAdminPort() + 1; /* Note: this is a dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);
    VeniceProperties newControllerProps = builder.build();
    VeniceControllerClusterConfig newConfig = new VeniceControllerClusterConfig(newControllerProps);
    VeniceHelixAdmin newAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory(),
        pubSubBrokerWrapper.getPubSubPositionTypeRegistry(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    // Start stand by controller
    newAdmin.initStorageCluster(clusterName);
    List<VeniceHelixAdmin> allAdmins = new ArrayList<>();
    allAdmins.add(veniceAdmin);
    allAdmins.add(newAdmin);
    waitForALeader(allAdmins, clusterName, LEADER_CHANGE_TIMEOUT_MS);

    // Can not add store through a standby controller
    Assert.assertThrows(VeniceNoClusterException.class, () -> {
      VeniceHelixAdmin follower = getFollower(allAdmins, clusterName);
      follower.createStore(clusterName, "failedStore", "dev", KEY_SCHEMA, VALUE_SCHEMA);
    });

    // Stop current leader.
    final VeniceHelixAdmin curLeader = getLeader(allAdmins, clusterName);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> !resourceMissingTopState(
            curLeader.getHelixVeniceClusterResources(clusterName).getHelixManager(),
            clusterName,
            version.kafkaTopicName()));
    curLeader.stop(clusterName);
    Thread.sleep(1000);
    VeniceHelixAdmin oldLeader = curLeader;
    // wait leader change event
    waitForALeader(allAdmins, clusterName, LEADER_CHANGE_TIMEOUT_MS);
    // Now get status from new leader controller.
    VeniceHelixAdmin newLeader = getLeader(allAdmins, clusterName);
    Assert.assertFalse(
        resourceMissingTopState(
            newLeader.getHelixVeniceClusterResources(clusterName).getHelixManager(),
            clusterName,
            version.kafkaTopicName()));
    // Stop and start participant to use new leader to trigger state transition.
    stopAllParticipants();
    HelixExternalViewRepository routing =
        newLeader.getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    // The routing repository's cached leader controller is updated asynchronously via Helix
    // callbacks, so it may lag behind the actual leader election result. Retry until it converges.
    int expectedPort = Utils.parsePortFromHelixNodeIdentifier(newLeader.getControllerName());
    TestUtils.waitForNonDeterministicAssertion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        true,
        true,
        () -> Assert
            .assertEquals(routing.getLeaderController().getPort(), expectedPort, "leader controller is changed."));
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> routing.getWorkingInstances(version.kafkaTopicName(), 0).isEmpty());
    startParticipant(true, NODE_ID);
    Thread.sleep(1000l);
    // New leader controller create resource and trigger state transition on participant.
    newLeader.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Version newVersion = new VersionImpl(storeName, 2);
    Assert.assertEquals(
        newLeader.getOffLinePushStatus(clusterName, newVersion.kafkaTopicName()).getExecutionStatus(),
        ExecutionStatus.STARTED,
        "Can not trigger state transition from new leader");
    // Start original controller again, now it should become leader again based on Helix's logic.
    oldLeader.initStorageCluster(clusterName);
    newLeader.stop(clusterName);
    Thread.sleep(1000l);
    waitForALeader(allAdmins, clusterName, LEADER_CHANGE_TIMEOUT_MS);
    // find the leader controller and test it could continue to add store as normal.
    getLeader(allAdmins, clusterName).createStore(clusterName, "failedStore", "dev", KEY_SCHEMA, VALUE_SCHEMA);
  }

  @Test
  public void testGetLeaderController() {
    Assert.assertEquals(
        veniceAdmin.getLeaderController(clusterName).getNodeId(),
        Utils.getHelixNodeIdentifier(controllerConfig.getAdminHostname(), controllerConfig.getAdminPort()));
    // Create a new controller and test getLeaderControllerDetails again.
    int newAdminPort = controllerConfig.getAdminPort() - 10;
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);
    VeniceProperties newControllerProps = builder.build();
    VeniceControllerClusterConfig newConfig = new VeniceControllerClusterConfig(newControllerProps);
    VeniceHelixAdmin newLeaderAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory(),
        pubSubBrokerWrapper.getPubSubPositionTypeRegistry(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    newLeaderAdmin.initStorageCluster(clusterName);
    List<VeniceHelixAdmin> admins = new ArrayList<>();
    admins.add(veniceAdmin);
    admins.add(newLeaderAdmin);
    waitForALeader(admins, clusterName, LEADER_CHANGE_TIMEOUT_MS);
    if (veniceAdmin.isLeaderControllerFor(clusterName)) {
      Assert.assertEquals(
          veniceAdmin.getLeaderController(clusterName).getNodeId(),
          Utils.getHelixNodeIdentifier(controllerConfig.getAdminHostname(), controllerConfig.getAdminPort()));
    } else {
      Assert.assertEquals(
          veniceAdmin.getLeaderController(clusterName).getNodeId(),
          Utils.getHelixNodeIdentifier(controllerConfig.getAdminHostname(), newAdminPort));
    }
    newLeaderAdmin.stop(clusterName);
    admins.remove(newLeaderAdmin);
    waitForALeader(admins, clusterName, LEADER_CHANGE_TIMEOUT_MS);
    Assert.assertEquals(
        veniceAdmin.getLeaderController(clusterName).getNodeId(),
        Utils.getHelixNodeIdentifier(controllerConfig.getAdminHostname(), controllerConfig.getAdminPort()),
        "Controller should be back to original one.");
    veniceAdmin.stop(clusterName);
    TestUtils.waitForNonDeterministicCompletion(
        LEADER_CHANGE_TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        () -> !veniceAdmin.isLeaderControllerFor(clusterName));

    // The cluster should be leaderless now
    Assert.assertFalse(veniceAdmin.isLeaderControllerFor(clusterName));
    Assert.assertFalse(newLeaderAdmin.isLeaderControllerFor(clusterName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testGetFutureVersionsNotBlocked() throws InterruptedException {
    ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    try {
      String storeName = Utils.getUniqueString("test_store");
      veniceAdmin.createStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
      asyncExecutor.submit(() -> {
        // A time-consuming store operation that holds cluster-level read lock and store-level write lock.
        HelixVeniceClusterResources resources = veniceAdmin.getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
          try {
            Thread.sleep(TOTAL_TIMEOUT_FOR_SHORT_TEST_MS);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
      // Give some time for above thread to take cluster level read lock.
      Thread.sleep(1000);
      // Should not be blocked even though another thread is holding cluster-level read lock.
      veniceAdmin.getFutureVersion(clusterName, storeName);
    } finally {
      TestUtils.shutdownExecutor(asyncExecutor);
    }
  }

  @Test
  public void testExternalViewDataChangeDeadLock() throws InterruptedException {
    ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    try {
      String storeName = Utils.getUniqueString("testExternalViewDataChangeDeadLock");
      veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
      asyncExecutor.submit(() -> {
        // Add version. Hold store write lock and release it before polling EV status.
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      });
      Thread.sleep(500);

      // Simulate node_removable request. Hold resourceAssignment synchronized block
      HelixVeniceClusterResources resources = veniceAdmin.getHelixVeniceClusterResources(clusterName);
      RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      synchronized (resourceAssignment) {
        try {
          resources.getPushMonitor().getOfflinePushOrThrow(storeName + "_v1");
        } catch (VeniceException e) {
          // Ignore VeniceException
        }
      }
      // If there is a deadlock and then version cannot become online
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), 1));
    } finally {
      // Kill the running thread so remove the deadlock so that the controller can shut down properly for clean up.
      TestUtils.shutdownExecutor(asyncExecutor);
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testAbortMigrationStoreDeletion() {
    String storeName = Utils.getUniqueString("test_abort_migration_cleanup_store");
    try {
      veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
      veniceAdmin.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams().setStoreMigration(false).setEnableReads(false).setEnableWrites(false));

      PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(storeName, 1));
      veniceAdmin.getTopicManager().createTopic(rtTopic, 1, 1, true);

      Assert.assertTrue(veniceAdmin.getTopicManager().containsTopic(rtTopic));
      boolean abort = true;
      veniceAdmin.deleteStore(clusterName, storeName, abort, Store.IGNORE_VERSION, false);
      Assert.assertTrue(veniceAdmin.getTopicManager().containsTopic(rtTopic));
      Assert.assertNotNull(veniceAdmin.getStore(clusterName, storeName));

      String newStoreName = Utils.getUniqueString("test_cleanup_store");
      veniceAdmin.createStore(clusterName, newStoreName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
      veniceAdmin.updateStore(
          clusterName,
          newStoreName,
          new UpdateStoreQueryParams().setStoreMigration(false).setEnableReads(false).setEnableWrites(false));
      PubSubTopic newRtTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(newStoreName, 1));
      veniceAdmin.getTopicManager().createTopic(newRtTopic, 1, 1, true);
      abort = false;
      Assert.assertTrue(veniceAdmin.getTopicManager().containsTopic(newRtTopic));
      veniceAdmin.deleteStore(clusterName, newStoreName, abort, Store.IGNORE_VERSION, false);
      Assert.assertNull(veniceAdmin.getStore(clusterName, newStoreName));
    } finally {
      veniceAdmin.deleteStore(clusterName, storeName, false, Store.IGNORE_VERSION, false);
    }
  }

  @Test
  public void testIdempotentStoreDeletion() {
    String storeName = Utils.getUniqueString("test_delete_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    // Mimic a partial deletion where only the StoreConfig is deleted.
    ZkStoreConfigAccessor storeConfigAccessor =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
    storeConfigAccessor.deleteConfig(storeName);
    Assert.assertNull(storeConfigAccessor.getStoreConfig(storeName), "StoreConfig should have been deleted");
    Assert.assertNotNull(veniceAdmin.getStore(clusterName, storeName));
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName));

    // Mimic another case where Store object is deleted but StoreConfig still exists.
    String newStoreName = Utils.getUniqueString("test_delete_store2");
    veniceAdmin.createStore(clusterName, newStoreName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(
        clusterName,
        newStoreName,
        new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().deleteStore(newStoreName);
    Assert.assertNull(veniceAdmin.getStore(clusterName, newStoreName));
    Assert.assertNotNull(storeConfigAccessor.getStoreConfig(newStoreName));
    veniceAdmin.deleteStore(clusterName, newStoreName, Store.IGNORE_VERSION, true);
    Assert.assertNull(storeConfigAccessor.getStoreConfig(newStoreName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testOrphanPushCleanupPreservesSystemResourceWithLiveOwner() throws Exception {
    String orphanStore = Utils.getUniqueString("orphan-store");
    String liveStore = Utils.getUniqueString("live-store");
    String systemStore = VeniceSystemStoreType.META_STORE.getSystemStoreName(liveStore);
    String orphanTopic = Version.composeKafkaTopic(orphanStore, 1);
    String liveTopic = Version.composeKafkaTopic(systemStore, 1);
    HelixVeniceClusterResources resources = veniceAdmin.getHelixVeniceClusterResources(clusterName);
    HelixAdmin helixAdmin = veniceAdmin.getHelixAdminClient().getHelixAdmin();
    ZkClient zkClient = ZkClientFactory.newZkClient(zkAddress);
    MetricsRepository cleanupMetrics = new MetricsRepository();
    try {
      veniceAdmin.createStore(clusterName, liveStore, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
      Assert.assertTrue(veniceAdmin.isLeaderControllerFor(clusterName));
      Assert.assertNull(resources.getStoreMetadataRepository().getStore(orphanStore));
      Assert.assertNotNull(resources.getStoreMetadataRepository().getStore(liveStore));
      Assert.assertNull(resources.getStoreMetadataRepository().getStore(systemStore));
      Assert.assertNull(
          veniceAdmin.getReadOnlyZKSharedSystemStoreRepository()
              .getStore(VeniceSystemStoreType.META_STORE.getZkSharedStoreName()));

      VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
          clusterName,
          zkClient,
          new HelixAdapterSerializer(),
          controllerConfig.getLogContext(),
          1);
      for (String topic: Arrays.asList(orphanTopic, liveTopic)) {
        accessor.createOfflinePushStatusAndItsPartitionStatuses(
            new OfflinePushStatus(topic, 1, 1, OfflinePushStrategy.WAIT_ALL_REPLICAS));
        veniceAdmin.getHelixAdminClient().createVeniceStorageClusterResources(clusterName, topic, 1, 1);
        Assert.assertNotNull(helixAdmin.getResourceIdealState(clusterName, topic));
        Assert.assertEquals(
            zkClient.getChildren(accessor.getOfflinePushStatuesParentPath() + "/" + topic),
            Collections.singletonList("0"));
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, topic);
          Assert.assertNotNull(externalView);
          Assert.assertEquals(externalView.getStateMap(topic + "_0"), Collections.singletonMap(NODE_ID, "LEADER"));
        });
      }

      AggPushStatusCleanUpStats stats =
          new AggPushStatusCleanUpStats(clusterName, cleanupMetrics, resources.getStoreMetadataRepository(), false);
      LeakedPushStatusCleanUpService cleanup = new LeakedPushStatusCleanUpService(
          clusterName,
          accessor,
          resources.getStoreMetadataRepository(),
          veniceAdmin,
          resources.getClusterLockManager(),
          stats,
          100,
          0);
      cleanup.start();
      try {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          Assert.assertNull(helixAdmin.getResourceIdealState(clusterName, orphanTopic));
          Assert.assertNull(helixAdmin.getResourceExternalView(clusterName, orphanTopic));
          Assert.assertFalse(zkClient.exists(accessor.getOfflinePushStatuesParentPath() + "/" + orphanTopic));
          Assert.assertFalse(zkClient.exists(accessor.getOfflinePushStatuesParentPath() + "/" + orphanTopic + "/0"));
        });
        Assert.assertTrue(veniceAdmin.isLeaderControllerFor(clusterName));
        Assert.assertNotNull(resources.getStoreMetadataRepository().getStore(liveStore));
        Assert.assertNotNull(helixAdmin.getResourceIdealState(clusterName, liveTopic));
        Assert.assertEquals(
            helixAdmin.getResourceExternalView(clusterName, liveTopic).getStateMap(liveTopic + "_0"),
            Collections.singletonMap(NODE_ID, "LEADER"));
        Assert.assertEquals(
            zkClient.getChildren(accessor.getOfflinePushStatuesParentPath() + "/" + liveTopic),
            Collections.singletonList("0"));
      } finally {
        cleanup.stop();
        TestUtils
            .waitForNonDeterministicAssertion(
                10,
                TimeUnit.SECONDS,
                () -> Assert.assertEquals(
                    cleanupMetrics
                        .getMetric(
                            stats.getTotalStats().getName() + "--leaked_push_status_clean_up_service_state.Gauge")
                        .value(),
                    (double) PushStatusCleanUpServiceState.STOPPED.getValue()));
      }
    } finally {
      zkClient.close();
      cleanupMetrics.close();
    }
  }

  @Test
  public void testStoreLifecycle_ValidateDeleted() {
    String storeName = Utils.getUniqueString("test_validate_deleted_store");
    int largestUsedVersion = Store.IGNORE_VERSION;

    // Create store
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    // Disable reads and writes before deletion (required by Venice)
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));

    // Delete store
    veniceAdmin.deleteStore(clusterName, storeName, largestUsedVersion, true);

    // Validate deletion
    StoreDeletedValidation result = veniceAdmin.validateStoreDeleted(clusterName, storeName);

    Assert.assertTrue(result.isDeleted(), "Store should be completely deleted. Failure reason: " + result.getError());
  }

  @Test
  public void testStoreLifecycleValidateDeleted_WithLeftoverTopics() {
    String storeName = Utils.getUniqueString("test_validate_deleted_store_with_leftover_topics");

    // Create store
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    // Create a version topic manually to simulate a version that existed
    String versionTopicName = Version.composeKafkaTopic(storeName, 1);
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(versionTopicName);
    TopicManager topicManager = veniceAdmin.getTopicManager();

    // Create the topic to simulate that a version existed
    topicManager.createTopic(versionTopic, 1, 1, true);

    // Verify topic exists
    Assert.assertTrue(topicManager.containsTopic(versionTopic), "Version topic should exist");

    // Disable reads and writes before deletion (required by Venice)
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));

    // Delete store metadata only (without trying to delete versions through normal flow)
    // This simulates the scenario where store metadata is deleted but topics remain due to disabled cleanup
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().deleteStore(storeName);

    // Also delete the store config to simulate complete metadata cleanup
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor().deleteConfig(storeName);

    // Verify store is gone from metadata but topic still exists (simulating leftover scenario)
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted from metadata");
    Assert.assertTrue(topicManager.containsTopic(versionTopic), "Version topic should still exist");

    // Now validate store deletion - this should detect the leftover topics
    StoreDeletedValidation result = veniceAdmin.validateStoreDeleted(clusterName, storeName);

    Assert.assertFalse(result.isDeleted(), "Store validation should fail due to leftover topics");
    Assert.assertTrue(
        result.getError().contains("PubSub topic still exists"),
        "Error message should mention leftover PubSub topics. Actual error: " + result.getError());
    Assert.assertTrue(
        result.getError().contains(versionTopicName),
        "Error message should mention the specific leftover topic. Actual error: " + result.getError());

    // Clean up the leftover topic for the validation test
    topicManager.ensureTopicIsDeletedAndBlockWithRetry(versionTopic);

    // Verify that after cleanup, validation passes
    StoreDeletedValidation resultAfterCleanup = veniceAdmin.validateStoreDeleted(clusterName, storeName);
    Assert.assertTrue(
        resultAfterCleanup.isDeleted(),
        "Store validation should pass after topic cleanup. Error: " + resultAfterCleanup.getError());
  }

  public static boolean resourceMissingTopState(SafeHelixManager helixManager, String clusterName, String resourceID) {
    ExternalView externalView = helixManager.getClusterManagmentTool().getResourceExternalView(clusterName, resourceID);
    for (String partition: externalView.getPartitionSet()) {
      for (String state: externalView.getStateMap(partition).values()) {
        if (state.equals("ERROR") || state.equals("OFFLINE")) {
          return true;
        }
      }
    }
    return false;
  }

  @Test
  public void testDeadStoreStatsInitialization() {
    int newAdminPort = controllerConfig.getAdminPort() + 100;
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties())
        .put("admin.port", newAdminPort)
        .put("cluster.name", clusterName)
        .put("controller.dead.store.endpoint.enabled", true)
        .put("controller.dead.store.stats.class.name", MockDeadStoreStats.class.getName());
    VeniceProperties newControllerProps = builder.build();
    VeniceControllerClusterConfig newConfig = new VeniceControllerClusterConfig(newControllerProps);

    VeniceHelixAdmin admin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory(),
        pubSubBrokerWrapper.getPubSubPositionTypeRegistry(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    Assert.assertTrue(admin.getDeadStoreStats(clusterName) instanceof MockDeadStoreStats);
  }

  public static class MockDeadStoreStats implements DeadStoreStats {
    public MockDeadStoreStats(VeniceProperties props) {
    }

    @Override
    public List<StoreInfo> getDeadStores(List<StoreInfo> storeInfos, Map<String, String> params) {
      return null;
    }

    @Override
    public void preFetchStats(List<StoreInfo> storeInfos) {
    }
  }
}

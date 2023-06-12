package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Venice Helix Admin tests that run in isolated cluster. This suite is pretty time-consuming.
 * Please consider adding cases to {@link TestVeniceHelixAdminWithSharedEnvironment}.
 */
public class TestVeniceHelixAdminWithIsolatedEnvironment extends AbstractTestVeniceHelixAdmin {
  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(false);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    cleanupCluster();
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
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory());
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
    Assert.assertEquals(
        routing.getLeaderController().getPort(),
        Utils.parsePortFromHelixNodeIdentifier(newLeader.getControllerName()),
        "leader controller is changed.");
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

  @Test(timeOut = LEADER_CHANGE_TIMEOUT_MS)
  public void testIsInstanceRemovable() throws Exception {
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(false, newNodeId);
    int partitionCount = 2;
    int replicationFactor = 2;
    String storeName = "testMovable";

    veniceAdmin.createStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(replicationFactor));
    Version version = veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        replicationFactor);

    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getWorkingInstances().size() != replicationFactor) {
          return false;
        }
      }
      return true;
    });

    // Without waiting for offline push status to be COMPLETED, isCurrentVersion check will fail, then node removable
    // check will not work as expected.
    TestUtils.waitForNonDeterministicCompletion(
        10,
        TimeUnit.SECONDS,
        () -> !resourceMissingTopState(
            veniceAdmin.getHelixVeniceClusterResources(clusterName).getHelixManager(),
            clusterName,
            version.kafkaTopicName()));
    // Make version ONLINE
    ReadWriteStoreRepository storeRepository =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    storeRepository.updateStore(store);
    // Enough number of replicas, any of instance is able to moved out.
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable());
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, newNodeId, Collections.emptyList(), false).isRemovable());

    // Shutdown one instance
    stopParticipant(NODE_ID);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getWorkingInstances().size() == 1;
    });

    NodeRemovableResult result =
        veniceAdmin.isInstanceRemovable(clusterName, newNodeId, Collections.emptyList(), false);
    Assert.assertFalse(result.isRemovable(), "Only one instance is alive, can not be moved out.");
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA.toString());
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable(),
        "Instance is shutdown.");
  }

  @Test
  public void testIsInstanceRemovableOnOldVersion() throws Exception {
    int partitionCount = 2;
    int replicaCount = 1;
    String storeName = "testIsInstanceRemovableOnOldVersion";

    veniceAdmin.createStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(1));
    Version version = veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        replicaCount);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(
          resourceMissingTopState(
              veniceAdmin.getHelixVeniceClusterResources(clusterName).getHelixManager(),
              clusterName,
              version.kafkaTopicName()));
    });

    Assert.assertFalse(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable());
    // Add a new node and increase the replica count to 2.
    String newNodeId = "localhost_9900";
    startParticipant(false, newNodeId);
    int newVersionReplicaCount = 2;
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(newVersionReplicaCount));
    veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        newVersionReplicaCount);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(
          resourceMissingTopState(
              veniceAdmin.getHelixVeniceClusterResources(clusterName).getHelixManager(),
              clusterName,
              version.kafkaTopicName()));
    });
    // The old instance should now be removable because its replica is no longer the current version.
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable());
  }

  @Test
  public void testIsInstanceRemovableForRunningPush() throws Exception {
    stopAllParticipants();
    startParticipant(true, NODE_ID);
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(true, newNodeId);
    int partitionCount = 2;
    int replicas = 2;
    String storeName = "testIsInstanceRemovableForRunningPush";

    veniceAdmin.createStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin
        .incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitionCount, replicas);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getWorkingInstances().size() != replicas) {
          return false;
        }
      }
      return true;
    });

    // Now we have 2 replicas in bootstrap in each partition.
    veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false);
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable());
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, newNodeId, Collections.emptyList(), false).isRemovable());

    // Shutdown one instance
    stopParticipant(newNodeId);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getWorkingInstances().size() == 1;
    });

    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, newNodeId, Collections.emptyList(), false).isRemovable(),
        "Even there is only one live instance, it could be removed and our push would not failed.");
    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, Collections.emptyList(), false).isRemovable(),
        "Instance is shutdown.");
  }

  @Test
  public void testGetLeaderController() {
    Assert.assertEquals(
        veniceAdmin.getLeaderController(clusterName).getNodeId(),
        Utils.getHelixNodeIdentifier(controllerConfig.getAdminHostname(), controllerConfig.getAdminPort()));
    // Create a new controller and test getLeaderController again.
    int newAdminPort = controllerConfig.getAdminPort() - 10;
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);
    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newLeaderAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory());
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
}

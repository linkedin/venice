package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
  public void setup() throws Exception {
    setupCluster();
  }

  @AfterMethod(alwaysRun =  true)
  public void cleanup() {
    cleanupCluster();
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  public void testControllerFailOver() throws Exception {
    String storeName = TestUtils.getUniqueString("test");
    veniceAdmin.addStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    int newAdminPort = config.getAdminPort() + 1; /* Note: this is a dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);
    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newAdmin= new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(newConfig), new MetricsRepository());
    //Start stand by controller
    newAdmin.start(clusterName);
    List<VeniceHelixAdmin> allAdmins = new ArrayList<>();
    allAdmins.add(veniceAdmin);
    allAdmins.add(newAdmin);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);

    //Can not add store through a standby controller
    Assert.assertThrows(VeniceNoClusterException.class, () -> {
      VeniceHelixAdmin slave = getSlave(allAdmins, clusterName);
      slave.addStore(clusterName, "failedStore", "dev", KEY_SCHEMA, VALUE_SCHEMA);
    });

    //Stop current master.
    final VeniceHelixAdmin curMaster = getMaster(allAdmins, clusterName);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> curMaster.getOffLinePushStatus(clusterName, version.kafkaTopicName())
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));
    curMaster.stop(clusterName);
    Thread.sleep(1000);
    VeniceHelixAdmin oldMaster = curMaster;
    //wait master change event
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    //Now get status from new master controller.
    VeniceHelixAdmin newMaster = getMaster(allAdmins, clusterName);
    Assert.assertEquals(newMaster.getOffLinePushStatus(clusterName, version.kafkaTopicName()).getExecutionStatus(), ExecutionStatus.COMPLETED,
        "Offline push should be completed");
    // Stop and start participant to use new master to trigger state transition.
    stopParticipants();
    HelixExternalViewRepository routing = newMaster.getVeniceHelixResource(clusterName).getRoutingDataRepository();
    Assert.assertEquals(routing.getMasterController().getPort(),
        Utils.parsePortFromHelixNodeIdentifier(newMaster.getControllerName()),
        "Master controller is changed.");
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> routing.getReadyToServeInstances(version.kafkaTopicName(), 0).isEmpty());
    startParticipant(true, NODE_ID);
    Thread.sleep(1000l);
    //New master controller create resource and trigger state transition on participant.
    newMaster.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Version newVersion = new Version(storeName, 2);
    Assert.assertEquals(newMaster.getOffLinePushStatus(clusterName, newVersion.kafkaTopicName()).getExecutionStatus(),
        ExecutionStatus.STARTED, "Can not trigger state transition from new master");
    //Start original controller again, now it should become leader again based on Helix's logic.
    oldMaster.start(clusterName);
    newMaster.stop(clusterName);
    Thread.sleep(1000l);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    // find the leader controller and test it could continue to add store as normal.
    getMaster(allAdmins, clusterName).addStore(clusterName, "failedStore", "dev", KEY_SCHEMA, VALUE_SCHEMA);
  }

  @Test
  public void testIsInstanceRemovable() throws Exception {
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(false, newNodeId);
    int partitionCount = 2;
    int replicationFactor = 2;
    String storeName = "testMovable";

    veniceAdmin.addStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(replicationFactor));
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, replicationFactor);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getReadyToServeInstances().size() != replicationFactor) {
          return false;
        }
      }
      return true;
    });
    //Make version ONLINE
    ReadWriteStoreRepository storeRepository = veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    storeRepository.updateStore(store);

    //Enough number of replicas, any of instance is able to moved out.
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId,  false).isRemovable());

    //Shutdown one instance
    stopParticipant(NODE_ID);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getReadyToServeInstances().size() == 1;
    });

    NodeRemovableResult result = veniceAdmin.isInstanceRemovable(clusterName, newNodeId, false);
    Assert.assertFalse(result.isRemovable(), "Only one instance is alive, can not be moved out.");
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA.toString());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable(), "Instance is shutdown.");
  }

  @Test
  public void testIsInstanceRemovableOnOldVersion() throws Exception {
    int partitionCount = 2;
    int replicaCount = 1;
    String storeName = "testIsInstanceRemovableOnOldVersion";

    veniceAdmin.addStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(1));
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, replicaCount);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(veniceAdmin.getOffLinePushStatus(clusterName, version.kafkaTopicName()).getExecutionStatus(),
          ExecutionStatus.COMPLETED);
    });

    Assert.assertFalse(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable());
    // Add a new node and increase the replica count to 2.
    String newNodeId = "localhost_9900";
    startParticipant(false, newNodeId);
    int newVersionReplicaCount = 2;
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(2));
    Version newVersion = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, newVersionReplicaCount);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(veniceAdmin.getOffLinePushStatus(clusterName, newVersion.kafkaTopicName()).getExecutionStatus(),
          ExecutionStatus.COMPLETED);
    });
    // The old instance should now be removable because its replica is no longer the current version.
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable());
  }

  @Test
  public void testIsInstanceRemovableForRunningPush() throws Exception {
    stopParticipants();
    startParticipant(true, NODE_ID);
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(true, newNodeId);
    int partitionCount = 2;
    int replicas = 2;
    String storeName = "testIsInstanceRemovableForRunningPush";

    veniceAdmin.addStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, replicas);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
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

    //Now we have 2 replicas in bootstrap in each partition.
    NodeRemovableResult result = veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false);
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId, false).isRemovable());

    //Shutdown one instance
    stopParticipant(newNodeId);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getWorkingInstances().size() == 1;
    });

    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId, false).isRemovable(),
        "Even there is only one live instance, it could be removed and our push would not failed.");
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, NODE_ID, false).isRemovable(), "Instance is shutdown.");
  }

    @Test
    public void testGetMasterController() {
      Assert.assertEquals(veniceAdmin.getMasterController(clusterName).getNodeId(),
          Utils.getHelixNodeIdentifier(config.getAdminPort()));
      // Create a new controller and test getMasterController again.
      int newAdminPort = config.getAdminPort() - 10;
      PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);
      VeniceProperties newControllerProps = builder.build();
      VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
      VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(newConfig), new MetricsRepository());
      List<VeniceHelixAdmin> admins = new ArrayList<>();
      admins.add(veniceAdmin);
      admins.add(newMasterAdmin);
      waitForAMaster(admins, clusterName, MASTER_CHANGE_TIMEOUT);
      if (veniceAdmin.isMasterController(clusterName)) {
        Assert.assertEquals(veniceAdmin.getMasterController(clusterName).getNodeId(),
            Utils.getHelixNodeIdentifier(config.getAdminPort()));
      } else {
        Assert.assertEquals(veniceAdmin.getMasterController(clusterName).getNodeId(),
            Utils.getHelixNodeIdentifier(newAdminPort));
      }
      newMasterAdmin.stop(clusterName);
      admins.remove(newMasterAdmin);
      waitForAMaster(admins, clusterName, MASTER_CHANGE_TIMEOUT);
      Assert.assertEquals(veniceAdmin.getMasterController(clusterName).getNodeId(),
          Utils.getHelixNodeIdentifier(config.getAdminPort()), "Controller should be back to original one.");
      veniceAdmin.stop(clusterName);
      TestUtils.waitForNonDeterministicCompletion(MASTER_CHANGE_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> !veniceAdmin.isMasterController(clusterName));

      //The cluster should be leaderless now
      Assert.assertFalse(veniceAdmin.isMasterController(clusterName));
      Assert.assertFalse(newMasterAdmin.isMasterController(clusterName));
    }

  @Test
  public void testEnableSSLForPush() throws IOException {
    veniceAdmin.stop(clusterName);
    veniceAdmin.close();
    String storeName1 = "testEnableSSLForPush1";
    String storeName2 = "testEnableSSLForPush2";
    String storeName3 = "testEnableSSLForPush3";
    Properties properties = getControllerProperties(clusterName);
    properties.put(ConfigKeys.SSL_TO_KAFKA, true);
    properties.put(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getSSLAddress());
    properties.put(ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_WHITELIST, true);
    properties.put(ConfigKeys.ENABLE_HYBRID_PUSH_SSL_WHITELIST,  false);
    properties.put(ConfigKeys.PUSH_SSL_WHITELIST, storeName1);

    veniceAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(new VeniceControllerConfig(new VeniceProperties(properties))),
        new MetricsRepository());

    veniceAdmin.start(clusterName);

    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, ()->veniceAdmin.isMasterController(clusterName));
    veniceAdmin.addStore(clusterName, storeName1, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.addStore(clusterName, storeName2, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.addStore(clusterName, storeName3, "test", KEY_SCHEMA, VALUE_SCHEMA);
    //store3 is hybrid store.
    veniceAdmin.updateStore(clusterName, storeName3, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(1000L)
        .setHybridOffsetLagThreshold(1000L));

    Assert.assertTrue(veniceAdmin.isSSLEnabledForPush(clusterName, storeName1),
        "Store1 is in the whitelist, ssl should be enabled.");
    Assert.assertFalse(veniceAdmin.isSSLEnabledForPush(clusterName, storeName2),
        "Store2 is not in the whitelist, ssl should be disabled.");
    Assert.assertTrue(veniceAdmin.isSSLEnabledForPush(clusterName, storeName3),
        "Store3 is hybrid store, and ssl for nearline push is disabled, so by default ssl should be enabled because we turned on the cluster level ssl switcher.");
  }
}

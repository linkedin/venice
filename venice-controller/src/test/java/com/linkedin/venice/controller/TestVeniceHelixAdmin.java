package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * Test cases for VeniceHelixAdmin
 *
 * TODO: separate out tests that can share environment to save time when running tests
 */
public class TestVeniceHelixAdmin {
  private VeniceHelixAdmin veniceAdmin;
  private String clusterName;
  private VeniceControllerConfig config;
  private String keySchema = "\"string\"";
  private String valueSchema = "\"string\"";
  private int maxNumberOfPartition = 10;

  private String zkAddress;
  private String kafkaZkAddress;
  private String nodeId = "localhost_9985";
  private ZkServerWrapper zkServerWrapper;
  private KafkaBrokerWrapper kafkaBrokerWrapper;

  private Map<String, HelixManager> participants = new HashMap<>();

  private VeniceProperties controllerProps;
  private MockTestStateModelFactory stateModelFactory;

  public static final long MASTER_CHANGE_TIMEOUT = 10 * Time.MS_PER_SECOND;
  public static final long TOTAL_TIMEOUT_FOR_LONG_TEST = 30 * Time.MS_PER_SECOND;
  public static final long TOTAL_TIMEOUT_FOR_SHORT_TEST = 10 * Time.MS_PER_SECOND;

  @BeforeMethod
  public void setup()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    kafkaZkAddress = kafkaBrokerWrapper.getZkAddress();
    stateModelFactory = new MockTestStateModelFactory();
    String currentPath = Paths.get("").toAbsolutePath().toString();
    if (currentPath.endsWith("venice-controller")) {
      currentPath += "/..";
    }
    VeniceProperties clusterProps = Utils.parseProperties(currentPath + "/venice-server/config/cluster.properties");
    VeniceProperties baseControllerProps = Utils.parseProperties(currentPath + "/venice-controller/config/controller.properties");
    clusterName = TestUtils.getUniqueString("test-cluster");
    clusterProps.getString(ConfigKeys.CLUSTER_NAME);
    PropertyBuilder builder = new PropertyBuilder().put(clusterProps.toProperties())
        .put(baseControllerProps.toProperties())
        .put(ENABLE_TOPIC_REPLICATOR, false)
        .put(KAFKA_ZK_ADDRESS, kafkaZkAddress)
        .put(ZOOKEEPER_ADDRESS, zkAddress)
        .put(CLUSTER_NAME, clusterName)
        .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
        .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, maxNumberOfPartition)
        .put(DEFAULT_PARTITION_SIZE, 100)
        .put(CLUSTER_TO_D2, TestUtils.getClusterToDefaultD2String(clusterName));

    controllerProps = builder.build();

    config = new VeniceControllerConfig(controllerProps);
    veniceAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(config), new MetricsRepository());
    veniceAdmin.start(clusterName);
    startParticipant();
    waitUntilIsMaster(veniceAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
  }

  @AfterMethod
  public void cleanup() {
    stopParticipants();
    try {
      veniceAdmin.stop(clusterName);
      veniceAdmin.close();
    } catch (Exception e) {
    }
    zkServerWrapper.close();
    kafkaBrokerWrapper.close();
  }

  private void startParticipant()
      throws Exception {
    startParticipant(false, nodeId);
  }

  private void startParticipant(boolean isDelay, String nodeId)
      throws Exception {
    stateModelFactory.setBlockTransition(isDelay);
    HelixManager manager = TestUtils.getParticipant(clusterName, nodeId, zkAddress, 9985, stateModelFactory,
        VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL);
    participants.put(nodeId, manager);
    manager.connect();
    HelixUtils.setupInstanceConfig(clusterName, nodeId, zkAddress);
  }

  private void stopParticipants() {
    for (String nodeId : participants.keySet()) {
      participants.get(nodeId).disconnect();
    }
    participants.clear();
  }

  private void stopParticipant(String nodeId) {
    if (participants.containsKey(nodeId)) {
      participants.get(nodeId).disconnect();
      participants.remove(nodeId);
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testStartClusterAndCreatePush()
      throws Exception {
    try {
      String storeName = TestUtils.getUniqueString("test-store");
      veniceAdmin.addStore(clusterName, storeName, "dev", keySchema, valueSchema);
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertEquals(veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED, "Offline job status should not already exist.");
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      Assert.assertNotEquals(veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED, "Can not get offline job status correctly.");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  //@Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  @Test
  public void testControllerFailOver()
      throws Exception {
    String storeName = TestUtils.getUniqueString("test");
    veniceAdmin.addStore(clusterName, storeName, "dev", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);

    int newAdminPort = config.getAdminPort() + 1; /* Note: this is a dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(newConfig), new MetricsRepository());
    //Start stand by controller
    newMasterAdmin.start(clusterName);
    List<VeniceHelixAdmin> allAdmins = new ArrayList<>();
    allAdmins.add(veniceAdmin);
    allAdmins.add(newMasterAdmin);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    try {
      newMasterAdmin.addStore(clusterName, "failedStore", "dev", keySchema, valueSchema);
      Assert.fail("Can not add store through a standby controller");
    } catch (VeniceNoClusterException e) {
      //expect
    }
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getOffLinePushStatus(clusterName, version.kafkaTopicName())
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    //Stop original master.
    veniceAdmin.stop(clusterName);
    //wait master change event
    waitUntilIsMaster(newMasterAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
    //Now get status from new master controller.
    Assert.assertEquals(newMasterAdmin.getOffLinePushStatus(clusterName, version.kafkaTopicName()).getExecutionStatus(), ExecutionStatus.COMPLETED,
        "Offline push should be completed");

    // Stop and start participant to use new master to trigger state transition.
    stopParticipants();
    HelixRoutingDataRepository routing = newMasterAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
    //Assert routing data repository can find the new master controller.
    Assert.assertEquals(routing.getMasterController().getPort(), newAdminPort,
        "Master controller is changed, now" + newAdminPort + " is used.");
    Thread.sleep(1000l);
    Assert.assertTrue(routing.getReadyToServeInstances(version.kafkaTopicName(), 0).isEmpty(),
        "Participant became offline. No instance should be living in test_v1");
    startParticipant(true, nodeId);
    Thread.sleep(1000l);
    //New master controller create resource and trigger state transition on participant.
    newMasterAdmin.incrementVersion(clusterName, storeName, 1, 1);
    Version newVersion = new Version(storeName, 2);
    Assert.assertEquals(newMasterAdmin.getOffLinePushStatus(clusterName, newVersion.kafkaTopicName()).getExecutionStatus(),
        ExecutionStatus.STARTED, "Can not trigger state transition from new master");
    //Start original controller again, now it should become leader again based on Helix's logic.
    veniceAdmin.start(clusterName);
    newMasterAdmin.stop(clusterName);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    // find the leader controller and test it could continue to add store as normal.
    if (veniceAdmin.isMasterController(clusterName)) {
      veniceAdmin.addStore(clusterName, "failedStore", "dev", keySchema, valueSchema);
    } else {
      Assert.fail("No leader controller is found for cluster" + clusterName);
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  public void testIsMasterController()
      throws IOException, InterruptedException {
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName),
        "The default controller should be the master controller.");

    int newAdminPort = config.getAdminPort() + 1; /* Note: dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(newConfig), new MetricsRepository());
    //Start stand by controller
    newMasterAdmin.start(clusterName);
    Assert.assertFalse(newMasterAdmin.isMasterController(clusterName),
        "The new controller should be stand-by right now.");
    veniceAdmin.stop(clusterName);
    // Waiting state transition from standby->leader on new admin
    Thread.sleep(1000L);
    waitUntilIsMaster(newMasterAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
    Assert.assertTrue(newMasterAdmin.isMasterController(clusterName),
        "The new controller should be the master controller right now.");
    veniceAdmin.start(clusterName);
    waitForAMaster(Arrays.asList(veniceAdmin, newMasterAdmin), clusterName, MASTER_CHANGE_TIMEOUT);

    /* XOR */
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName) || newMasterAdmin.isMasterController(clusterName));
    Assert.assertFalse(veniceAdmin.isMasterController(clusterName) && newMasterAdmin.isMasterController(clusterName));
    newMasterAdmin.close();
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testMultiCluster() {
    String newClusterName = "new_test_cluster";
    PropertyBuilder builder =
        new PropertyBuilder().put(controllerProps.toProperties()).put("cluster.name", newClusterName);

    VeniceProperties newClusterProps = builder.build();
    VeniceControllerConfig newClusterConfig = new VeniceControllerConfig(newClusterProps);

    veniceAdmin.addConfig(newClusterName, newClusterConfig);
    veniceAdmin.start(newClusterName);
    waitUntilIsMaster(veniceAdmin, newClusterName, MASTER_CHANGE_TIMEOUT);

    Assert.assertTrue(veniceAdmin.isMasterController(clusterName));
    Assert.assertTrue(veniceAdmin.isMasterController(newClusterName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartition() {
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    veniceAdmin.addStore(clusterName, "test", "dev", keySchema, valueSchema);

    long storeSize = partitionSize * (minPartitionNumber + 1);
    int numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, storeSize / partitionSize,
        "Number partition is smaller than max and bigger than min. So use the calculated result.");
    storeSize = 1;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, minPartitionNumber,
        "Store size is too small so should use min number of partitions.");
    storeSize = partitionSize * (maxPartitionNumber + 1);
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber,
        "Store size is too big, should use max number of paritions.");

    storeSize = Long.MAX_VALUE;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber, "Partition is overflow from Integer, use max one.");
    storeSize = -1;
    try {
      numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
      Assert.fail("Invalid store.");
    } catch (VeniceException e) {
      //expected.
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartitionsFromPreviousVersion() {
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    veniceAdmin.addStore(clusterName, "test", "dev", keySchema, valueSchema);
    long storeSize = partitionSize * (minPartitionNumber) + 1;
    int numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Version v = veniceAdmin.incrementVersion(clusterName, "test", numberOfParition, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, "test", v.getNumber());
    Store store = veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().getStore("test");
    store.setPartitionCount(numberOfParition);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);

    v = veniceAdmin.incrementVersion(clusterName, "test", maxPartitionNumber, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, "test", v.getNumber());
    storeSize = partitionSize * (maxPartitionNumber - 2);
    numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfParition, minPartitionNumber,
        "Should use the number of partition from previous version");
  }

  void waitUntilIsMaster(VeniceHelixAdmin admin, String cluster, long timeout) {
    List<VeniceHelixAdmin> admins = Collections.singletonList(admin);
    waitForAMaster(admins, cluster, timeout);
  }

  void waitForAMaster(List<VeniceHelixAdmin> admins, String cluster, long timeout) {
    int sleepDuration = 100;
    for (long i = 0; i < timeout; i += sleepDuration) {

      boolean aMaster = false;
      for (VeniceHelixAdmin admin : admins) {
        if (admin.isMasterController(cluster)) {
          aMaster = true;
          break;
        }
      }

      if (aMaster) {
        return;
      } else {
        try {
          Thread.sleep(sleepDuration);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
    Assert.fail("No VeniceHelixAdmin became master for cluster: " + cluster + " after timeout: " + timeout);
  }

  @Test
  public void testHandleVersionCreationFailure() {
    String storeName = "test";
    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(participants.get(nodeId));
    channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
      @Override
      public void handleMessage(KillOfflinePushMessage message) {
        //ignore.
      }
    });

    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
    int versionNumber = version.getNumber();
    veniceAdmin.handleVersionCreationFailure(clusterName, storeName, versionNumber);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
    Assert.assertEquals(veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber)).getExecutionStatus(), ExecutionStatus.ERROR);
  }

  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testDeleteOldVersions()
      throws InterruptedException {
    String storeName = "test";
    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(participants.get(nodeId));
    channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
      @Override
      public void handleMessage(KillOfflinePushMessage message) {
        //ignore.
      }
    });
    Version version = null;
    for (int i = 0; i < 3; i++) {
      version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      int versionNumber = version.getNumber();

      TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
          () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);
    }

    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.versionsForStore(clusterName, storeName).size() == 2);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).get(0).getNumber(), version.getNumber() - 1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).get(1).getNumber(), version.getNumber());

    Version deletedVersion = new Version(storeName, version.getNumber() - 2);
    // Ensure job and topic are deleted
    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getOffLinePushStatus(clusterName, deletedVersion.kafkaTopicName()).getExecutionStatus()
            .equals(ExecutionStatus.NOT_CREATED));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(veniceAdmin.getTopicManager().containsTopic(deletedVersion.kafkaTopicName()));
    });
  }

  @Test
  public void testDeleteResourceThenRestartParticipant()
      throws Exception {
    stopParticipant(nodeId);
    startParticipant(true, nodeId);
    String storeName = "testDeleteResource";
    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
    // Ensure the the replica has became BOOSTRAP
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return routingDataRepository.containsKafkaTopic(version.kafkaTopicName()) &&
          routingDataRepository.getPartitionAssignments(version.kafkaTopicName())
              .getPartition(0)
              .getBootstrapInstances()
              .size() == 1;
    });
    // disconnect the participant
    stopParticipant(nodeId);
    // ensure it has disappeared from external view.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return routingDataRepository.getPartitionAssignments(version.kafkaTopicName()).getAssignedNumberOfPartitions()
          == 0;
    });
    veniceAdmin.deleteHelixResource(clusterName, version.kafkaTopicName());
    // Ensure idealstate is null which means resource has been deleted.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
      IdealState idealState = veniceAdmin.getVeniceHelixResource(clusterName)
          .getController()
          .getHelixDataAccessor()
          .getProperty(keyBuilder.idealStates(version.kafkaTopicName()));
      return idealState == null;
    });
    // Start participant again
    startParticipant(true, nodeId);
    // Ensure resource has been deleted in external view.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return !routingDataRepository.containsKafkaTopic(version.kafkaTopicName());
    });
    Assert.assertEquals(stateModelFactory.getModelList(version.kafkaTopicName(), 0).size(), 1);
    // Replica become OFFLINE state
    Assert.assertEquals(stateModelFactory.getModelList(version.kafkaTopicName(), 0).get(0).getCurrentState(), "OFFLINE");
  }

  @Test
  public void testUpdateStoreMetadata() throws Exception {
    String storeName = TestUtils.getUniqueString("test");
    String owner = TestUtils.getUniqueString("owner");
    int partitionCount = 1;

    //test setting new version

    // The existing participant uses a non-blocking state model which will switch to COMPLETE immediately.  We add
    // an additional participant here that uses a blocking state model so it doesn't switch to complete.  This way
    // the replicas will not all be COMPLETE, and the new version will not immediately be activated.
    startParticipant(true, "localhost_6868");
    veniceAdmin.addStore(clusterName, storeName, owner, keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, partitionCount, 2); // 2 replicas puts a replica on the blocking participant
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), 0);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());

    try {
      veniceAdmin.setStoreCurrentVersion(clusterName, storeName, 100);
      Assert.fail("Version 100 does not exist. Should be failed.");
    } catch (VeniceException e) {
      //expected
    }

    //test setting owner
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), owner);
    String newOwner = TestUtils.getUniqueString("owner");

    veniceAdmin.setStoreOwner(clusterName, storeName, newOwner);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), newOwner);

    //test setting partition count
    int newPartitionCount = 2;
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getPartitionCount(), partitionCount);

    veniceAdmin.setStorePartitionCount(clusterName, storeName, maxNumberOfPartition + 1);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getPartitionCount(), maxNumberOfPartition,
        "Should not exceed the max partition.");
    veniceAdmin.setStorePartitionCount(clusterName, storeName, newPartitionCount);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getPartitionCount(), newPartitionCount);

    // test hybrid config
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).isHybrid());
    HybridStoreConfig hybridConfig = new HybridStoreConfig(TimeUnit.SECONDS.convert(2, TimeUnit.DAYS), 1000);
    veniceAdmin.updateStore(clusterName, storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(hybridConfig.getRewindTimeInSeconds()),
        Optional.of(hybridConfig.getOffsetLagThresholdToGoOnline()), Optional.empty());
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybrid());
  }

  public void testAddVersion() throws Exception {
    stopParticipants();
    startParticipant(true, nodeId); //because we need the new version NOT to transition directly to "online" for the
                                    //idempotent test to work
    String storeName = TestUtils.getUniqueString("test");
    try {
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      Assert.fail("store " + storeName + " does not exist, admin should throw a VeniceException if we try to incrementVersion for it");
    } catch (VeniceException e) {
      //Expected
    }

    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 1);
    try {
      veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
      Assert.fail("Version 1 has already existed");
    } catch (Exception e) {
      //Expected
    }

    veniceAdmin.addVersion(clusterName, storeName, 101, 1, 1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 2);

    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, 101); // set version 101 to be current;

    String pushJobId = TestUtils.getUniqueString("pushJobId");
    Version idempotentOne = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
    Version idempotentTwo = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
    Assert.assertEquals(idempotentOne.getNumber(), idempotentTwo.getNumber(), "Idempotent version increment with same pushId must return same version number");
    Assert.assertEquals(idempotentOne.kafkaTopicName(), idempotentTwo.kafkaTopicName(), "Idempotent version increment with same pushId must return same kafka topic");
  }

  @Test
  public void testGetRealTimeTopic(){
    String storeName = TestUtils.getUniqueString("store");
    try {
      String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, storeName);
      Assert.fail("Must not be able to get a real time topic until the store is created");
    } catch (VeniceNoStoreException e){
      //expected
    }

    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    veniceAdmin.updateStore(clusterName, storeName, Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.of(25L), Optional.of(100L), Optional.empty()); //make store hybrid

    try {
      veniceAdmin.getRealTimeTopic(clusterName, storeName);
      Assert.fail("Must not be able to get a real time topic until the store is initialized with a version");
    } catch (VeniceException e){
      Assert.assertTrue(e.getMessage().contains("is not initialized with a version"), "Got unexpected error message: " + e.getMessage());
    }

    int partitions = 2; //TODO verify partition count for RT topic.
    veniceAdmin.addVersion(clusterName, storeName, 1, partitions, 1);

    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, storeName);
    Assert.assertEquals(rtTopic, storeName + "_rt");

  }

  @Test
  public void testGetBootstrapReplicas()
      throws Exception {
    stopParticipants();
    startParticipant(true, nodeId);
    String storeName = "test";
    veniceAdmin.addStore(clusterName, storeName, "owner", keySchema, valueSchema);
    veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      try {
        PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
            .getRoutingDataRepository()
            .getPartitionAssignments(Version.composeKafkaTopic(storeName, 1));
        return partitionAssignment.getAssignedNumberOfPartitions() == 1;
      }catch (VeniceException e){
        return false;
      }
    });

    List<Replica> replicas = veniceAdmin.getBootstrapReplicas(clusterName, Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(replicas.size(), 1);
    Assert.assertEquals(replicas.get(0).getStatus(), HelixState.BOOTSTRAP_STATE);
    Assert.assertEquals(replicas.get(0).getPartitionId(), 0);

    // Make participant complete BOOTSTRAP->ONLINE
    stateModelFactory.makeTransitionCompleted(Version.composeKafkaTopic(storeName, 1), 0);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(Version.composeKafkaTopic(storeName, 1));
      return partitionAssignment.getPartition(0).getReadyToServeInstances().size() == 1;
    });
    replicas = veniceAdmin.getBootstrapReplicas(clusterName, Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(replicas.size(), 0);
  }

  @Test
  public void testIsInstanceRemovableForRunningPush()
      throws Exception {
    stopParticipants();
    startParticipant(true, nodeId);
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(true, newNodeId);
    int partitionCount = 2;
    int replicas = 2;
    String storeName = "testIsInstanceRemovableForRuningPush";

    veniceAdmin.addStore(clusterName, storeName, "test", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, partitionCount, replicas);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getBootstrapAndReadyToServeInstances().size() != replicas) {
          return false;
        }
      }
      return true;
    });

    //Now we have 2 replicas in bootstrap in each partition.
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId, 1).isRemovable());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId, 1).isRemovable());

    //Shutdown one instance
    stopParticipant(nodeId);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getBootstrapAndReadyToServeInstances().size() == 1;
    });

    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId, 1).isRemovable(),
        "Even there is only one live instance, it could be removed and our push would not failed.");
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId, 1).isRemovable(), "Instance is shutdown.");
  }

  @Test
  public void testIsInstanceRemovable()
      throws Exception {
    // Create another participant so we will get two running instances.
    String newNodeId = "localhost_9900";
    startParticipant(false, newNodeId);
    int partitionCount = 2;
    int replicas = 2;
    String storeName = "testMovable";

    veniceAdmin.addStore(clusterName, storeName, "test", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, partitionCount, replicas);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getReadyToServeInstances().size() != replicas) {
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
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId, 1).isRemovable());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, newNodeId, 1).isRemovable());
    // If min required replica number is 2, we can not remove any of server.
    NodeRemovableResult result = veniceAdmin.isInstanceRemovable(clusterName, nodeId, 2);
    Assert.assertFalse(result.isRemovable());
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.toString());
    result = veniceAdmin.isInstanceRemovable(clusterName, newNodeId, 2);
    Assert.assertFalse(result.isRemovable());
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.toString());

    //Shutdown one instance
    stopParticipant(nodeId);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getReadyToServeInstances().size() == 1;
    });

    result = veniceAdmin.isInstanceRemovable(clusterName, newNodeId, 1);
    Assert.assertFalse(result.isRemovable(), "Only one instance is alive, can not be moved out.");
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA.toString());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId, 1).isRemovable(), "Instance is shutdown.");
  }

  @Test
  public void testGetAndCompareStorageNodeStatusForStorageNode()
      throws Exception {
    String storeName = "testGetStorageNodeStatusForStorageNode";
    int partitionCount = 2;
    int replicaCount = 2;
    //Start a new participant which would hang on bootstrap state.
    String newNodeId = "localhost_9900";
    startParticipant(true, newNodeId);
    veniceAdmin.addStore(clusterName, storeName, "unittestOwner", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, partitionCount, replicaCount);

    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getBootstrapInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });
    //Now all of replica in bootstrap state
    StorageNodeStatus status1 = veniceAdmin.getStorageNodesStatus(clusterName, nodeId);
    StorageNodeStatus status2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(status1.getStatusValueForReplica(HelixUtils.getPartitionName(version.kafkaTopicName(), i)),
          HelixState.BOOTSTRAP.getStateValue(), "Replica in server1 should hang on BOOTSTRAP");
    }
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(status2.getStatusValueForReplica(HelixUtils.getPartitionName(version.kafkaTopicName(), i)),
          HelixState.BOOTSTRAP.getStateValue(), "Replica in server2 should hang on BOOTSTRAP");
    }

    //Set replicas to ONLINE.
    for (int i = 0; i < partitionCount; i++) {
      stateModelFactory.makeTransitionCompleted(version.kafkaTopicName(), i);
    }

    TestUtils.waitForNonDeterministicCompletion(10000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(version.kafkaTopicName());
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getReadyToServeInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });
    StorageNodeStatus newStatus2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);
    Assert.assertTrue(newStatus2.isNewerOrEqual(status2), "ONLINE replicas should be newer than BOOTSTRAP replicas");
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
    try {
      veniceAdmin.getMasterController(clusterName);
      Assert.fail("There is no master controller for cluster:" + clusterName);
    } catch (VeniceException e) {

    }
  }

  @Test
  public void testDisableStoreWrite() {
    String storeName = "testDisableStoreWriter";
    veniceAdmin.addStore(clusterName, storeName, "unittestOwner", keySchema, valueSchema);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);

    try {
      veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
      Assert.fail("Store has been disabled, can not accept a new version");
    } catch (VeniceException e) {
    }
    Assert.assertEquals(veniceAdmin.getAllStores(clusterName).get(0), store);

    try {
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      Assert.fail("Store has been disabled, can not accept a new version");
    } catch (VeniceException e) {
    }
    Assert.assertEquals(veniceAdmin.getAllStores(clusterName).get(0), store);

    try {
      veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
      Assert.fail("Store has been disabled, can not accept a new version");
    } catch (VeniceException e) {
    }
    Assert.assertEquals(veniceAdmin.getAllStores(clusterName).get(0), store);

    veniceAdmin.setStoreWriteability(clusterName, storeName, true);

    veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
    veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);

    store = veniceAdmin.getAllStores(clusterName).get(0);
    // version 1 and version 2 are added to this store.
    Assert.assertTrue(store.isEnableWrites());
    Assert.assertEquals(store.getVersions().size(), 2);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3);
    // two offline jobs are running.
    OfflinePushMonitor monitor = veniceAdmin.getVeniceHelixResource(clusterName).getOfflinePushMonitor();
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> monitor.getOfflinePushStatus(Version.composeKafkaTopic(storeName, 1)).equals(ExecutionStatus.COMPLETED)
            && monitor.getOfflinePushStatus(Version.composeKafkaTopic(storeName, 2)).equals(ExecutionStatus.COMPLETED)
    );
  }

  @Test
  public void testDisableStoreRead() {
    String storeName = "testDisableStoreRead";
    veniceAdmin.addStore(clusterName, storeName, "unittestOwner", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), Store.NON_EXISTING_VERSION,
        "After disabling, store has no version available to serve.");

    veniceAdmin.setStoreReadability(clusterName, storeName, true);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber(),
        "After enabling, version:" + version.getNumber() + " is ready to serve.");
  }

  @Test
  public void testAccessControl() {
    String storeName = "testAccessControl";
    veniceAdmin.addStore(clusterName, storeName, "unittestOwner", keySchema, valueSchema);

    veniceAdmin.setAccessControl(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.isAccessControlled(), false);

    veniceAdmin.setAccessControl(clusterName, storeName, true);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.isAccessControlled(), true);

    veniceAdmin.updateStore(clusterName, storeName, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.of(false));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.isAccessControlled(), false);

    veniceAdmin.updateStore(clusterName, storeName, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.of(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.isAccessControlled(), true);
  }

  @Test
  public void testWhitelist() {
    int testPort = 5555;
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 0, "White list should be empty.");

    veniceAdmin.addInstanceToWhitelist(clusterName, Utils.getHelixNodeIdentifier(testPort));
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 1,
        "After adding a instance into white list, the size of white list should be 1");

    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).iterator().next(), Utils.getHelixNodeIdentifier(testPort),
        "Instance in the white list is not the one added before.");
    veniceAdmin.removeInstanceFromWhiteList(clusterName, Utils.getHelixNodeIdentifier(testPort));
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 0,
        "After removing the instance, white list should be empty.");
  }

  //TODO slow test, ~15 seconds.  Can we improve it?
  @Test
  public void testKillOfflinePush()
      throws Exception {
    String newNodeId = Utils.getHelixNodeIdentifier(9786);
    startParticipant(true, newNodeId);
    String storeName = "testKillPush";
    int partitionCount = 2;
    int replicaFactor = 1;
    // Start a new version with 2 partition and 1 replica
    veniceAdmin.addStore(clusterName, storeName, "test", keySchema, valueSchema);
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, partitionCount, replicaFactor);
    Map<String, Integer> nodesToPartitionMap = new HashMap<>();
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      try {
        PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
            .getRoutingDataRepository()
            .getPartitionAssignments(version.kafkaTopicName());
        if(partitionAssignment.getAllPartitions().size() < partitionCount){
          return false;
        }
        if (partitionAssignment.getPartition(0).getBootstrapInstances().size() == 1
            && partitionAssignment.getPartition(1).getBootstrapInstances().size() == 1) {
          nodesToPartitionMap.put(partitionAssignment.getPartition(0).getBootstrapInstances().get(0).getNodeId(), 0);
          nodesToPartitionMap.put(partitionAssignment.getPartition(1).getBootstrapInstances().get(0).getNodeId(), 1);
          return true;
        }
        return false;
      }catch(VeniceException e){
        return false;
      }
    });
    //Now we have two participants blocked on ST from BOOTSTRAP to ONLINE.

    try {
      veniceAdmin.killOfflinePush(clusterName, version.kafkaTopicName());
      Assert.fail("Storage node have not registered the handler to process kill message, sending should fail");
    } catch (VeniceException e) {
      //expected
    }

    final CopyOnWriteArrayList<KillOfflinePushMessage> processedMessage = new CopyOnWriteArrayList<>();
    for (HelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager);
      channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
        @Override
        public void handleMessage(KillOfflinePushMessage message) {
          processedMessage.add(message);
          //make ST error to simulate kill consumption task.
          stateModelFactory.makeTransitionError(message.getKafkaTopic(), nodesToPartitionMap.get(manager.getInstanceName()));
        }
      });
    }

    veniceAdmin.deleteHelixResource(clusterName, version.kafkaTopicName());
    Thread.sleep(2000);
    //Make sure the resource has not been deleted due to blocking on ST.
    Assert.assertTrue(veniceAdmin.getVeniceHelixResource(clusterName)
        .getRoutingDataRepository()
        .containsKafkaTopic(version.kafkaTopicName()));

    try {
      veniceAdmin.killOfflinePush(clusterName, version.kafkaTopicName());
      TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
          () -> processedMessage.size() == 2);
    } catch (VeniceException e) {
      Assert.fail("Sending message should not fail.", e);
    }

    // Ensure that after killing, resource could continue to be deleted.
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS,
        () -> !veniceAdmin.getVeniceHelixResource(clusterName)
            .getRoutingDataRepository()
            .containsKafkaTopic(version.kafkaTopicName()));
  }

  @Test
  public void testDeleteAllVersionsInStore()
      throws Exception {
    stopParticipants();
    startParticipant(true, nodeId);

    String storeName = TestUtils.getUniqueString("testDeleteAllVersions");
    // register kill message handler for participants.
    for (HelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager);
      channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
        @Override

        public void handleMessage(KillOfflinePushMessage message) {
          //make state transition failed to simulate kill consumption task.
          stateModelFactory.makeTransitionError(message.getKafkaTopic(), 0);
        }
      });
    }
    // Store has not been created.
    try {
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.fail("Store has not been created.");
    } catch (VeniceNoStoreException e) {
    }
    // Prepare 3 version. The first two are completed and the last one is still ongoing.
    int versionCount = 3;
    veniceAdmin.addStore(clusterName, storeName, "testOwner", keySchema, valueSchema);
    Version lastVersion = null;
    for (int i = 0; i < versionCount; i++) {
      lastVersion = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      if (i < versionCount - 1) {
        // Hang the state transition of the last version only. Otherwise, retiring would be triggered.
        stateModelFactory.makeTransitionCompleted(lastVersion.kafkaTopicName(), 0);
      }
    }
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 3);
    // Store has not been disabled.
    try {
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.fail("Store has not been disabled.");
    } catch (VeniceException e) {

    }
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    try {
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.fail("Store has not been disabled to write.");
    } catch (VeniceException e) {

    }
    veniceAdmin.setStoreReadability(clusterName, storeName, true);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    try {
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.fail("Store has not been disabled to read.");
    } catch (VeniceException e) {

    }
    // Store has been disabled.
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0,
        " Versions should be deleted.");
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), Store.NON_EXISTING_VERSION);
    // After enabling store, the serving version is -1 because there is not version available in this store.
    veniceAdmin.setStoreReadability(clusterName, storeName, true);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), Store.NON_EXISTING_VERSION,
        "No version should be available to read");
    Assert.assertTrue(veniceAdmin.getTopicManager().containsTopic(lastVersion.kafkaTopicName()),
        "Kafka topic should be kept for the uncompleted version.");
    Assert.assertFalse(
        veniceAdmin.getTopicManager().containsTopic(Version.composeKafkaTopic(storeName, lastVersion.getNumber() - 1)),
        "Kafka topic should be deleted for the completed version.");
  }

  @Test
  public void testDeleteAllVersionsInStoreWithoutJobAndResource() {
    String storeName = "testDeleteVersionInWithoutJobAndResource";
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    Version version = store.increaseVersion();
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.setCurrentVersion(version.getNumber());
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().addStore(store);
    stopParticipants();
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);

    veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
  }

  @Test
  public void testDeleteStore() {
    String storeName = "testDeleteStore";
    TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    for (HelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager);
      channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
        @Override
        public void handleMessage(KillOfflinePushMessage message) {
          stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0);
        }
      });
    }
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"string\"", "\"string\"");
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1,1);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
    Assert.assertTrue(
        veniceAdmin.getTopicManager().containsTopic(Version.composeKafkaTopic(storeName, version.getNumber())),
        "Kafka topic should be created.");

    // Store has not been disabled.
    try {
      veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);
      Assert.fail("Store has not been disabled.");
    } catch (VeniceException e) {
    }
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
    Assert.assertEquals(
        veniceAdmin.getVeniceHelixResource(clusterName).getStoreGraveyard().getLargestUsedVersionNumber(storeName),
        version.getNumber(), "LargestUsedVersionNumber should be kept in graveyard.");
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_LONG_TEST, TimeUnit.MILLISECONDS,
        () -> !veniceAdmin.getTopicManager().containsTopic(Version.composeKafkaTopic(storeName, version.getNumber())));
  }

  @Test
  public void testDeleteStoreWithLargestUsedVersionNumberOverwritten() {
    String storeName = "testDeleteStore";
    int largestUsedVersionNumber = 1000;
    TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    for (HelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager);
      channel.registerHandler(KillOfflinePushMessage.class, new StatusMessageHandler<KillOfflinePushMessage>() {
        @Override
        public void handleMessage(KillOfflinePushMessage message) {
          stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0);
        }
      });
    }
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"string\"", "\"string\"");
    Version version = veniceAdmin.incrementVersion(clusterName, storeName, 1,1);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
    Assert.assertTrue(
        veniceAdmin.getTopicManager().containsTopic(Version.composeKafkaTopic(storeName, version.getNumber())),
        "Kafka topic should be created.");

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    veniceAdmin.deleteStore(clusterName, storeName, largestUsedVersionNumber);
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
    Assert.assertEquals(
        veniceAdmin.getVeniceHelixResource(clusterName).getStoreGraveyard().getLargestUsedVersionNumber(storeName),
        largestUsedVersionNumber, "LargestUsedVersionNumber should be overwritten and kept in graveyard.");
  }

  @Test
  public void testReCreateStore() {
    String storeName = "testReCreateStore";
    int largestUsedVersionNumber = 100;
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableReads(false);
    store.setEnableWrites(false);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);

    //Re-create store with incompatible schema
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"long\"", "\"long\"");
    veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  @Test
  public void testReCreateStoreWithLegacyStore(){
    String storeName = "testReCreateStore";
    int largestUsedVersionNumber = 100;
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"string\"", "\"string\"");


    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableWrites(false);
    store.setEnableReads(false);
    // Legacy store
    StoreConfig storeConfig = veniceAdmin.getStoreConfigAccessor().getStoreConfig(storeName);
    storeConfig.setDeleting(true);
    veniceAdmin.getStoreConfigAccessor().updateConfig(storeConfig);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);
    //Re-create store with incompatible schema
    veniceAdmin.addStore(clusterName, storeName, "unittest", "\"long\"", "\"long\"");
    veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  // Test the scenairo that some old store exists before we introduce the store config mapping feature.
  // So they do not exist in the mapping, but once we refresh the helix resources, this issue should be repaired.
  @Test
  public void testRepairStoreConfigMapping() {
    HelixReadWriteStoreRepository repo = veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository();
    int storeCount = 3;
    for (int i = 0; i < storeCount; i++) {
      repo.addStore(TestUtils.createTestStore("testRepair" + i, "test", System.currentTimeMillis()));
      Assert.assertFalse(veniceAdmin.getStoreConfigAccessor().containsConfig("testRepair" + i), "Store should not exist in the mapping.");
    }

    veniceAdmin.getVeniceHelixResource(clusterName).clear();
    veniceAdmin.getVeniceHelixResource(clusterName).refresh();
    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(veniceAdmin.getStoreConfigAccessor().getStoreConfig("testRepair"+i).getCluster(), clusterName, "Mapping should be repaired by refresh.");
    }

  }
}
package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controlmessage.ControlMessageChannel;
import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixControlMessageChannel;
import com.linkedin.venice.helix.HelixInstanceConverter;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.helix.TestHelixRoutingDataRepository;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestUtils;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for VeniceHelixAdmin
 *
 * TODO: separate out tests that can share enviornment to save time when running tests
 */
public class TestVeniceHelixAdmin {
  private VeniceHelixAdmin veniceAdmin;
  private String clusterName = "test-cluster";
  private VeniceControllerConfig config;

  private String zkAddress;
  private String kafkaZkAddress;
  private String nodeId = "localhost_9985";
  private ZkServerWrapper zkServerWrapper;
  private KafkaBrokerWrapper kafkaBrokerWrapper;

  private HelixManager manager;

  private VeniceProperties controllerProps;

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
    String currentPath = Paths.get("").toAbsolutePath().toString();
    if (currentPath.endsWith("venice-controller")) {
      currentPath += "/..";
    }
    VeniceProperties clusterProps = Utils.parseProperties(currentPath + "/venice-server/config/cluster.properties");
    VeniceProperties baseControllerProps = Utils.parseProperties(
        currentPath + "/venice-controller/config/controller.properties");

    clusterProps.getString(ConfigKeys.CLUSTER_NAME);
    PropertyBuilder builder = new PropertyBuilder()
            .put(clusterProps.toProperties())
            .put(baseControllerProps.toProperties())
            .put("kafka.zk.address", kafkaZkAddress)
            .put("zookeeper.address", zkAddress)
            .put(VeniceControllerClusterConfig.MAX_NUMBER_OF_PARTITIONS,10)
            .put(VeniceControllerClusterConfig.PARTITION_SIZE, 100);

    controllerProps = builder.build();

    config = new VeniceControllerConfig(controllerProps);
    veniceAdmin = new VeniceHelixAdmin(config);
    veniceAdmin.start(clusterName);
    startParticipant();
    waitUntilIsMaster(veniceAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
  }

  @AfterMethod
  public void cleanup() {
    stopParticipant();
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
    manager = HelixManagerFactory.getZKHelixManager(clusterName, nodeId, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine().registerStateModelFactory("PartitionOnlineOfflineModel",
        new TestHelixRoutingDataRepository.UnitTestStateModelFactory());
    Instance instance = new Instance(nodeId, Utils.getHostName(), 9985);
    manager.setLiveInstanceInfoProvider(new LiveInstanceInfoProvider() {
      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        return HelixInstanceConverter.convertInstanceToZNRecord(instance);
      }
    });
    manager.connect();
  }

  private void stopParticipant() {
    if (manager != null) {
      manager.disconnect();
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testStartClusterAndCreatePush()
      throws Exception {
    try {
      String storeName = TestUtils.getUniqueString("test-store");
      veniceAdmin.addStore(clusterName, storeName, "dev");
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      Assert.assertEquals(veniceAdmin.getOffLineJobStatus(clusterName, new Version(storeName, 1).kafkaTopicName()), ExecutionStatus.STARTED,
          "Can not get offline job status correctly.");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void reserveAndCreateVersion() throws Exception {
    String storeName = TestUtils.getUniqueString("store");
    String owner = "owner";
    try {
      veniceAdmin.addStore(clusterName, storeName, owner);
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);

      int maxVersionBeforeAction = veniceAdmin
          .versionsForStore(clusterName, storeName)
          .stream().map(v -> v.getNumber())
          .max(Comparator.<Integer>naturalOrder()).orElseGet(() -> -1);

      int nextVersion = veniceAdmin.peekNextVersion(clusterName, storeName).getNumber();
      veniceAdmin.reserveVersion(clusterName, storeName, nextVersion);
      veniceAdmin.addVersion(clusterName, storeName, nextVersion, 1, 1);

      int maxVersionAfterAction = veniceAdmin
          .versionsForStore(clusterName, storeName)
          .stream().map(v -> v.getNumber())
          .max(Comparator.<Integer>naturalOrder()).orElseGet(() -> -1);

      Assert.assertEquals(maxVersionAfterAction, nextVersion,
          "Max version after creation must be same as peeked version");
      Assert.assertNotEquals(maxVersionAfterAction, maxVersionBeforeAction,
          "Max version after creation must be different than before");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  //@Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  @Test
  public void testControllerFailOver()
      throws Exception {
    String storeName = TestUtils.getUniqueString("test");
    Version version = new Version(storeName, 1);
    veniceAdmin.addStore(clusterName, storeName, "dev");
    veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);

    ControlMessageChannel channel = new HelixControlMessageChannel(manager, Integer.MAX_VALUE, 1);
    channel.sendToController(new StoreStatusMessage(version.kafkaTopicName(), 0, nodeId, ExecutionStatus.STARTED));

    int newAdminPort = config.getAdminPort()+1; /* Note: this is a dummy port */
    PropertyBuilder builder = new PropertyBuilder()
        .put(controllerProps.toProperties())
        .put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(newConfig);
    //Start stand by controller
    newMasterAdmin.start(clusterName);
    List<VeniceHelixAdmin> allAdmins = new ArrayList<>();
    allAdmins.add(veniceAdmin);
    allAdmins.add(newMasterAdmin);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    try {
      newMasterAdmin.addStore(clusterName, "failedStore", "dev");
      Assert.fail("Can not add store through a standby controller");
    } catch (VeniceException e) {
      //expected
    }

    //Stop original master.
    veniceAdmin.stop(clusterName);
    //wait master change event
    waitUntilIsMaster(newMasterAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
    //Now get status from new master controller.
    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, version.kafkaTopicName()), ExecutionStatus.STARTED,
        "Can not get offline job status correctly.");
    channel.sendToController(new StoreStatusMessage(version.kafkaTopicName(), 0, nodeId, ExecutionStatus.COMPLETED));
    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, version.kafkaTopicName()), ExecutionStatus.COMPLETED,
        "Job should be completed after getting update from message channel");

    // Stop and start participant to use new master to trigger state transition.
    stopParticipant();
    HelixRoutingDataRepository routing = newMasterAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
    //Assert routing data repository can find the new master controller.
    Assert.assertEquals(routing.getMasterController().getPort(), newAdminPort,
        "Master controller is changed, now" + newAdminPort + " is used.");
    Thread.sleep(1000l);
    Assert.assertTrue(routing.getInstances(version.kafkaTopicName(), 0).isEmpty(),
        "Participant became offline. No instance should be living in test_v1");
    startParticipant();
    Thread.sleep(1000l);
    //New master controller create resource and trigger state transition on participant.
    newMasterAdmin.incrementVersion(clusterName, storeName, 1, 1);
    Version newVersion = new Version(storeName,2);
    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, newVersion.kafkaTopicName()), ExecutionStatus.STARTED,
            "Can not trigger state transition from new master");

    //Start original controller again, now it should become leader again based on Helix's logic.
    veniceAdmin.start(clusterName);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    // This should not fail, as it  should be the master again.
    veniceAdmin.addStore(clusterName, "failedStore", "dev");
    newMasterAdmin.stop(clusterName);
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  public void testIsMasterController()
      throws IOException, InterruptedException {
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName),
        "The default controller should be the master controller.");

    int newAdminPort = config.getAdminPort()+1; /* Note: dummy port */
    PropertyBuilder builder = new PropertyBuilder()
        .put(controllerProps.toProperties())
        .put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(newConfig);
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
  public void testMultiCluster(){
    String newClusterName = "new_test_cluster";
    PropertyBuilder builder = new PropertyBuilder()
        .put(controllerProps.toProperties())
        .put("cluster.name", newClusterName);

    VeniceProperties newClusterProps = builder.build();
    VeniceControllerConfig newClusterConfig = new VeniceControllerConfig(newClusterProps);

    veniceAdmin.addConfig(newClusterName, newClusterConfig);
    veniceAdmin.start(newClusterName);
    waitUntilIsMaster(veniceAdmin, newClusterName, MASTER_CHANGE_TIMEOUT);

    Assert.assertTrue(veniceAdmin.isMasterController(clusterName));
    Assert.assertTrue(veniceAdmin.isMasterController(newClusterName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartition(){
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    veniceAdmin.addStore(clusterName, "test", "dev");

    long storeSize = partitionSize*(minPartitionNumber+1);
    int numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, storeSize / partitionSize,
        "Number partition is smaller than max and bigger than min. So use the calculated result.");
    storeSize = 1;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition,minPartitionNumber, "Store size is too small so should use min number of partitions.");
    storeSize = partitionSize*(maxPartitionNumber+1);
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition,maxPartitionNumber, "Store size is too big, should use max number of paritions.");

    storeSize = Long.MAX_VALUE;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber, "Partition is overflow from Integer, use max one.");
    storeSize = -1;
    try{
      numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
      Assert.fail("Invalid store.");
    }catch (VeniceException e){
      //expected.
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartitionsFromPreviousVersion() {
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    veniceAdmin.addStore(clusterName, "test", "dev");
    long storeSize = partitionSize * (minPartitionNumber) + 1;
    int numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Version v = veniceAdmin.incrementVersion(clusterName, "test", numberOfParition, 1);
    veniceAdmin.setCurrentVersion(clusterName, "test", v.getNumber());
    Store store = veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().getStore("test");
    store.setPartitionCount(numberOfParition);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);

    v = veniceAdmin.incrementVersion(clusterName, "test", maxPartitionNumber, 1);
    veniceAdmin.setCurrentVersion(clusterName, "test", v.getNumber());
    storeSize = partitionSize * (maxPartitionNumber - 2);
    numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, "test", storeSize);
    Assert.assertEquals(numberOfParition, minPartitionNumber,
        "Should use the number of partition from previous version");
  }

  void waitUntilIsMaster(VeniceHelixAdmin admin, String cluster, long timeout){
    List<VeniceHelixAdmin> admins = Collections.singletonList(admin);
    waitForAMaster(admins, cluster, timeout);
  }

  void waitForAMaster(List<VeniceHelixAdmin> admins, String cluster, long timeout){
    int sleepDuration = 100;
    for (long i=0; i<timeout; i+= sleepDuration){

      boolean aMaster = false;
      for (VeniceHelixAdmin admin : admins){
        if (admin.isMasterController(cluster)){
          aMaster = true;
          break;
        }
      }

      if (aMaster){
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
  public void testDeleteOldVersions()
      throws InterruptedException {
    String storeName = "test";
    veniceAdmin.addStore(clusterName,storeName,"owner");
    Version version = null;
    for(int i=0;i<3;i++) {
      version = veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      VeniceJobManager jobManager = veniceAdmin.getVeniceHelixResource(clusterName).getJobManager();
      jobManager.handleMessage(new StoreStatusMessage(version.kafkaTopicName(), 0, nodeId, ExecutionStatus.STARTED));
      jobManager.handleMessage(new StoreStatusMessage(version.kafkaTopicName(), 0, nodeId, ExecutionStatus.COMPLETED));

      long startTime = System.currentTimeMillis();
      Store store = null;
      do {
        Thread.sleep(300);
        if (System.currentTimeMillis() - startTime > 3000) {
          Assert.fail("Time out while waiting for status update message for topic:"+version.kafkaTopicName());
        }
      }while(veniceAdmin.getCurrentVersion(clusterName,storeName)!=version.getNumber());
    }

    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName,storeName).size(),2, "Only keep 2 version for each store");
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName,storeName), version.getNumber());
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName,storeName).get(0).getNumber(), version.getNumber()-1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName,storeName).get(1).getNumber(), version.getNumber());
  }

  @Test
  public void testCurrentVersion(){
    String storeName = "test";
    veniceAdmin.addStore(clusterName,storeName,"owner");
    Version version = veniceAdmin.incrementVersion(clusterName,storeName,1,1);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName,storeName),0);
    veniceAdmin.setCurrentVersion(clusterName,storeName,version.getNumber());
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName,storeName),version.getNumber());

    try{
      veniceAdmin.setCurrentVersion(clusterName,storeName,100);
      Assert.fail("Version 100 does not exist. Should be failed.");
    }catch (VeniceException e){
      //expected
    }
  }

  @Test
  public void testAddVersion(){
    String storeName = "test";
    try {
      veniceAdmin.incrementVersion(clusterName, storeName, 1, 1);
      Assert.fail(storeName+" does not exist.");
    }catch(VeniceException e){
      //Expected
    }

    veniceAdmin.addStore(clusterName,storeName,"owner");
    veniceAdmin.addVersion(clusterName,storeName,1,1,1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName,storeName).size(), 1);
    try {
      veniceAdmin.addVersion(clusterName, storeName, 1, 1, 1);
      Assert.fail("Version 1 has already existed");
    }catch(Exception e){
      //Expected
    }

    veniceAdmin.reserveVersion(clusterName,storeName,100);
    veniceAdmin.addVersion(clusterName,storeName,101,1,1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName,storeName).size(),2);
  }
}

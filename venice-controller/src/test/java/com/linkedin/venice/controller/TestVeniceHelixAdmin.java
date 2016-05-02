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
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  public static final long MASTER_CHANGE_TIMEOUT = 10*1000; /* 10 seconds */

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
            .put("zookeeper.address", zkAddress);

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

  @Test
  public void testStartClusterAndCreatePush()
      throws Exception {
    try {
      veniceAdmin.addStore(clusterName, "test", "dev");
      veniceAdmin.incrementVersion(clusterName, "test", 1, 1);
      Assert.assertEquals(veniceAdmin.getOffLineJobStatus(clusterName, "test_v1"), ExecutionStatus.STARTED,
          "Can not get offline job status correctly.");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  @Test
  public void testControllerFailOver()
      throws Exception {
    veniceAdmin.addStore(clusterName, "test", "dev");
    veniceAdmin.incrementVersion(clusterName, "test", 1, 1);

    ControlMessageChannel channel = new HelixControlMessageChannel(manager, Integer.MAX_VALUE, 1);
    channel.sendToController(new StoreStatusMessage(1, "test_v1", 0, nodeId, ExecutionStatus.STARTED));

    int newAdminPort = config.getAdminPort()+1;
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
    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, "test_v1"), ExecutionStatus.STARTED,
        "Can not get offline job status correctly.");
    channel.sendToController(new StoreStatusMessage(1, "test_v1", 0, nodeId, ExecutionStatus.COMPLETED));

    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, "test_v1"), ExecutionStatus.COMPLETED,
        "Job should be completed after getting update from message channel");

    // Stop and start participant to use new master to trigger state transition.
    stopParticipant();
    HelixRoutingDataRepository routing = newMasterAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
    //Assert routing data repository can find the new master controller.
    Assert.assertEquals(routing.getMasterController().getPort(), newAdminPort,
        "Master controller is changed, now" + newAdminPort + " is used.");
    Thread.sleep(1000l);
    Assert.assertTrue(routing.getInstances("test_v1", 0).isEmpty(),
        "Participant became offline. No instance should be living in test_v1");
    startParticipant();
    Thread.sleep(1000l);
    //New master controller create resource and trigger state transition on participant.
    newMasterAdmin.incrementVersion(clusterName, "test", 1, 1);
    Assert.assertEquals(newMasterAdmin.getOffLineJobStatus(clusterName, "test_v2"), ExecutionStatus.STARTED,
            "Can not trigger state transition from new master");

    //Start original controller again, now it should become leader again based on Helix's logic.
    veniceAdmin.start(clusterName);
    waitForAMaster(allAdmins, clusterName, MASTER_CHANGE_TIMEOUT);
    // This should not fail, as it  should be the master again.
    veniceAdmin.addStore(clusterName, "failedStore", "dev");
    newMasterAdmin.stop(clusterName);
  }

  @Test
  public void testIsMasterController()
      throws IOException, InterruptedException {
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName),
        "The default controller should be the master controller.");

    int newAdminPort = config.getAdminPort()+1;
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

  @Test
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

}

package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataChangedListener;
import com.linkedin.venice.utils.PortUtils;
import com.linkedin.venice.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.venice.utils.ZkServerWrapper;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test case for HelixRoutingDataRepository.
 */
public class TestHelixRoutingDataRepository {
  // Test behavior configuration
  private static final int WAIT_TIME = 1000; // FIXME: Non-deterministic. Will lead to flaky tests.

  private HelixManager manager;
  private HelixManager controller;
  private HelixAdmin admin;
  private String clusterName = "UnitTestCLuster";
  private String resourceName = "UnitTest";
  private String zkAddress;
  private int httpPort;
  private int adminPort;
  private ZkServerWrapper zkServerWrapper;
  private HelixRoutingDataRepository repository;
  private HelixManager readManager;

  @BeforeMethod
  public void HelixSetup()
      throws Exception {
    zkServerWrapper = ZkServerWrapper.getZkServer();
    zkAddress = zkServerWrapper.getZkAddress();
    httpPort = PortUtils.getFreePort();
    adminPort = PortUtils.getFreePort();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(clusterName);
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(clusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(clusterName, UnitTestStateModel.UNIT_TEST_STATE_MODEL, UnitTestStateModel.getDefinition());

    admin.addResource(clusterName, resourceName, 1, UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 1);

    controller = HelixControllerMain
        .startHelixController(zkAddress, clusterName, "UnitTestController", HelixControllerMain.STANDALONE);

    String nodeId = Utils.getHostName() + "_" + httpPort;
    manager = HelixManagerFactory.getZKHelixManager(clusterName, nodeId, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory(UnitTestStateModel.UNIT_TEST_STATE_MODEL, new UnitTestStateModelFactory());
    Instance instance = new Instance(nodeId, Utils.getHostName(), adminPort, httpPort);
    manager.setLiveInstanceInfoProvider(new LiveInstanceInfoProvider() {
      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        return HelixInstanceConverter.convertInstanceToZNRecord(instance);
      }
    });

    manager.connect();
    //Waiting essential notification from ZK.
    Thread.sleep(WAIT_TIME);

    readManager = HelixManagerFactory.getZKHelixManager(clusterName, "reader", InstanceType.SPECTATOR, zkAddress);
    readManager.connect();
    repository = new HelixRoutingDataRepository(readManager);
    repository.start();
  }

  @AfterMethod
  public void HelixCleanup() {
    manager.disconnect();
    readManager.disconnect();
    controller.disconnect();
    admin.dropCluster(clusterName);
    admin.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetInstances()
      throws Exception {
    List<Instance> instances = repository.getInstances(resourceName, 0);
    Assert.assertEquals(1, instances.size());
    Instance instance = instances.get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getHttpPort());
    Assert.assertEquals(adminPort, instance.getAdminPort());

    //Participant become off=line.
    manager.disconnect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    //No online instance now.
    instances = repository.getInstances(resourceName, 0);
    Assert.assertEquals(0, instances.size());
  }

  @Test
  public void testGetNumberOfPartitions()
      throws Exception {
    Assert.assertEquals(1, repository.getNumberOfPartitions(resourceName));
    //Participant become off=line.
    manager.disconnect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    //Result should be same.
    Assert.assertEquals(1, repository.getNumberOfPartitions(resourceName));
  }
  @Test
  public void testGetNumberOfPartitionsWhenResourceDropped()
      throws Exception {
    Assert.assertTrue(admin.getResourcesInCluster(clusterName).contains(resourceName));
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    admin.dropResource(clusterName, resourceName);
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertFalse(admin.getResourcesInCluster(clusterName).contains(resourceName));
    try {
      //Should not find the resource.
      repository.getNumberOfPartitions(resourceName);
      Assert.fail("IAE should be thrown because resource dose not exist now.");
    }catch(IllegalArgumentException iae){
      //expected
    }
  }

  @Test
  public void testGetPartitions()
      throws Exception {
    Map<Integer, Partition> partitions = repository.getPartitions(resourceName);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(1, partitions.get(0).getInstances().size());

    Instance instance = partitions.get(0).getInstances().get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getHttpPort());
    Assert.assertEquals(adminPort, instance.getAdminPort());

    //Participant become off=line.
    manager.disconnect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    partitions = repository.getPartitions(resourceName);
    //No online partition now
    Assert.assertEquals(0, partitions.size());
  }

  @Test
  public void testListeners()
      throws Exception {
    final boolean[] isNoticed = {false};
    RoutingDataChangedListener listener = new RoutingDataChangedListener() {
      @Override
      public void handleRoutingDataChange(String kafkaTopic, Map<Integer, Partition> partitions) {
        isNoticed[0] = true;
      }
    };

    repository.subscribeRoutingDataChange(resourceName, listener);
    //Participant become off=line.
    manager.disconnect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(isNoticed[0], true, "Can not get notification from repository.");

    isNoticed[0] = false;
    repository.unSubscribeRoutingDataChange(resourceName, listener);
    manager.connect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(isNoticed[0], false, "Should not get notification after un-registering.");
  }

  public static class UnitTestStateModel {

    public static final String UNIT_TEST_STATE_MODEL = "UnitTestStateModel";

    public static StateModelDefinition getDefinition() {

      StateModelDefinition.Builder builder = new StateModelDefinition.Builder(UNIT_TEST_STATE_MODEL);

      builder.addState(HelixState.ONLINE.toString(), 1);
      builder.addState(HelixState.OFFLINE.toString());
      builder.addState(HelixState.DROPPED.toString());
      builder.initialState(HelixState.OFFLINE.toString());
      builder.addTransition(HelixState.OFFLINE.toString(), HelixState.ONLINE.toString());
      builder.addTransition(HelixState.ONLINE.toString(), HelixState.OFFLINE.toString());
      builder.addTransition(HelixState.OFFLINE.toString(), HelixDefinedState.DROPPED.toString());
      builder.dynamicUpperBound(HelixState.ONLINE.toString(), "R");

      return builder.build();
    }
  }

  public static class UnitTestStateModelFactory extends StateModelFactory<StateModel> {
    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
      return stateModel;
    }

    @StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFLINE")
    public static class OnlineOfflineStateModel extends StateModel {
      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      }
    }
  }
}

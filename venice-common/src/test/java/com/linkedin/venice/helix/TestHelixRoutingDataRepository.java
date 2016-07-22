package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
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
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
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

    httpPort = 50000 + (int)(System.currentTimeMillis() % 10000); //port never actually used
    adminPort = 50000 + (int)(System.currentTimeMillis() % 10000) +1; //port never actually used
    controller = HelixControllerMain
        .startHelixController(zkAddress, clusterName, Utils.getHelixNodeIdentifier(adminPort), HelixControllerMain.STANDALONE);

    manager = createParticipant(httpPort);
    manager.connect();
    //Waiting essential notification from ZK. TODO: use a listener to find out when ZK is ready
    Thread.sleep(WAIT_TIME);

    readManager = HelixManagerFactory.getZKHelixManager(clusterName, "reader", InstanceType.SPECTATOR, zkAddress);
    readManager.connect();
    repository = new HelixRoutingDataRepository(readManager);
    repository.refresh();
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

  private HelixManager createParticipant(int port, UnitTestStateModelFactory factory){
    HelixManager manager = HelixManagerFactory.getZKHelixManager(clusterName, Utils.getHelixNodeIdentifier(port), InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory(UnitTestStateModel.UNIT_TEST_STATE_MODEL, factory);
    Instance instance = new Instance(Utils.getHelixNodeIdentifier(port), Utils.getHostName(), port);
    manager.setLiveInstanceInfoProvider(new LiveInstanceInfoProvider() {
      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        return HelixInstanceConverter.convertInstanceToZNRecord(instance);
      }
    });
    return manager;
  }
  private HelixManager createParticipant(int port){
    //Create participatn with out transition delay.
    return createParticipant(port, new UnitTestStateModelFactory());
  }

  @Test
  public void testGetInstances()
      throws Exception {
    // TODO: Fix this test. It is flaky, especially when running on a slow or heavily loaded machine.
    List<Instance> instances = repository.getReadyToServeInstances(resourceName, 0);
    Assert.assertEquals(1, instances.size());
    Instance instance = instances.get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getPort());
    //Participant become offline.
    manager.disconnect();
    //Wait notification.
    Thread.sleep(WAIT_TIME);
    //No online instance now.
    instances = repository.getReadyToServeInstances(resourceName, 0);
    Assert.assertEquals(0, instances.size());
    int newHttpPort = httpPort+10;
    HelixManager newManager = createParticipant(newHttpPort);
    newManager.connect();
    Thread.sleep(WAIT_TIME);
    instances = repository.getReadyToServeInstances(resourceName, 0);
    Assert.assertEquals(instances.size(), 1);
    Assert.assertEquals(instances.get(0).getPort(), newHttpPort);
    newManager.disconnect();
  }

  @Test
  public void testGetNumberOfPartitions()
      throws Exception {
    Assert.assertEquals(1, repository.getNumberOfPartitions(resourceName));
    //Participant become offline.
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
    }catch(VeniceException iae){
      //expected
    }
  }

  @Test
  public void testGetPartitions()
      throws Exception {
    Map<Integer, Partition> partitions = repository.getPartitions(resourceName);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(1, partitions.get(0).getReadyToServeInstances().size());
    Assert.assertEquals(1, partitions.get(0).getBootstrapAndReadyToServeInstances().size());

    Instance instance = partitions.get(0).getReadyToServeInstances().get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getPort());

    Instance liveInstance = partitions.get(0).getBootstrapAndReadyToServeInstances().get(0);
    Assert.assertEquals(liveInstance, instance);

    //Participant become offline.
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
    RoutingDataRepository.RoutingDataChangedListener listener = new RoutingDataRepository.RoutingDataChangedListener() {
      @Override
      public void onRoutingDataChanged(String kafkaTopic, Map<Integer, Partition> partitions) {
        isNoticed[0] = true;
      }
    };

    repository.subscribeRoutingDataChange(resourceName, listener);
    //Participant become offline.
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

  @Test
  public void testControllerChanged()
      throws Exception {
    Instance master = repository.getMasterController();
    Assert.assertEquals(master.getHost(), Utils.getHostName());
    Assert.assertEquals(master.getPort(), adminPort);

    //Start up stand by controller by different port
    int newAdminPort = adminPort + 1;
    HelixManager newMaster = HelixManagerFactory
        .getZKHelixManager(clusterName, Utils.getHelixNodeIdentifier(newAdminPort), InstanceType.CONTROLLER, zkAddress);
    newMaster.connect();
    //Stop master and wait stand by become master
    controller.disconnect();
    Thread.sleep(1000l);
    master = repository.getMasterController();
    Assert.assertEquals(master.getHost(), Utils.getHostName());
    Assert.assertEquals(master.getPort(), newAdminPort);

    newMaster.disconnect();
  }

  @Test
  public void testNodeChanged()
      throws InterruptedException {
    manager.disconnect();
    Utils.waitForNonDeterministicCompetion(WAIT_TIME, TimeUnit.MILLISECONDS, new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        return repository.getReadyToServeInstances(resourceName, 0).size() == 0;
      }
    });
    Assert.assertEquals(repository.getReadyToServeInstances(resourceName, 0).size(), 0);
    Assert.assertEquals(repository.getPartitions(resourceName).size(), 0);
  }

  @Test
  public void testGetBootstrapInstances()
      throws Exception {
    manager.disconnect();
    //Create new factory to create delayed state model.
    UnitTestStateModelFactory factory = new UnitTestStateModelFactory();
    factory.isDelay = true;
    manager = createParticipant(httpPort + 1, factory);
    manager.connect();
    Thread.sleep(WAIT_TIME);

    // because bootstrap to online transition is blocked, so there is only one bootstrap instance.
    Assert.assertEquals(repository.getReadyToServeInstances(resourceName, 0).size(), 0,
        "Transition should be delayed, so there is no online instance.");
    Assert.assertEquals(repository.getPartitions(resourceName).size(), 1);
    Assert.assertEquals(repository.getPartitions(resourceName).get(0).getBootstrapAndReadyToServeInstances().size(), 1,
        "One bootstrap instance should be found");
    // make bootstrap to online transition completed, now there is one online instance.
    factory.makeTransitionCompleted(resourceName, 0);
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(repository.getReadyToServeInstances(resourceName, 0).size(), 1, "One online instance should be found");
    Assert.assertEquals(repository.getPartitions(resourceName).size(), 1);
    Assert.assertEquals(repository.getPartitions(resourceName).get(0).getBootstrapAndReadyToServeInstances().size(), 1,
        "One online instance should be found");
    Assert.assertEquals(repository.getPartitions(resourceName).get(0).getReadyToServeInstances().size(), 1,
        "One online instance should be found");
  }

  public static class UnitTestStateModel {

    public static final String UNIT_TEST_STATE_MODEL = "UnitTestStateModel";

    public static StateModelDefinition getDefinition() {

      StateModelDefinition.Builder builder = new StateModelDefinition.Builder(UNIT_TEST_STATE_MODEL);

      builder.addState(HelixState.ONLINE.toString(), 1);
      builder.addState(HelixState.OFFLINE.toString());
      builder.addState(HelixState.BOOTSTRAP.toString());
      builder.addState(HelixState.DROPPED.toString());
      builder.initialState(HelixState.OFFLINE.toString());
      builder.addTransition(HelixState.OFFLINE.toString(), HelixState.BOOTSTRAP.toString());
      builder.addTransition(HelixState.BOOTSTRAP.toString(), HelixState.ONLINE.toString());
      builder.addTransition(HelixState.ONLINE.toString(), HelixState.OFFLINE.toString());
      builder.addTransition(HelixState.OFFLINE.toString(), HelixDefinedState.DROPPED.toString());
      builder.dynamicUpperBound(HelixState.ONLINE.toString(), "R");

      return builder.build();
    }
  }

  public static class UnitTestStateModelFactory extends StateModelFactory<StateModel> {
    private boolean isDelay = false;
    private Map<String, CountDownLatch> modelTolatchMap = new HashMap<>();
    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel(isDelay);
      CountDownLatch latch = new CountDownLatch(1);
      modelTolatchMap.put(resourceName + "_" + HelixUtils.getPartitionId(partitionName) , latch);
      stateModel.latch = latch;
      return stateModel;
    }

    public void setDelayTransistion(boolean isDelay){
      this.isDelay = isDelay;
    }

    public void makeTransitionCompleted(String resourceName, int partitionId) {
      modelTolatchMap.get(resourceName + "_" + partitionId).countDown();
    }

    @StateModelInfo(states = "{'OFFLINE','ONLINE','BOOTSTRAP'}", initialState = "OFFLINE")
    public static class OnlineOfflineStateModel extends StateModel {
      private boolean isDelay;

      OnlineOfflineStateModel(boolean isDelay){
        this.isDelay = isDelay;
      }

      private CountDownLatch latch;
      @Transition(from = "OFFLINE", to = "BOOTSTRAP")
      public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "BOOTSTRAP", to = "ONLINE")
      public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context)
          throws InterruptedException {
        if (isDelay) {
          // mock the delay during becoming online.
          latch.await();
        }
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      }
    }
  }
}

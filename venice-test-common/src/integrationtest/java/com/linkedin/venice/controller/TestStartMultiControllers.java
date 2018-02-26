package com.linkedin.venice.controller;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStartMultiControllers {
  public static final long TEST_TIME_OUT_MS = 15000l;

  /**
   * Test could we start multiple controllers, especially the number of controller is larger than the required number of
   * one cluster. Make sure the extra controller could be started as normal but do not join the cluster which is full.
   * Then after failing one of controller in that cluster, the controller which did not join would join the cluster
   * instead.
   */
  @Test
  public void testStartControllersMoreThanRequiredForOneCluster()
      throws Exception {
    int numberOfControler = 4;
    // Start a cluster with 4 controllers but the cluster only need 3 controller by default.
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(numberOfControler, 1, 1);
    String clusterName = cluster.getClusterName();
    String partitionName = HelixUtils.getPartitionName(clusterName, 0);
    HelixManager manager =
        new ZKHelixManager(clusterName, "testStartControllersMoreThanRequiredForOneCluster", InstanceType.SPECTATOR,
            cluster.getZk().getAddress());
    manager.connect();
    //Assert there are 3 controllers join the cluster.
    ExternalView externalView = getExternViewOfCluster(cluster, manager);
    Assert.assertEquals(externalView.getStateMap(partitionName).keySet().size(), 3);

    VeniceControllerWrapper controllerNotInCluster = null;
    Map<String, String> controllerToStateMap = getExternViewOfCluster(cluster, manager).getStateMap(partitionName);
    //Find the one which did not join the cluster and test whether it can get master controller correctly.
    for (VeniceControllerWrapper controller : cluster.getVeniceControllers()) {
      String instanceId = Utils.getHelixNodeIdentifier(controller.getPort());

      if (!controllerToStateMap.containsKey(instanceId)) {
        // The controller which did not join the cluster.
        String masterInstanceId = controller.getVeniceAdmin().getMasterController(clusterName).getNodeId();
        controllerNotInCluster = controller;
        Assert.assertEquals(masterInstanceId,
            Utils.getHelixNodeIdentifier(cluster.getMasterVeniceController().getPort()),
            "Controller that did not join cluster can not get the master controller correctly");
        break;
      }
    }
    TestUtils.waitForNonDeterministicAssertion(3000, TimeUnit.MILLISECONDS, () -> {
      int masterControllerCntForControllerCluster = 0;
      for (VeniceControllerWrapper controller : cluster.getVeniceControllers()) {
        if (controller.isMasterControllerForControllerCluster()) {
          masterControllerCntForControllerCluster++;
        }
      }
      Assert.assertEquals(masterControllerCntForControllerCluster, 1, "There should be only one"
          + " master controller for controller cluster");
    });
    String instanceIdOfControllerNotInCluster = Utils.getHelixNodeIdentifier(controllerNotInCluster.getPort());
    //Stop master controller and make sure the controller which did not join in is join in cluster right now.
    cluster.stopMasterVeniceControler();
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      ExternalView exView = getExternViewOfCluster(cluster, manager);
      return exView.getStateMap(partitionName).containsKey(instanceIdOfControllerNotInCluster);
    });

    manager.disconnect();
    cluster.close();
  }

  private ExternalView getExternViewOfCluster(VeniceClusterWrapper cluster, HelixManager manager) {
    String clusterName = cluster.getClusterName();
    String controllerClusterName = "venice-controllers";
    // Check whether enough controller has been assigned.

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
    ExternalView externalView = manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));

    return externalView;
  }
}

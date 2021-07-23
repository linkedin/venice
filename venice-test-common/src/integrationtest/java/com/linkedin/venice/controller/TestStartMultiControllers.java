package com.linkedin.venice.controller;

import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;


public class TestStartMultiControllers {
  final int minControllerCount = 3;
  final int controllerCount = minControllerCount + 1;
  private VeniceClusterWrapper cluster;
  private SafeHelixManager helixManager;

  @BeforeClass
  public void setUp() throws Exception {
    cluster = ServiceFactory.getVeniceCluster(controllerCount, 0, 0);
    helixManager = new SafeHelixManager(new ZKHelixManager(
        cluster.getClusterName(), TestUtils.getUniqueString(), InstanceType.SPECTATOR, cluster.getZk().getAddress()));
    helixManager.connect();
  }

  @AfterClass
  public void cleanUp() {
    if (null != helixManager) {
      helixManager.disconnect();
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  /**
   * Test that we can start multiple controllers, especially when number of controllers exceeds required number for a
   * cluster. Make sure that the extra controller can be started and will join the cluster if one of the controllers fail.
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testStartMoreThanRequiredControllersForOneCluster() {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () ->
      Assert.assertTrue(getActiveControllerCount(helixManager) >= minControllerCount,
          "Not enough active controllers in the cluster"));

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      int masterControllerCount = 0;
      for (VeniceControllerWrapper controller : cluster.getVeniceControllers()) {
        if (controller.isMasterControllerOfControllerCluster()) {
          masterControllerCount++;
        }
      }
      Assert.assertEquals(masterControllerCount, 1, "There should be only one master controller in the cluster");
    });

    int oldMasterControllerPort = cluster.stopMasterVeniceControler();

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () ->
      Assert.assertTrue(getActiveControllerCount(helixManager) >= minControllerCount,
          "Not enough active controllers in the cluster"));

    Assert.assertNotSame(cluster.getMasterVeniceController().getPort(), oldMasterControllerPort);
  }

  private int getActiveControllerCount(SafeHelixManager helixManager) {
    String clusterName = helixManager.getClusterName();
    String partitionName = HelixUtils.getPartitionName(clusterName, 0);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder("venice-controllers");
    ExternalView view = helixManager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
    return view.getStateMap(partitionName).size();
  }
}

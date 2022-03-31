package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;


public class TestWritePathComputation {
  private static final Logger logger = Logger.getLogger(TestWritePathComputation.class);
  private static final long GET_LEADER_CONTROLLER_TIMEOUT = 20 * Time.MS_PER_SECOND;

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFeatureFlagSingleDC() {
    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(1, 1, 1, 0)) {
      String clusterName = multiClusterWrapper.getClusterNames()[0];
      String storeName = "test-store0";

      // Create store
      Admin admin = multiClusterWrapper.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      admin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
      Assert.assertTrue(admin.hasStore(clusterName, storeName));
      Assert.assertFalse(admin.getStore(clusterName, storeName).isWriteComputationEnabled());

      // Set flag
      String controllerUrl = multiClusterWrapper.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getControllerUrl();
      try (ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl)) {
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
        Assert.assertTrue(admin.getStore(clusterName, storeName).isWriteComputationEnabled());

        // Reset flag
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(false));
        Assert.assertFalse(admin.getStore(clusterName, storeName).isWriteComputationEnabled());
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFeatureFlagMultipleDC() {
    try (VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        1, 1, 1, 1, 1, 0)) {

      VeniceMultiClusterWrapper multiCluster = twoLayerMultiColoMultiClusterWrapper.getClusters().get(0);
      VeniceControllerWrapper parentController = twoLayerMultiColoMultiClusterWrapper.getParentControllers().get(0);
      String clusterName = multiCluster.getClusterNames()[0];
      String storeName = "test-store0";

      // Create store
      Admin parentAdmin = twoLayerMultiColoMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName).getVeniceAdmin();
      Admin childAdmin = multiCluster.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      parentAdmin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(parentAdmin.hasStore(clusterName, storeName));
        Assert.assertTrue(childAdmin.hasStore(clusterName, storeName));
        Assert.assertFalse(parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
        Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
      });

      // Set flag
      String parentControllerUrl = parentController.getControllerUrl();
      try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl)) {
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertTrue(parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
          Assert.assertTrue(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
        });

        // Reset flag
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(false));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
          Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
        });
      }
    }
  }
}

package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWritePathComputation {
  private static final long GET_LEADER_CONTROLLER_TIMEOUT = 20 * Time.MS_PER_SECOND;

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFeatureFlagSingleDC() {
    VeniceMultiClusterCreateOptions options = new VeniceMultiClusterCreateOptions.Builder(1).numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(0)
        .build();
    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(options)) {
      String clusterName = multiClusterWrapper.getClusterNames()[0];
      String storeName = "test-store0";

      // Create store
      Admin admin =
          multiClusterWrapper.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      admin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
      Assert.assertTrue(admin.hasStore(clusterName, storeName));
      Assert.assertFalse(admin.getStore(clusterName, storeName).isWriteComputationEnabled());

      // Set flag
      String controllerUrl =
          multiClusterWrapper.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getControllerUrl();
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
    try (VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(1, 1, 1, 1, 1, 0)) {

      VeniceMultiClusterWrapper multiCluster = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
      VeniceControllerWrapper parentController = twoLayerMultiRegionMultiClusterWrapper.getParentControllers().get(0);
      String clusterName = multiCluster.getClusterNames()[0];
      String storeName = "test-store0";

      // Create store
      Admin parentAdmin =
          twoLayerMultiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName).getVeniceAdmin();
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
        ControllerResponse response = parentControllerClient
            .updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
        Assert.assertTrue(response.isError());
        Assert.assertTrue(response.getError().contains("top level field probably missing defaults"));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(
              parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled(),
              "Write Compute should not be enabled before the value schema is not a Record.");
          Assert.assertFalse(
              childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled(),
              "Write Compute should not be enabled before the value schema is not a Record.");
        });

        // Reset flag
        response = parentControllerClient
            .updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(false));
        Assert.assertFalse(response.isError(), "No error is expected to disable Write Compute (that was not enabled)");
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
          Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
        });
      }
    }
  }
}

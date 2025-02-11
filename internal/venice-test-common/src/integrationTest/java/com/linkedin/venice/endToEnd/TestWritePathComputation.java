package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWritePathComputation {
  private static final long GET_LEADER_CONTROLLER_TIMEOUT = 20 * Time.MS_PER_SECOND;
  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String SECOND_VALUE_FIELD_NAME = "opt_int_field";
  private static final String VALUE_SCHEMA_V2_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\", \"default\": 10},\n"
      + "{\"name\": \"" + SECOND_VALUE_FIELD_NAME + "\", \"type\": [\"null\", \"int\"], \"default\": null}]\n" + "}";

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFeatureFlagSingleDC() {
    VeniceMultiClusterCreateOptions options = new VeniceMultiClusterCreateOptions.Builder().numberOfClusters(1)
        .numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(0)
        .regionName(VeniceClusterWrapperConstants.STANDALONE_REGION_NAME)
        .build();
    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(options)) {
      String clusterName = multiClusterWrapper.getClusterNames()[0];
      VeniceControllerWrapper childController = multiClusterWrapper.getLeaderController(clusterName);
      String storeName = "test-store0";

      // Create store
      Admin childAdmin =
          multiClusterWrapper.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      childAdmin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(childAdmin.hasStore(clusterName, storeName));
        Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
      });

      // Set flag
      String childControllerUrl = childController.getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl)) {
        assertCommand(
            childControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true)),
            "Write Compute should be enabled");
        Assert.assertTrue(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());

        // Reset flag
        assertCommand(
            childControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(false)));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
        });
      }
    }
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testFeatureFlagMultipleDC() {
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(0);
    try (VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build())) {

      VeniceMultiClusterWrapper multiCluster = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
      VeniceControllerWrapper parentController = twoLayerMultiRegionMultiClusterWrapper.getParentControllers().get(0);
      String clusterName = multiCluster.getClusterNames()[0];
      String storeName = "test-store0";
      String storeName2 = "test-store2";

      // Create store
      Admin parentAdmin =
          twoLayerMultiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName).getVeniceAdmin();
      Admin childAdmin = multiCluster.getLeaderController(clusterName, GET_LEADER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      parentAdmin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
      parentAdmin.createStore(clusterName, storeName2, "tester", KEY_SCHEMA_STR, VALUE_SCHEMA_V2_STR);
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

        ControllerResponse response2 = parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1000)
                .setHybridOffsetLagThreshold(1000)
                .setWriteComputationEnabled(true));
        Assert.assertTrue(response2.isError());
        Assert.assertTrue(response2.getError().contains("top level field probably missing defaults"));

        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(
              parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled(),
              "Write Compute should not be enabled before the value schema is not a Record.");
          Assert.assertFalse(
              childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled(),
              "Write Compute should not be enabled before the value schema is not a Record.");
        });

        assertCommand(
            parentControllerClient.updateStore(
                storeName2,
                new UpdateStoreQueryParams().setHybridRewindSeconds(1000)
                    .setHybridOffsetLagThreshold(1000)
                    .setWriteComputationEnabled(true)));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertTrue(parentAdmin.getStore(clusterName, storeName2).isWriteComputationEnabled());
          Assert.assertTrue(childAdmin.getStore(clusterName, storeName2).isWriteComputationEnabled());
        });

        // Reset flag
        assertCommand(
            parentControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(false)));
        assertCommand(
            parentControllerClient
                .updateStore(storeName2, new UpdateStoreQueryParams().setWriteComputationEnabled(false)));
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Assert.assertFalse(parentAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
          Assert.assertFalse(childAdmin.getStore(clusterName, storeName).isWriteComputationEnabled());
          Assert.assertFalse(parentAdmin.getStore(clusterName, storeName2).isWriteComputationEnabled());
          Assert.assertFalse(childAdmin.getStore(clusterName, storeName2).isWriteComputationEnabled());
        });
      }
    }
  }
}

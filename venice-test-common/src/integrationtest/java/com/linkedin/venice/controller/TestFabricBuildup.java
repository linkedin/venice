package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreComparisonResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;

public class TestFabricBuildup {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, "true");
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.empty(),
        Optional.of(properties),
        Optional.empty(),
        false,
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);

    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoresMetadataCopyOver() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    // Test the admin channel with the regular KMM pipeline
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    ControllerClient parentControllerClient = ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());

    ControllerClient dc0Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    // Create a test store only in dc0 colo
    NewStoreResponse newStoreResponse = dc0Client.retryableRequest(3, c -> c.createNewStore(storeName,
        "", "\"string\"", "\"string\""));
    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());
    checkStoreConfig(dc0Client, storeName);

    // Call metadata copy over to copy dc0's store configs to dc1
    parentControllerClient.copyOverStoresMetadata("dc-0","dc-1");
    ControllerClient dc1Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    checkStoreConfig(dc1Client, storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompareStore() {
    String clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController = parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
        ControllerClient childControllerClient0 = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient childControllerClient1 = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      String testStoreName = Utils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse = childControllerClient0.createNewStore(testStoreName, "test",
          "\"string\"", TestPushUtils.NESTED_SCHEMA_STRING);
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfig(childControllerClient0, testStoreName);
      newStoreResponse = childControllerClient1.createNewStore(testStoreName, "test", "\"string\"",
          TestPushUtils.NESTED_SCHEMA_STRING);
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfig(childControllerClient1, testStoreName);

      // Only modify the store in dc-0
      VersionCreationResponse versionCreationResponse = childControllerClient0.emptyPush(testStoreName,
          Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      SchemaResponse schemaResponse = childControllerClient0.addValueSchema(testStoreName, TestPushUtils.NESTED_SCHEMA_STRING_V2);
      Assert.assertFalse(schemaResponse.isError());

      StoreComparisonResponse response = parentControllerClient.compareStore(testStoreName, "dc-0", "dc-1");
      // Make sure the diffs are discovered.
      Assert.assertFalse(response.getPropertyDiff().isEmpty());
      Assert.assertFalse(response.getSchemaDiff().isEmpty());
      Assert.assertFalse(response.getVersionStateDiff().isEmpty());
    }
  }

  private static void checkStoreConfig(ControllerClient childControllerClient,
      String storeName) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      Assert.assertNotNull(storeInfo);
    });
  }
}

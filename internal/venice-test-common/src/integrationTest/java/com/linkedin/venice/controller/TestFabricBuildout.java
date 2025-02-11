package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreComparisonResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


/**
 * Note: These tests use an unsupported mode during setup - by creating stores and running pushes
 * to them directly in child regions. This is not how new regions will be added. We need the test setup to support
 * adding blank regions so that we can simulate the true fabric buildout process. Because of this, I've disabled these
 * tests for now.
 */
@Ignore
public class TestFabricBuildout {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private String[] clusterNames;
  private String[] dcNames;

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .childControllerProperties(childControllerProperties)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    clusterNames = multiRegionMultiClusterWrapper.getClusterNames();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoresMetadataCopyOver() {
    String clusterName = clusterNames[0];
    String storeName = Utils.getUniqueString("store");

    // Test the admin channel
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (
        ControllerClient parentControllerClient =
            ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls);
        ControllerClient dc0Client = ControllerClient
            .constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString())) {
      // Create a test store only in dc0 region
      NewStoreResponse newStoreResponse = dc0Client.retryableRequest(
          3,
          c -> c.createNewStore(storeName, "", "\"string\"", TestWriteUtils.USER_WITH_DEFAULT_SCHEMA.toString()));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());
      // Enable read compute to test superset schema registration.
      Assert.assertFalse(
          dc0Client.updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(true)).isError());
      Assert.assertFalse(
          dc0Client.addValueSchema(storeName, TestWriteUtils.USER_WITH_DEFAULT_SCHEMA.toString()).isError());
      checkStoreConfig(dc0Client, storeName);
      // Mimic source fabric store-level execution id
      Assert.assertFalse(
          dc0Client.updateAdminTopicMetadata(2L, Optional.of(storeName), Optional.empty(), Optional.empty()).isError());

      // Call metadata copy over to copy dc0's store configs to dc1
      parentControllerClient.copyOverStoreMetadata(dcNames[0], dcNames[1], storeName);
      ControllerClient dc1Client = ControllerClient
          .constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
      checkStoreConfig(dc1Client, storeName);
      AdminTopicMetadataResponse response = dc1Client.getAdminTopicMetadata(Optional.of(storeName));
      Assert.assertFalse(response.isError());
      Assert.assertEquals(response.getExecutionId(), 2L);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStartFabricBuildout() throws Exception {
    String clusterName = clusterNames[0];
    try (
        ControllerClient childControllerClient0 =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient childControllerClient1 =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      String testStoreName1 = Utils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse =
          childControllerClient0.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfig(childControllerClient0, testStoreName1);
      VersionCreationResponse versionCreationResponse =
          childControllerClient0.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      String testStoreName2 = Utils.getUniqueString("test-store");
      newStoreResponse = childControllerClient0.createNewStore(testStoreName2, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfigAndCurrentVersion(childControllerClient0, testStoreName1, 1);
      checkStoreConfig(childControllerClient0, testStoreName2);

      // Create some leftovers in the dest fabric. The leftovers should be cleaned up during fabric buildout.
      newStoreResponse = childControllerClient1.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      childControllerClient1.updateStore(testStoreName1, new UpdateStoreQueryParams().setPartitionCount(1));
      versionCreationResponse =
          childControllerClient1.emptyPush(testStoreName1, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      checkStoreConfigAndCurrentVersion(childControllerClient1, testStoreName1, 1);

      String[] args = { "--start-fabric-buildout", "--url", parentControllers.get(0).getControllerUrl(), "--cluster",
          clusterName, "--source-fabric", dcNames[0], "--dest-fabric", dcNames[1] };
      AdminTool.main(args);

      checkStoreConfigAndCurrentVersion(childControllerClient1, testStoreName1, 1);
      checkStoreConfig(childControllerClient1, testStoreName2);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompareStore() {
    String clusterName = clusterNames[0];
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls);
        ControllerClient childControllerClient0 =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient childControllerClient1 =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      String testStoreName = Utils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse = childControllerClient0.createNewStore(
          testStoreName,
          "test",
          TestWriteUtils.STRING_SCHEMA.toString(),
          TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString());
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfig(childControllerClient0, testStoreName);
      newStoreResponse = childControllerClient1.createNewStore(
          testStoreName,
          "test",
          TestWriteUtils.STRING_SCHEMA.toString(),
          TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString());
      Assert.assertFalse(newStoreResponse.isError());
      checkStoreConfig(childControllerClient1, testStoreName);

      // Only modify the store in dc-0
      VersionCreationResponse versionCreationResponse =
          childControllerClient0.emptyPush(testStoreName, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());
      SchemaResponse schemaResponse =
          childControllerClient0.addValueSchema(testStoreName, TestWriteUtils.NAME_RECORD_V2_SCHEMA.toString());
      Assert.assertFalse(schemaResponse.isError());

      StoreComparisonResponse response = parentControllerClient.compareStore(testStoreName, dcNames[0], dcNames[1]);
      // Make sure the diffs are discovered.
      Assert.assertFalse(response.getPropertyDiff().isEmpty());
      Assert.assertFalse(response.getSchemaDiff().isEmpty());
      Assert.assertFalse(response.getVersionStateDiff().isEmpty());
    }
  }

  private static void checkStoreConfig(ControllerClient childControllerClient, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      Assert.assertNotNull(storeInfo);
    });
  }

  private static void checkStoreConfigAndCurrentVersion(
      ControllerClient childControllerClient,
      String storeName,
      int versionNum) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      Assert.assertNotNull(storeResponse.getStore());
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), versionNum);
    });
  }
}

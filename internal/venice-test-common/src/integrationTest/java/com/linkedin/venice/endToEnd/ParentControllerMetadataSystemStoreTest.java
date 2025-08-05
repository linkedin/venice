package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_METADATA_STORE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ParentControllerMetadataSystemStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(ParentControllerMetadataSystemStoreTest.class);
  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String parentControllerMetadataSystemStoreName;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(PARENT_CONTROLLER_METADATA_STORE_CLUSTER_NAME, "venice-cluster0");
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentControllerProperties(controllerProps)
            .build());
    clusterName = venice.getClusterNames()[0];
    parentControllerMetadataSystemStoreName =
        VeniceSystemStoreUtils.getParentControllerMetadataStoreNameForCluster(clusterName);
    VeniceClusterWrapper veniceLocalCluster = venice.getChildRegions().get(0).getClusters().get(clusterName);

    controllerClient = new ControllerClient(clusterName, veniceLocalCluster.getAllControllersURLs());
    parentControllerClient = new ControllerClient(clusterName, venice.getControllerConnectString());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    IOUtils.closeQuietly(venice);
  }

  @Test
  public void testParentControllerMetadataStoreCreationOneAttempt() {
    StoreResponse storeResponse = parentControllerClient.getStore(parentControllerMetadataSystemStoreName);
    assertFalse(storeResponse.isError(), "Failed to get parent controller metadata store: " + storeResponse.getError());
    assertNotNull(storeResponse.getStore(), "Parent controller metadata store should exist");
    assertEquals(
        storeResponse.getStore().getName(),
        parentControllerMetadataSystemStoreName,
        "Parent controller metadata store name should match expected name");
    assertEquals(
        storeResponse.getCluster(),
        clusterName,
        "Parent controller metadata store should be in the expected cluster");
  }

  @Test
  public void testParentControllerMetadataSystemStoreCreationAndShowOtherStores() {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      MultiStoreInfoResponse multiStoreInfoResponse = controllerClient.getClusterStores(clusterName);
      LOGGER.info("All stores in cluster {}:", clusterName);
      for (StoreInfo storeInfo: multiStoreInfoResponse.getStoreInfoList()) {
        LOGGER.info(storeInfo.getName());
      }

      boolean foundStore = multiStoreInfoResponse.getStoreInfoList()
          .stream()
          .anyMatch(storeInfo -> storeInfo.getName().equals(parentControllerMetadataSystemStoreName));
      assertTrue(foundStore, "Parent controller metadata store should appear in cluster store list");
    });
  }
}

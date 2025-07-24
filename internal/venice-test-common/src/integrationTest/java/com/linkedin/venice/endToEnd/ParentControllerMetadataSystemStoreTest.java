package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_METADATA_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.PARENT_CONTROLLER_METADATA_STORE_ENABLED;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import java.util.Map;
import java.util.Optional;
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
  private VeniceClusterWrapper veniceLocalCluster;
  private VeniceServerWrapper veniceServerWrapper;

  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String parentControllerMetadataSystemStoreName;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(PARENT_CONTROLLER_METADATA_STORE_ENABLED, "true");
    controllerProps.put(PARENT_CONTROLLER_METADATA_STORE_CLUSTER_NAME, "venice-cluster0");
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper( // where the store initalization logic
                                                                             // acutally ahppends
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
    veniceLocalCluster = venice.getChildRegions().get(0).getClusters().get(clusterName);
    veniceServerWrapper = veniceLocalCluster.getVeniceServers().get(0);

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
  public void testParentControllerMetadataNewStoreVersionCreation() {
    // Verify the parent controller metadata store is created.
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient, true); // only works
                                                                                                        // when the
                                                                                                        // parentControllerClient
                                                                                                        // is used
    assertFalse(versionCreationResponse.isError());
  }

  @Test
  public void testParentControllerMetadataStoreCreation() {
    // Verify the parentControllerMetadataStore is created.
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      StoreResponse storeResponse = controllerClient.getStore(parentControllerMetadataSystemStoreName);
      System.out.println(storeResponse);

      MultiStoreInfoResponse multiStoreInfoResponse = controllerClient.getClusterStores(clusterName);
      for (StoreInfo storeInfo: multiStoreInfoResponse.getStoreInfoList()) {
        System.out.println(storeInfo.getName());
      }
    });
    assertTrue(false);
  }

  private static Metric getMetric(Map<String, ? extends Metric> metrics, String name) {
    Metric metric = metrics.get(name);
    assertNotNull(metric, "Metric '" + name + "' was not found!");
    return metric;
  }

  private VersionCreationResponse getNewStoreVersion(
      ControllerClient controllerClient,
      String storeName,
      boolean newStore) {
    if (newStore) {
      controllerClient.createNewStore(storeName, "test-user", "\"string\"", "\"string\"");
    }
    return parentControllerClient.requestTopicForWrites(
        storeName,
        1024,
        Version.PushType.BATCH,
        Version.guidBasedDummyPushId(),
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
  }

  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, boolean newStore) {
    return getNewStoreVersion(controllerClient, Utils.getUniqueString("test-kill"), newStore);
  }
}

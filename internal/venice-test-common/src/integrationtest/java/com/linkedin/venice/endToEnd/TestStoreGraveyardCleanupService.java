package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreGraveyardCleanupService {
  private static final int TEST_TIMEOUT = 60_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    Properties parentControllerProperties = new Properties();
    parentControllerProperties.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED, "true");
    parentControllerProperties.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES, 0);
    parentControllerProperties.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES, -1);

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.of(new VeniceProperties(parentControllerProperties)),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false);

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCleanupStoreGraveyard() {
    String parentControllerUrls =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrls)) {
      String batchStoreName = Utils.getUniqueString("testStoreGraveyardCleanupBatch");
      NewStoreResponse newStoreResponse =
          parentControllerClient.createNewStore(batchStoreName, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());

      ControllerResponse updateResponse = parentControllerClient
          .updateStore(batchStoreName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      Assert.assertFalse(updateResponse.isError());

      TrackableControllerResponse response = parentControllerClient.deleteStore(batchStoreName);
      Assert.assertFalse(response.isError());

      StoreGraveyard parentStoreGraveyard = parentControllers.get(0).getVeniceAdmin().getStoreGraveyard();
      StoreGraveyard childStoreGraveyard =
          childDatacenters.get(0).getLeaderController(CLUSTER_NAME).getVeniceAdmin().getStoreGraveyard();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        Assert.assertNull(parentStoreGraveyard.getStoreFromGraveyard(CLUSTER_NAME, batchStoreName, null));
        Assert.assertNull(childStoreGraveyard.getStoreFromGraveyard(CLUSTER_NAME, batchStoreName, null));
      });
    }
  }
}

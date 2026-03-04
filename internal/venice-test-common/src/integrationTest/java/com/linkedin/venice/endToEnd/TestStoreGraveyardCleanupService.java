package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreGraveyardCleanupService extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 60_000; // ms

  @Override
  protected int getNumberOfRegions() {
    return 1;
  }

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraParentControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED, "true");
    controllerProps.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES, 0);
    controllerProps.put(CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES, -1);
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCleanupStoreGraveyard() {
    String parentControllerUrls = parentController.getControllerUrl();
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

      StoreGraveyard parentStoreGraveyard = parentController.getVeniceAdmin().getStoreGraveyard();
      StoreGraveyard childStoreGraveyard =
          childDatacenters.get(0).getLeaderController(CLUSTER_NAME).getVeniceAdmin().getStoreGraveyard();
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, true, () -> {
        Assert.assertNull(parentStoreGraveyard.getStoreFromGraveyard(CLUSTER_NAME, batchStoreName, null));
        Assert.assertNull(childStoreGraveyard.getStoreFromGraveyard(CLUSTER_NAME, batchStoreName, null));
      });
    }
  }
}

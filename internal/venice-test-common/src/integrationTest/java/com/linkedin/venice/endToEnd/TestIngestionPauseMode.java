package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestIngestionPauseMode extends AbstractMultiRegionTest {
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testPushBlockedOnParentWhenIngestionPausedGlobally() {
    String storeName = Utils.getUniqueString("test_ingestion_pause");
    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
      assertCommand(parentClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Verify push works before pausing
      VersionCreationResponse response = assertCommand(parentClient.emptyPush(storeName, "push-1", 1000));
      assertEquals(response.getVersion(), 1);

      // Pause globally (empty regions list = all regions)
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)));

      // Parent should reject new pushes
      VersionCreationResponse blockedResponse = parentClient.emptyPush(storeName, "push-2", 1000);
      assertTrue(blockedResponse.isError(), "Push should be blocked on parent when globally paused");
      assertTrue(
          String.valueOf(blockedResponse.getError()).contains("paused"),
          "Error should mention 'paused', got: " + blockedResponse.getError());

      // Resume and verify push works again
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));
      response = assertCommand(parentClient.emptyPush(storeName, "push-3", 1000));
      assertEquals(response.getVersion(), 2);
    }
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testPushBlockedOnParentWhenPauseTargetsOnlyOtherRegions() {
    String storeName = Utils.getUniqueString("test_ingestion_pause_region_scoped");
    String dc0 = multiRegionMultiClusterWrapper.getChildRegionNames().get(0);
    String dc1 = multiRegionMultiClusterWrapper.getChildRegionNames().get(1);

    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl());
        ControllerClient dc0Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {
      assertCommand(parentClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Pause ingestion only in dc0
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)
                  .setIngestionPausedRegions(Arrays.asList(dc0))));

      // Wait for the admin channel update to propagate to all child controllers
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = assertCommand(parentClient.getStore(storeName)).getStore();
        assertEquals(parentStore.getIngestionPauseMode(), IngestionPauseMode.ALL_VERSIONS);
        assertEquals(parentStore.getIngestionPausedRegions(), Arrays.asList(dc0));

        // dc0 child: should have ALL_VERSIONS locally since it's in the paused regions list
        StoreInfo dc0Store = assertCommand(dc0Client.getStore(storeName)).getStore();
        assertEquals(dc0Store.getIngestionPauseMode(), IngestionPauseMode.ALL_VERSIONS);
        assertEquals(dc0Store.getIngestionPausedRegions(), Arrays.asList(dc0));

        // dc1 child: should have NOT_PAUSED locally since its region is NOT in the list,
        // but still retains the regions list for observability
        StoreInfo dc1Store = assertCommand(dc1Client.getStore(storeName)).getStore();
        assertEquals(dc1Store.getIngestionPauseMode(), IngestionPauseMode.NOT_PAUSED);
        assertEquals(dc1Store.getIngestionPausedRegions(), Arrays.asList(dc0));
      });

      // Push to parent should be blocked because any region is paused
      VersionCreationResponse blockedResponse = parentClient.emptyPush(storeName, "push-1", 1000);
      assertTrue(
          blockedResponse.isError(),
          "Push should be blocked on parent even when pause targets a subset of regions");
      assertTrue(
          String.valueOf(blockedResponse.getError()).contains(dc0),
          "Error should mention the paused region, got: " + blockedResponse.getError());

      // Resume and verify push works
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));
      VersionCreationResponse response = assertCommand(parentClient.emptyPush(storeName, "push-2", 1000));
      assertEquals(response.getVersion(), 1);
    }
  }
}

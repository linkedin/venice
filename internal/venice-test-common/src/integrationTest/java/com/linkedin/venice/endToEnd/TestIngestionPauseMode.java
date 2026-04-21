package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestIngestionPauseMode {
  private VeniceClusterWrapper venice;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(1)
        .replicationFactor(1)
        .sslToKafka(false)
        .sslToStorageNodes(false)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushBlockedWhenIngestionPaused() {
    String storeName = Utils.getUniqueString("test_ingestion_pause");
    try (ControllerClient controllerClient =
        new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs())) {

      // Create store
      assertCommand(controllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Verify push works before pausing
      VersionCreationResponse response = assertCommand(controllerClient.emptyPush(storeName, "push-1", 1000));
      assertEquals(response.getVersion(), 1);

      // Pause ingestion (global, all regions)
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)));

      // Verify store metadata reflects the pause
      StoreInfo storeInfo = assertCommand(controllerClient.getStore(storeName)).getStore();
      assertEquals(storeInfo.getIngestionPauseMode(), IngestionPauseMode.ALL_VERSIONS);

      // Push should be blocked
      VersionCreationResponse blockedResponse = controllerClient.emptyPush(storeName, "push-2", 1000);
      assertTrue(blockedResponse.isError(), "Push should be blocked when ingestion is paused");
      assertTrue(
          blockedResponse.getError().contains("paused"),
          "Error should mention 'paused', got: " + blockedResponse.getError());

      // Resume ingestion
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));

      // Verify store metadata reflects the resume and regions are cleared
      storeInfo = assertCommand(controllerClient.getStore(storeName)).getStore();
      assertEquals(storeInfo.getIngestionPauseMode(), IngestionPauseMode.NOT_PAUSED);

      // Push should work after resuming
      response = assertCommand(controllerClient.emptyPush(storeName, "push-3", 1000));
      assertEquals(response.getVersion(), 2);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushBlockedWithCurrentVersionPauseMode() {
    String storeName = Utils.getUniqueString("test_ingestion_pause_cv");
    try (ControllerClient controllerClient =
        new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs())) {

      assertCommand(controllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Pause with CURRENT_VERSION mode
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.CURRENT_VERSION)));

      // Push should still be blocked (any pause mode blocks pushes)
      VersionCreationResponse blockedResponse = controllerClient.emptyPush(storeName, "push-1", 1000);
      assertTrue(blockedResponse.isError(), "Push should be blocked when ingestion is paused with CURRENT_VERSION");

      // Resume and verify push works
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));
      VersionCreationResponse response = assertCommand(controllerClient.emptyPush(storeName, "push-2", 1000));
      assertEquals(response.getVersion(), 1);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushBlockedWithRegionScopedPause() {
    String storeName = Utils.getUniqueString("test_ingestion_pause_region");
    try (ControllerClient controllerClient =
        new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs())) {

      assertCommand(controllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Pause with region scope targeting this cluster's region
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)
                  .setIngestionPausedRegions(Arrays.asList(STANDALONE_REGION_NAME))));

      // Verify store metadata reflects pause (child controller applied it since its region matches)
      StoreInfo storeInfo = assertCommand(controllerClient.getStore(storeName)).getStore();
      assertEquals(storeInfo.getIngestionPauseMode(), IngestionPauseMode.ALL_VERSIONS);

      // Push should be blocked
      VersionCreationResponse blockedResponse = controllerClient.emptyPush(storeName, "push-1", 1000);
      assertTrue(blockedResponse.isError(), "Push should be blocked for region-scoped pause");
      assertTrue(
          blockedResponse.getError().contains("paused"),
          "Error should mention 'paused', got: " + blockedResponse.getError());

      // Verify pause is NOT applied when region doesn't match
      assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)
                  .setIngestionPausedRegions(Arrays.asList("prod-lor1"))));
      storeInfo = assertCommand(controllerClient.getStore(storeName)).getStore();
      // Child controller's region ("standalone") is not in the list, so it should be NOT_PAUSED locally
      assertEquals(
          storeInfo.getIngestionPauseMode(),
          IngestionPauseMode.NOT_PAUSED,
          "Pause should not apply when this region is not in the paused regions list");

      // Push should work since pause doesn't apply to this region
      VersionCreationResponse response = assertCommand(controllerClient.emptyPush(storeName, "push-2", 1000));
      assertEquals(response.getVersion(), 1);
    }
  }
}

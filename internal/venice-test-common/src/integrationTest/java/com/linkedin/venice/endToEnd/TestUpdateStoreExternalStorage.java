package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestUpdateStoreExternalStorage extends AbstractMultiRegionTest {
  /**
   * UpdateStore that sets storageMode + externalStorageReadMode without a regions filter should
   * propagate to all child regions. Both fields are store-level: externalStorageReadMode applies to
   * the store immediately; storageMode is a default that the controller copies into
   * StoreVersion.storageMode at version-creation time -- existing versions are unaffected, and the
   * next empty-push picks up the new default.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testStorageModeAndExternalStorageReadModePropagateToAllRegions() {
    String storeName = Utils.getUniqueString("test_external_storage_all");
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
      assertCommand(parentClient.emptyPush(storeName, "push-1", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          60,
          TimeUnit.SECONDS);

      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageMode(StorageMode.DUAL_WRITE)
                  .setExternalStorageReadMode(ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK)));

      // Store-level fields propagate to every region; v1 (created before the update) stays at the
      // schema-default storageMode = INTERNAL because the controller does not retroactively rewrite
      // existing versions.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (ControllerClient client: new ControllerClient[] { parentClient, dc0Client, dc1Client }) {
          StoreInfo storeInfo = assertCommand(client.getStore(storeName)).getStore();
          assertEquals(
              storeInfo.getExternalStorageReadMode(),
              ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK,
              "externalStorageReadMode should propagate via UpdateStore");
          assertEquals(
              storeInfo.getStorageMode(),
              StorageMode.DUAL_WRITE,
              "store-level storageMode default should propagate via UpdateStore");
          assertEquals(
              storeInfo.getVersion(1).get().getStorageMode(),
              StorageMode.INTERNAL,
              "Existing version 1 was created before the update and must not be retroactively rewritten");
        }
      });

      // Push a new version after the store-level update; the new version inherits the default.
      assertCommand(parentClient.emptyPush(storeName, "push-2", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentClient,
          60,
          TimeUnit.SECONDS);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (ControllerClient client: new ControllerClient[] { parentClient, dc0Client, dc1Client }) {
          StoreInfo storeInfo = assertCommand(client.getStore(storeName)).getStore();
          assertEquals(
              storeInfo.getVersion(2).get().getStorageMode(),
              StorageMode.DUAL_WRITE,
              "New version 2 should pick up the store-level storageMode default at creation time");
          assertEquals(
              storeInfo.getVersion(1).get().getStorageMode(),
              StorageMode.INTERNAL,
              "Existing version 1 stays at the value it had when created");
        }
      });
    }
  }

  /**
   * UpdateStore with a regions filter that excludes a child region must NOT take effect on that
   * region. This proves the new store-level fields ride the same regions-filter gate used by every
   * other UpdateStore field (the early-return at the top of internalUpdateStore).
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testRegionsFilterScopesStorageModeAndExternalStorageReadMode() {
    String storeName = Utils.getUniqueString("test_external_storage_region_scoped");
    String dc0Region = multiRegionMultiClusterWrapper.getChildRegionNames().get(0);
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

      // Restrict the update to dc0 only.
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageMode(StorageMode.EXTERNAL)
                  .setExternalStorageReadMode(ExternalStorageReadMode.EXTERNAL_ONLY)
                  .setRegionsFilter(dc0Region)));

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        // dc0 applies the update at the store level.
        StoreInfo dc0Store = assertCommand(dc0Client.getStore(storeName)).getStore();
        assertEquals(dc0Store.getExternalStorageReadMode(), ExternalStorageReadMode.EXTERNAL_ONLY);
        assertEquals(dc0Store.getStorageMode(), StorageMode.EXTERNAL);

        // dc1 stays at defaults because the regions filter excluded it at internalUpdateStore.
        StoreInfo dc1Store = assertCommand(dc1Client.getStore(storeName)).getStore();
        assertEquals(dc1Store.getExternalStorageReadMode(), ExternalStorageReadMode.VENICE_ONLY);
        assertEquals(dc1Store.getStorageMode(), StorageMode.INTERNAL);
      });
    }
  }
}

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
   * Region-scoped UpdateStore --> follow-up push --> per-region inheritance.
   *
   * <p>Operator updates storageMode + externalStorageReadMode targeting dc0 only. Verifies:
   * <ul>
   *   <li>dc0 applies the new store-level defaults; parent and dc1 stay at the schema defaults
   *       because their regions-filter early-return in internalUpdateStore skipped the apply
   *       entirely.
   *   <li>v1 (created before the update) is NOT retroactively rewritten in any region.
   *   <li>A subsequent push creates v2 whose storageMode is seeded from each region's own
   *       store-level default at version-creation time: dc0's v2 picks up DUAL_WRITE, while
   *       parent's and dc1's v2 stay at INTERNAL.
   * </ul>
   *
   * <p>One test exercises three layers of the PR: regions-filter gating on internalUpdateStore,
   * the persisted store-level field on StoreProperties, and AbstractStore.addVersion seeding the
   * new version from the local store-level default at creation time.
   */
  @Test(timeOut = 240 * Time.MS_PER_SECOND)
  public void testRegionScopedStorageModeUpdatePersistsToFutureVersionsInThatRegionOnly() {
    String storeName = Utils.getUniqueString("test_region_scoped_storage_mode_push");
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
      assertCommand(parentClient.emptyPush(storeName, "push-1", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          60,
          TimeUnit.SECONDS);

      // Region-scoped update: only dc0 should apply.
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageMode(StorageMode.DUAL_WRITE)
                  .setExternalStorageReadMode(ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN)
                  .setRegionsFilter(dc0Region)));

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = assertCommand(parentClient.getStore(storeName)).getStore();
        StoreInfo dc0Store = assertCommand(dc0Client.getStore(storeName)).getStore();
        StoreInfo dc1Store = assertCommand(dc1Client.getStore(storeName)).getStore();

        // dc0 applied the update at the store level.
        assertEquals(dc0Store.getStorageMode(), StorageMode.DUAL_WRITE);
        assertEquals(dc0Store.getExternalStorageReadMode(), ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN);

        // Parent and dc1 stayed at schema defaults -- the regions-filter early-return in
        // internalUpdateStore skipped them entirely.
        assertEquals(parentStore.getStorageMode(), StorageMode.INTERNAL);
        assertEquals(parentStore.getExternalStorageReadMode(), ExternalStorageReadMode.VENICE_ONLY);
        assertEquals(dc1Store.getStorageMode(), StorageMode.INTERNAL);
        assertEquals(dc1Store.getExternalStorageReadMode(), ExternalStorageReadMode.VENICE_ONLY);

        // v1 was created before the update; no region should have retroactively rewritten it.
        for (StoreInfo s: new StoreInfo[] { parentStore, dc0Store, dc1Store }) {
          assertEquals(
              s.getVersion(1).get().getStorageMode(),
              StorageMode.INTERNAL,
              "v1 must not be retroactively rewritten");
        }
      });

      // Push v2. Each region's controller seeds the new version from its own (per-region)
      // store-level storageMode default at version-creation time -- so dc0's v2 picks up
      // DUAL_WRITE while parent's and dc1's v2 stay at INTERNAL.
      assertCommand(parentClient.emptyPush(storeName, "push-2", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentClient,
          60,
          TimeUnit.SECONDS);

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = assertCommand(parentClient.getStore(storeName)).getStore();
        StoreInfo dc0Store = assertCommand(dc0Client.getStore(storeName)).getStore();
        StoreInfo dc1Store = assertCommand(dc1Client.getStore(storeName)).getStore();

        assertEquals(
            dc0Store.getVersion(2).get().getStorageMode(),
            StorageMode.DUAL_WRITE,
            "dc0's v2 should inherit dc0's store-level storageMode default at creation");
        assertEquals(
            parentStore.getVersion(2).get().getStorageMode(),
            StorageMode.INTERNAL,
            "parent's v2 should inherit parent's untouched default");
        assertEquals(
            dc1Store.getVersion(2).get().getStorageMode(),
            StorageMode.INTERNAL,
            "dc1's v2 should inherit dc1's untouched default");

        // v1 still untouched everywhere after the second push.
        for (StoreInfo s: new StoreInfo[] { parentStore, dc0Store, dc1Store }) {
          assertEquals(s.getVersion(1).get().getStorageMode(), StorageMode.INTERNAL);
        }
      });
    }
  }
}

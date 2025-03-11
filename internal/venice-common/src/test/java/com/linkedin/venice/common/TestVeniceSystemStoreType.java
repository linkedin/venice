package com.linkedin.venice.common;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceSystemStoreType {
  @Test
  public void testExtractUserStoreName() {
    String userStoreName = "user_test_store";
    String schemaStore = "venice_system_store_METADATA_SYSTEM_SCHEMA_STORE";
    String heartBeatStore = VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix();

    assertEquals(VeniceSystemStoreType.extractUserStoreName(userStoreName), userStoreName);
    assertEquals(
        VeniceSystemStoreType.extractUserStoreName(VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName)),
        userStoreName);
    assertEquals(
        VeniceSystemStoreType
            .extractUserStoreName(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName)),
        userStoreName);
    assertEquals(VeniceSystemStoreType.extractUserStoreName(schemaStore), schemaStore);
    assertEquals(VeniceSystemStoreType.extractUserStoreName(heartBeatStore), heartBeatStore);
    assertNull(VeniceSystemStoreType.extractUserStoreName(null));
  }

  @Test
  public void testEnabledSystemStoreTypes() {
    Store userStoreWithNothingEnabled = mock(Store.class);
    Store userStoreWithSystemStoresEnabled = mock(Store.class);
    doReturn(true).when(userStoreWithSystemStoresEnabled).isDaVinciPushStatusStoreEnabled();
    doReturn(true).when(userStoreWithSystemStoresEnabled).isStoreMetaSystemStoreEnabled();

    assertTrue(VeniceSystemStoreType.getEnabledSystemStoreTypes(userStoreWithNothingEnabled).isEmpty());
    List<VeniceSystemStoreType> enabledSystemStores =
        VeniceSystemStoreType.getEnabledSystemStoreTypes(userStoreWithSystemStoresEnabled);
    assertEquals(enabledSystemStores.size(), 2);
    HashSet<VeniceSystemStoreType> systemStoreSet = new HashSet<>(enabledSystemStores);
    assertTrue(systemStoreSet.contains(VeniceSystemStoreType.META_STORE));
    assertTrue(systemStoreSet.contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE));
  }

  @Test
  public void testExtractSystemStoreName() {
    String dvcpushStatusStore = "venice_system_store_davinci_push_status_store_abc";
    String metaStore = "venice_system_store_meta_store_abc";
    String userStore = "userStore";
    assertEquals(
        VeniceSystemStoreUtils.extractSystemStoreType(dvcpushStatusStore),
        VeniceSystemStoreUtils.DAVINCI_PUSH_STATUS_STORE_STR);
    assertEquals(VeniceSystemStoreUtils.extractSystemStoreType(metaStore), VeniceSystemStoreUtils.META_STORE_STR);
    assertNull(VeniceSystemStoreUtils.extractSystemStoreType(userStore));
  }

  @Test
  public void testGetSystemStoreTypeWithValidSystemStoreNames() {
    String ps3StoreName =
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(Utils.getUniqueString("test_store_ps3"));
    assertFalse(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(ps3StoreName),
        "Cache should not have store info for " + ps3StoreName);
    assertEquals(
        VeniceSystemStoreType.getSystemStoreType(ps3StoreName),
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
        "Failed to detect Push Status System Store");
    assertTrue(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(ps3StoreName),
        "Cache should have store info for " + ps3StoreName);

    String metaStoreName =
        VeniceSystemStoreType.META_STORE.getSystemStoreName(Utils.getUniqueString("test_store_meta"));
    assertFalse(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(metaStoreName),
        "Cache should not have store info for " + metaStoreName);
    assertEquals(
        VeniceSystemStoreType.getSystemStoreType(metaStoreName),
        VeniceSystemStoreType.META_STORE,
        "Failed to detect Meta Store");
    assertTrue(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(metaStoreName),
        "Cache should have store info for " + metaStoreName);

    String batchJobHeartbeatStoreName = VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE
        .getSystemStoreName(Utils.getUniqueString("test_store_batch_job_heartbeat"));
    assertFalse(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(batchJobHeartbeatStoreName),
        "Cache should not have store info for " + batchJobHeartbeatStoreName);
    assertEquals(
        VeniceSystemStoreType.getSystemStoreType(batchJobHeartbeatStoreName),
        VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE,
        "Failed to detect Batch Job Heartbeat Store");
    assertTrue(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(batchJobHeartbeatStoreName),
        "Cache should have store info for " + batchJobHeartbeatStoreName);
  }

  @Test
  public void testGetSystemStoreTypeWithInvalidOrNonSystemStoreNames() {
    String userStoreName = Utils.getUniqueString("test_store_user");
    assertFalse(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(userStoreName),
        "Cache should not have store info for " + userStoreName);
    assertNull(VeniceSystemStoreType.getSystemStoreType(userStoreName), "Expected null for non-system store");
    assertFalse(
        VeniceSystemStoreType.getStoreTypeCache().containsKey(userStoreName),
        "Cache should not have store info for " + userStoreName);

    assertNull(
        VeniceSystemStoreType.getSystemStoreType("meta_store"),
        "Should return null when store name matches prefix exactly");

    assertNull(VeniceSystemStoreType.getSystemStoreType(null), "Expected null for null store name");

    assertNull(VeniceSystemStoreType.getSystemStoreType(""), "Expected null for empty store name");

    // Ensure exact prefix matches are ignored
    assertNull(
        VeniceSystemStoreType.getSystemStoreType(VeniceSystemStoreType.META_STORE.getPrefix()),
        "Should return null for exact prefix match");

    Assert.assertNotNull(
        VeniceSystemStoreType.getSystemStoreType(VeniceSystemStoreType.META_STORE.getPrefix() + "-extra"),
        "Should detect system store when prefix is followed by extra characters");
  }
}

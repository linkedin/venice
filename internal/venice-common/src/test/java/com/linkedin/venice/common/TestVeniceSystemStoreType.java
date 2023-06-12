package com.linkedin.venice.common;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.Store;
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

    Assert.assertEquals(VeniceSystemStoreType.extractUserStoreName(userStoreName), userStoreName);
    Assert.assertEquals(
        VeniceSystemStoreType.extractUserStoreName(VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName)),
        userStoreName);
    Assert.assertEquals(
        VeniceSystemStoreType
            .extractUserStoreName(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName)),
        userStoreName);
    Assert.assertEquals(VeniceSystemStoreType.extractUserStoreName(schemaStore), schemaStore);
    Assert.assertEquals(VeniceSystemStoreType.extractUserStoreName(heartBeatStore), heartBeatStore);
    Assert.assertNull(VeniceSystemStoreType.extractUserStoreName(null));
  }

  @Test
  public void testEnabledSystemStoreTypes() {
    Store userStoreWithNothingEnabled = mock(Store.class);
    Store userStoreWithSystemStoresEnabled = mock(Store.class);
    doReturn(true).when(userStoreWithSystemStoresEnabled).isDaVinciPushStatusStoreEnabled();
    doReturn(true).when(userStoreWithSystemStoresEnabled).isStoreMetaSystemStoreEnabled();

    Assert.assertTrue(VeniceSystemStoreType.getEnabledSystemStoreTypes(userStoreWithNothingEnabled).isEmpty());
    List<VeniceSystemStoreType> enabledSystemStores =
        VeniceSystemStoreType.getEnabledSystemStoreTypes(userStoreWithSystemStoresEnabled);
    Assert.assertEquals(enabledSystemStores.size(), 2);
    HashSet<VeniceSystemStoreType> systemStoreSet = new HashSet<>(enabledSystemStores);
    Assert.assertTrue(systemStoreSet.contains(VeniceSystemStoreType.META_STORE));
    Assert.assertTrue(systemStoreSet.contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE));
  }
}

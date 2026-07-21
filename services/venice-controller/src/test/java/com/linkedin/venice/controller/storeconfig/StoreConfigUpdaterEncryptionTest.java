package com.linkedin.venice.controller.storeconfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import org.testng.annotations.Test;


public class StoreConfigUpdaterEncryptionTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";

  private Store store(boolean isSystemStore, boolean encryptionEnabled) {
    Store store = mock(Store.class);
    when(store.isSystemStore()).thenReturn(isSystemStore);
    when(store.isEncryptionEnabled()).thenReturn(encryptionEnabled);
    return store;
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testCannotDisableEncryptionOnceEnabled() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), true, false, CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testCannotEnableEncryptionOutsideEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), false, true, CLUSTER, STORE);
  }

  @Test
  public void testCanEnableEncryptionInsideEncryptionCluster() {
    // Should not throw.
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), true, true, CLUSTER, STORE);
  }

  @Test
  public void testSystemStoreExemptFromValidation() {
    // A system store must be exempt even when disabling in a non-encryption cluster.
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(true, true), false, false, CLUSTER, STORE);
  }

  @Test
  public void testDisableAllowedWhenNotCurrentlyEnabled() {
    // Setting false on a store that is already false is a no-op, not a disable of an enabled store.
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), true, false, CLUSTER, STORE);
  }
}

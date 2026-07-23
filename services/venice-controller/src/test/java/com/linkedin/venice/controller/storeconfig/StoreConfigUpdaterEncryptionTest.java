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

  @Test
  public void testMatchingEnabledValueAllowedInEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), true, true, CLUSTER, STORE);
  }

  @Test
  public void testMatchingDisabledValueAllowedOutsideEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), false, false, CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testDisabledValueRejectedInEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), true, false, CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testEnabledValueRejectedOutsideEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), false, true, CLUSTER, STORE);
  }

  @Test
  public void testSystemStoreExemptFromValidation() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(true, true), true, false, CLUSTER, STORE);
  }
}

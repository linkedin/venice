package com.linkedin.venice.controller.storeconfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import java.util.Optional;
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
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), true, Optional.of(true), CLUSTER, STORE);
  }

  @Test
  public void testMatchingDisabledValueAllowedOutsideEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), false, Optional.of(false), CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testDisabledValueRejectedInEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), true, Optional.of(false), CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testEnabledValueRejectedOutsideEncryptionCluster() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), false, Optional.of(true), CLUSTER, STORE);
  }

  @Test
  public void testOmittedValueAllowedWhenMetadataMatchesPolicy() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, true), true, Optional.empty(), CLUSTER, STORE);
  }

  @Test(expectedExceptions = VeniceHttpException.class)
  public void testOmittedValueRejectedWhenMetadataConflictsWithPolicy() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(false, false), true, Optional.empty(), CLUSTER, STORE);
  }

  @Test
  public void testSystemStoreExemptFromValidation() {
    StoreConfigUpdater.validateEncryptionEnabledUpdate(store(true, true), true, Optional.of(false), CLUSTER, STORE);
  }
}

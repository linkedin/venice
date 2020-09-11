package com.linkedin.venice.controller;

import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.TestUtils;

import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminWithoutCluster {
  @Test
  public void canFindStartedVersionInStore(){
    String storeName = TestUtils.getUniqueString("store");
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.increaseVersion("123");
    Version version = store.getVersions().get(0);
    Optional<Version> returnedVersion = VeniceHelixAdmin.getStartedVersion(store);
    Assert.assertEquals(returnedVersion.get(), version);
  }

  /**
   * This test should fail and be removed when we revert to using push ID to guarantee idempotence.
   */
  @Test
  public void findStartedVersionIgnoresPushId() {
    String pushId = TestUtils.getUniqueString("pushid");
    String storeName = TestUtils.getUniqueString("store");
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.increaseVersion(pushId);
    Version version = store.getVersions().get(0);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.increaseVersion(pushId);
    Version startedVersion = store.getVersions().get(1);
    Optional<Version> returnedVersion = VeniceHelixAdmin.getStartedVersion(store);
    Assert.assertEquals(returnedVersion.get(), startedVersion);
  }

  @Test
  public void canMergeNewHybridConfigValuesToOldStore() {
    String storeName = TestUtils.getUniqueString("storeName");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Assert.assertFalse(store.isHybrid());

    Optional<Long> rewind = Optional.of(123L);
    Optional<Long> lagOffset = Optional.of(1500L);
    Optional<Long> timeLag = Optional.of(300L);
    HybridStoreConfig hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(store, Optional.empty(), Optional.empty(), Optional.empty());
    Assert.assertNull(hybridStoreConfig, "passing empty optionals and a non-hybrid store should generate a null hybrid config");

    hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(store, rewind, lagOffset, timeLag);
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(), 300L);

    // It's okay that time lag threshold is not specified
    hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(store, rewind, lagOffset, Optional.empty());
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(), HybridStoreConfig.DEFAULT_HYBRID_TIME_LAG_THRESHOLD);
  }
}

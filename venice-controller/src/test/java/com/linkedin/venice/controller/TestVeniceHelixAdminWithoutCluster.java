package com.linkedin.venice.controller;

import com.linkedin.venice.controller.VeniceHelixAdmin;
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
}

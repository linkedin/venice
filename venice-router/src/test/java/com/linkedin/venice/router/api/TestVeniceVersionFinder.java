package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.TestUtils;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/4/16.
 */
public class TestVeniceVersionFinder {
  @Test
  public void throws404onMissingStore(){
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    doReturn(null).when(mockRepo).getStore(anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo, Optional.empty(), stats);
    try{
      versionFinder.getVersion("");
      Assert.fail("versionFinder.getVersion() on previous line should throw a RouterException");
    } catch (RouterException e) {
      Assert.assertEquals(e.code(), 400);
    }
  }

  @Test
  public void returnNonExistingVersionOnceStoreIsDisabled()
      throws RouterException {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    String storeName = "TestVeniceVersionFinder";
    int currentVersion = 10;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setCurrentVersion(currentVersion);
    // disable store, should return the number indicates that none of version is avaiable to read.
    store.setEnableReads(false);
    doReturn(store).when(mockRepo).getStore(storeName);
    StaleVersionStats stats = mock(StaleVersionStats.class);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo, Optional.empty(), stats);
    try {
      versionFinder.getVersion(storeName);
      Assert.fail("Store should be disabled and forbidden to read.");
    } catch (RouterException e) {
      Assert.assertEquals(e.status(), HttpResponseStatus.FORBIDDEN, "Store should be disabled and forbidden to read.");
    }
    // enable store, should return the correct current version.
    store.setEnableReads(true);
    Assert.assertEquals(versionFinder.getVersion(storeName), currentVersion);
  }

  @Test
  public void onlySwapsVersionWhenAllPartitionsAreOnline() throws RouterException {

    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    int secondVersion = 2;
    int thirdVersion = 3;
    int fourthVersion = 4;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.addVersion(new Version(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    doReturn(store).when(storeRepository).getStore(storeName);

    //List<Instance> readyToServeInstances = Collections.singletonList(new Instance("id1", "host", 1234));
    List<Instance> instances = new LinkedList<>();

    RoutingDataRepository routingData = mock(RoutingDataRepository.class);
    doReturn(instances).when(routingData).getReadyToServeInstances(anyString(), anyInt());
    doReturn(3).when(routingData).getNumberOfPartitions(anyString());

    StaleVersionStats stats = mock(StaleVersionStats.class);

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository, Optional.of(routingData), stats);

    // for a new store, the versionFinder returns the current version, no matter the online replicas
    Assert.assertEquals(versionFinder.getVersion(storeName), firstVersion);

    // When the current version changes, without any online replicas the versionFinder returns the old version number
    store.setCurrentVersion(secondVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName), firstVersion);

    // When we retire an old version, we update to the new version anyways
    store.setCurrentVersion(thirdVersion);
    store.updateVersionStatus(1, VersionStatus.NOT_CREATED);
    Assert.assertEquals(versionFinder.getVersion(storeName), thirdVersion);

    // Next new version with no online instances still serves old ONLINE version
    store.setCurrentVersion(fourthVersion);
    store.addVersion(new Version(storeName, thirdVersion));
    store.updateVersionStatus(thirdVersion, VersionStatus.ONLINE);
    Assert.assertEquals(versionFinder.getVersion(storeName), thirdVersion);

    // Once we have online replicas, the versionFinder reflects the new version
    instances.add(new Instance("id1", "host", 1234));
    Assert.assertEquals(versionFinder.getVersion(storeName), fourthVersion);
  }
}

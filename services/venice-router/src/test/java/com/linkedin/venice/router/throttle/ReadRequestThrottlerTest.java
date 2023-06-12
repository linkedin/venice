package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ReadRequestThrottlerTest {
  private static final double PER_STORE_ROUTER_QUOTA_BUFFER = 1.5;

  private ReadOnlyStoreRepository storeRepository;
  private ZkRoutersClusterManager zkRoutersClusterManager;
  private RoutingDataRepository routingDataRepository;
  private AggRouterHttpRequestStats stats;
  private Store store;
  private long totalQuota;
  private int routerCount;
  private ReadRequestThrottler throttler;
  private final static long maxCapacity = 400;
  private VeniceRouterConfig routerConfig;

  private long appliedQuotaBuffer = 1 + (long) PER_STORE_ROUTER_QUOTA_BUFFER;

  @BeforeMethod
  public void setUp() {
    storeRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    zkRoutersClusterManager = Mockito.mock(ZkRoutersClusterManager.class);
    routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    routerConfig = Mockito.mock(VeniceRouterConfig.class);
    totalQuota = 1000;
    routerCount = 5;
    store = TestUtils.createTestStore("testGetQuotaForStore", "test", System.currentTimeMillis());
    store.setReadQuotaInCU(totalQuota);
    store.setCurrentVersion(1);
    Mockito.doReturn(store).when(storeRepository).getStore(Mockito.eq(store.getName()));
    Mockito.doReturn(Arrays.asList(new Store[] { store })).when(storeRepository).getAllStores();
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    Mockito.doReturn(true).when(zkRoutersClusterManager).isThrottlingEnabled();
    Mockito.doReturn(true).when(zkRoutersClusterManager).isMaxCapacityProtectionEnabled();
    Mockito.doReturn(true).when(routerConfig).isPerRouterStorageNodeThrottlerEnabled();
    stats = Mockito.mock(AggRouterHttpRequestStats.class);
    throttler = new ReadRequestThrottler(
        zkRoutersClusterManager,
        storeRepository,
        routingDataRepository,
        maxCapacity,
        stats,
        0.0,
        PER_STORE_ROUTER_QUOTA_BUFFER,
        1000,
        1000,
        true);
  }

  @Test
  public void testCalculateStoreQuotaPerRouter() {
    Assert.assertEquals(
        throttler.calculateStoreQuotaPerRouter(totalQuota),
        totalQuota / routerCount * appliedQuotaBuffer);
    // Mock one router has been crushed.
    routerCount = routerCount - 1;
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    throttler.handleRouterCountChanged(routerCount);
    Assert.assertEquals(
        throttler.calculateStoreQuotaPerRouter(totalQuota),
        totalQuota / routerCount * appliedQuotaBuffer);
    // Too many router failures, the ideal quota per router exceeds the max capacity
    routerCount = routerCount / 2;
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    throttler.handleRouterCountChanged(routerCount);
    Assert.assertEquals(throttler.calculateStoreQuotaPerRouter(totalQuota), maxCapacity);
  }

  @Test
  public void testMayThrottleRead() {
    int numberOfRequests = 10;
    try {
      for (int i = 0; i < numberOfRequests; i++) {
        throttler.mayThrottleRead(
            store.getName(),
            (int) (totalQuota / routerCount / numberOfRequests) * appliedQuotaBuffer,
            "test");
      }
    } catch (QuotaExceededException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }

    try {
      throttler.mayThrottleRead(store.getName(), 10 * appliedQuotaBuffer, "test");
      Assert.fail("Usage has exceed the quota. Should get the QuotaExceededException.");
    } catch (QuotaExceededException e) {
      // expected.
    }
  }

  @Test
  public void testOnRouterCountChanged() {
    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota / (routerCount - 1)) * appliedQuotaBuffer, "test");
      Assert.fail("Usage has exceeded the quota.");
    } catch (QuotaExceededException e) {
      // expected.
    }

    // Mock router count is changed.
    Mockito.doReturn(routerCount - 1).when(zkRoutersClusterManager).getLiveRoutersCount();
    throttler.handleRouterCountChanged(routerCount - 1);
    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota / (routerCount - 1)), "test");
      Mockito.verify(stats, Mockito.atLeastOnce()).recordTotalQuota((double) totalQuota / (routerCount - 1));
      Mockito.verify(stats, Mockito.atLeastOnce())
          .recordQuota(store.getName(), (double) totalQuota / (routerCount - 1) * appliedQuotaBuffer);
    } catch (QuotaExceededException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }
    throttler.handleRouterCountChanged((int) store.getReadQuotaInCU() + 1);
    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota / ((int) store.getReadQuotaInCU() + 1)), "test");
    } catch (QuotaExceededException e) {
      Assert.fail("Usage should not exceed the quota as we have non-zero quota amount.");
    }
  }

  @Test
  public void testOnStoreQuotaChanged() {

    long newQuota = totalQuota + 200;
    try {
      throttler.mayThrottleRead(store.getName(), (double) newQuota / routerCount * appliedQuotaBuffer, "test");
      Assert.fail("Quota has not been updated.");
    } catch (QuotaExceededException e) {
      // expected
    }
    store.setReadQuotaInCU(newQuota);
    Mockito.doReturn(store).when(storeRepository).getStore(Mockito.eq(store.getName()));
    Mockito.doReturn(Arrays.asList(new Store[] { store })).when(storeRepository).getAllStores();
    Mockito.doReturn(newQuota).when(storeRepository).getTotalStoreReadQuota();

    throttler.handleStoreChanged(store);
    Mockito.verify(stats, Mockito.atLeastOnce()).recordTotalQuota((double) newQuota / routerCount);
    Mockito.verify(stats, Mockito.atLeastOnce())
        .recordQuota(store.getName(), (double) newQuota / routerCount * appliedQuotaBuffer);

    try {
      throttler.mayThrottleRead(store.getName(), (double) newQuota / routerCount, "test");
    } catch (QuotaExceededException e) {
      Assert.fail("Quota has been updated. Usage does not exceed the new quota.", e);
    }
  }

  @Test
  public void testOnStoreQuotaChangedWithMultiStores() {
    int storeCount = 3;
    long totalQuota = 600;
    long maxCapcity = 500;
    routerCount = 2;
    Store[] stores = new Store[storeCount];
    // Generate stores with 100, 200, 300 quota.
    for (int i = 0; i < storeCount; i++) {
      stores[i] =
          TestUtils.createTestStore("testOnStoreQuotaChangedWithMultiStores" + i, "test", System.currentTimeMillis());
      stores[i].setReadQuotaInCU((long) 100 * (i + 1));
      stores[i].setCurrentVersion(1);
    }
    Mockito.doReturn(Arrays.asList(stores)).when(storeRepository).getAllStores();
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    Mockito.doReturn(maxCapcity).when(routerConfig).getMaxReadCapacityCu();
    Mockito.doReturn(true).when(routerConfig).isPerRouterStorageNodeThrottlerEnabled();

    ReadRequestThrottler multiStoreThrottler =
        new ReadRequestThrottler(zkRoutersClusterManager, storeRepository, routingDataRepository, stats, routerConfig);

    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          multiStoreThrottler.getStoreReadThrottler("testOnStoreQuotaChangedWithMultiStores" + i).getQuota(),
          stores[i].getReadQuotaInCU() / routerCount);
    }

    // One of Store's quota is updated.
    int extraQuota = 400;
    totalQuota += extraQuota;
    stores[0].setReadQuotaInCU(stores[0].getReadQuotaInCU() + extraQuota);
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    multiStoreThrottler.handleStoreChanged(stores[0]);
    Mockito.verify(stats, Mockito.atLeastOnce()).recordTotalQuota((double) totalQuota / routerCount);
    Mockito.verify(stats, Mockito.atLeastOnce())
        .recordQuota(stores[0].getName(), (double) stores[0].getReadQuotaInCU() / routerCount);

    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          multiStoreThrottler.getStoreReadThrottler("testOnStoreQuotaChangedWithMultiStores" + i).getQuota(),
          stores[i].getReadQuotaInCU() / routerCount);
    }

    // One router failed, now the total quota per router exceed the max capacity.
    Mockito.doReturn(routerCount - 1).when(zkRoutersClusterManager).getLiveRoutersCount();
    multiStoreThrottler.handleRouterCountChanged(routerCount - 1);
    // max capacity is 500, but we want 1000 per router, so in order to protect router, we reduce the quota for each
    // store in proportion.
    // Ideally store quota will be [400, 200, 300] but actually we have [200, 100, 150]
    Mockito.verify(stats, Mockito.atLeastOnce()).recordTotalQuota(maxCapcity);
    Mockito.verify(stats, Mockito.atLeastOnce())
        .recordQuota(stores[0].getName(), (double) stores[0].getReadQuotaInCU() / 2);
    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          multiStoreThrottler.getStoreReadThrottler("testOnStoreQuotaChangedWithMultiStores" + i).getQuota(),
          stores[i].getReadQuotaInCU() / 2);
    }

    int reduceQuota = 250;
    totalQuota -= reduceQuota;
    stores[0].setReadQuotaInCU(stores[0].getReadQuotaInCU() - reduceQuota);
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    multiStoreThrottler.handleStoreChanged(stores[0]);
    // now we have 750 quota total, ideally store quota wil be [250,200,300], but actual quotas are 2/3 of ideal quotas.
    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          multiStoreThrottler.getStoreReadThrottler("testOnStoreQuotaChangedWithMultiStores" + i).getQuota(),
          stores[i].getReadQuotaInCU() * maxCapcity / totalQuota);
    }

    totalQuota -= reduceQuota;
    stores[2].setReadQuotaInCU(stores[2].getReadQuotaInCU() - reduceQuota);
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    multiStoreThrottler.handleStoreChanged(stores[2]);
    // now we have 500 quota which does not exceed the max capacity, store quota will be [250, 200, 50]
    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          multiStoreThrottler.getStoreReadThrottler("testOnStoreQuotaChangedWithMultiStores" + i).getQuota(),
          stores[i].getReadQuotaInCU());
    }
  }

  @Test
  public void testOnStoreCreatedAndDeleted() {
    long extraQuota = 200;
    Store newStore = TestUtils.createTestStore("testOnStoreCreatedAndDeleted", "test", System.currentTimeMillis());
    newStore.setReadQuotaInCU(extraQuota);
    newStore.setCurrentVersion(1);
    Mockito.doReturn(Arrays.asList(store, newStore)).when(storeRepository).getAllStores();
    Mockito.doReturn(totalQuota + extraQuota).when(storeRepository).getTotalStoreReadQuota();
    throttler.handleStoreChanged(newStore);
    Assert.assertEquals(
        throttler.getStoreReadThrottler("testOnStoreCreatedAndDeleted").getQuota(),
        extraQuota / routerCount * appliedQuotaBuffer);
    // Mock delete the new store.
    Mockito.doReturn(Arrays.asList(store)).when(storeRepository).getAllStores();
    Mockito.doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();
    throttler.handleStoreDeleted(newStore.getName());
    Assert.assertNull(throttler.getStoreReadThrottler("testOnStoreCreatedAndDeleted"));

    // Too many router failure, protected the router that guarantee the quota per router will not exceed the max
    // capacity.
    routerCount = 1;
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    throttler.handleRouterCountChanged(routerCount);

    Mockito.doReturn(Arrays.asList(store, newStore)).when(storeRepository).getAllStores();
    Mockito.doReturn(totalQuota + extraQuota).when(storeRepository).getTotalStoreReadQuota();
    throttler.handleStoreCreated(newStore);
    Assert.assertEquals(
        throttler.getStoreReadThrottler(store.getName()).getQuota(),
        store.getReadQuotaInCU() * maxCapacity / (totalQuota + extraQuota));
    Assert.assertEquals(
        throttler.getStoreReadThrottler("testOnStoreCreatedAndDeleted").getQuota(),
        extraQuota * maxCapacity / (totalQuota + extraQuota));

    // Delete store
    Mockito.doReturn(Arrays.asList(newStore)).when(storeRepository).getAllStores();
    Mockito.doReturn(extraQuota).when(storeRepository).getTotalStoreReadQuota();
    throttler.handleStoreDeleted(store.getName());
    // Now the total quota per router falls back under the max capacity.
    Assert.assertEquals(
        throttler.getStoreReadThrottler("testOnStoreCreatedAndDeleted").getQuota(),
        extraQuota * appliedQuotaBuffer);
  }

  @Test
  public void testOnCurrentVersionChanged() {
    int newCurrentVersion = 100;
    store.setCurrentVersion(newCurrentVersion);
    PartitionAssignment assignment = Mockito.mock(PartitionAssignment.class);
    String topicName = Version.composeKafkaTopic(store.getName(), newCurrentVersion);
    Mockito.doReturn(topicName).when(assignment).getTopic();
    Mockito.doReturn(true).when(routingDataRepository).containsKafkaTopic(Mockito.eq(topicName));
    Mockito.doReturn(assignment).when(routingDataRepository).getPartitionAssignments(Mockito.eq(topicName));
    Mockito.doReturn(1).when(assignment).getExpectedNumberOfPartitions();
    throttler.handleStoreChanged(store);

    Assert.assertEquals(throttler.getStoreReadThrottler(store.getName()).getCurrentVersion(), newCurrentVersion);
    Mockito.verify(routingDataRepository, Mockito.atLeastOnce())
        .unSubscribeRoutingDataChange(Mockito.eq(Version.composeKafkaTopic(store.getName(), 0)), Mockito.eq(throttler));
    Mockito.verify(routingDataRepository, Mockito.atLeastOnce())
        .subscribeRoutingDataChange(
            Mockito.eq(Version.composeKafkaTopic(store.getName(), newCurrentVersion)),
            Mockito.eq(throttler));

    store.setCurrentVersion(101);
    throttler.handleStoreChanged(store);
    Mockito.verify(routingDataRepository, Mockito.times(1))
        .unSubscribeRoutingDataChange(Mockito.eq(topicName), Mockito.eq(throttler));

    // Verify no call to unSubscribeRoutingDataChange on non-existing version.
    store.setCurrentVersion(Store.NON_EXISTING_VERSION);
    throttler.handleStoreChanged(store);
    Mockito.verify(routingDataRepository, Mockito.times(1))
        .unSubscribeRoutingDataChange(Mockito.eq(topicName), Mockito.eq(throttler));
  }

  @Test
  public void testOnRoutingDataChanged() {
    int version = 1;
    PartitionAssignment assignment = Mockito.mock(PartitionAssignment.class);
    Mockito.doReturn(1).when(assignment).getExpectedNumberOfPartitions();
    String topicName = Version.composeKafkaTopic(store.getName(), version);
    Mockito.doReturn(topicName).when(assignment).getTopic();
    throttler.onExternalViewChange(assignment);
    // Make sure the current version is updated. The logic of updating storage node throttlers has been tested in
    // StoreReadThrottlerTest.
    Assert.assertEquals(throttler.getStoreReadThrottler(store.getName()).getCurrentVersion(), version);
  }

  @Test
  public void testDisableThrottling() {
    // disable throttling
    Mockito.doReturn(false).when(zkRoutersClusterManager).isThrottlingEnabled();
    int numberOfRequests = 10;
    try {
      for (int i = 0; i < numberOfRequests; i++) {
        // Every time send 10 time quota usage.
        throttler.mayThrottleRead(store.getName(), (int) (totalQuota * 10), "test");
      }
    } catch (QuotaExceededException e) {
      Assert.fail("Throttling should be disabled.");
    }

    // enable throttling again.
    Mockito.doReturn(true).when(zkRoutersClusterManager).isThrottlingEnabled();

    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota * 10), "test");
      Assert.fail("Usage has exceed the quota. Should get the QuotaExceededException.");
    } catch (QuotaExceededException e) {
      // expected.
    }
  }

  @Test
  public void testDisableMaxCapacityProtection() {
    // disable router protection
    Mockito.doReturn(false).when(zkRoutersClusterManager).isMaxCapacityProtectionEnabled();
    // Too many router failure, protected the router that guarantee the quota per router will not exceed the max
    // capacity.
    routerCount = 1;
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getLiveRoutersCount();
    throttler.handleRouterClusterConfigChanged(null);

    try {
      throttler.mayThrottleRead(store.getName(), (int) totalQuota, "test");
    } catch (QuotaExceededException e) {
      Assert.fail(
          "As router protection has been disable. Current usage does not exceed the quota, should not throttle the request.");
    }

    // enable again
    Mockito.doReturn(true).when(zkRoutersClusterManager).isMaxCapacityProtectionEnabled();
    throttler.handleRouterClusterConfigChanged(null);
    try {
      throttler.mayThrottleRead(store.getName(), (int) totalQuota * appliedQuotaBuffer, "test");
      Assert.fail("As router protection has been enabled. Current usage exceeds the quota.");
    } catch (QuotaExceededException e) {
      // expected
    }
  }
}

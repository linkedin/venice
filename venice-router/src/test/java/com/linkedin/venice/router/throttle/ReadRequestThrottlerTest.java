package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.ZkRoutersClusterManager;
import com.linkedin.venice.utils.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ReadRequestThrottlerTest {
  private ReadOnlyStoreRepository storeRepository;
  private ZkRoutersClusterManager zkRoutersClusterManager;
  private Store store;
  private long totalQuota;
  private int routerCount;
  private ReadRequestThrottler throttler;

  @BeforeMethod
  public void setup() {
    storeRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    zkRoutersClusterManager = Mockito.mock(ZkRoutersClusterManager.class);
    totalQuota = 1000;
    routerCount = 5;
    store = TestUtils.createTestStore("testGetQuotaForStore", "test", System.currentTimeMillis());
    store.setReadQuotaInCU(totalQuota);
    Mockito.doReturn(store).when(storeRepository).getStore(Mockito.eq(store.getName()));
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getRoutersCount();
    throttler = new ReadRequestThrottler(zkRoutersClusterManager, storeRepository);
  }

  @Test
  public void testGetQuotaForStore() {
    Assert.assertEquals(throttler.getQuotaForStore(store.getName()), totalQuota / routerCount);
    // Mock one router has been crushed.
    routerCount = routerCount - 1;
    Mockito.doReturn(routerCount).when(zkRoutersClusterManager).getRoutersCount();
    Assert.assertEquals(throttler.getQuotaForStore(store.getName()), totalQuota / routerCount);

    // Mock the case that the store was created before releasing quota feature.
    store.setReadQuotaInCU(0);
    Assert.assertEquals(throttler.getQuotaForStore(store.getName()),
        ReadRequestThrottler.DEFAULT_STORE_READ_QUOTA / routerCount);
  }

  @Test
  public void testMayThrottleRead() {
    int numberOfRequests = 10;
    try {
      for (int i = 0; i < numberOfRequests; i++) {
        throttler.mayThrottleRead(store.getName(), (int) (totalQuota / routerCount / numberOfRequests));
      }
    } catch (QuotaExceededException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }

    try {
      throttler.mayThrottleRead(store.getName(), 10);
      Assert.fail("Usage has exceed the quota. Should get the QuotaExceededException.");
    } catch (QuotaExceededException e) {
      //expected.
    }
  }

  @Test
  public void testOnRouterCountChanged() {
    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota / (routerCount - 1)));
      Assert.fail("Usage has exceeded the quota.");
    } catch (QuotaExceededException e) {
      // expected.
    }

    // Mock router count is changed.
    Mockito.doReturn(routerCount - 1).when(zkRoutersClusterManager).getRoutersCount();
    throttler.handleRouterCountChanged(routerCount - 1);
    try {
      throttler.mayThrottleRead(store.getName(), (int) (totalQuota / (routerCount - 1)));
    } catch (QuotaExceededException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }
  }
}

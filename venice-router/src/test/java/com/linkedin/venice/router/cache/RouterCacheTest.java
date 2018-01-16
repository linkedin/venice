package com.linkedin.venice.router.cache;

import com.linkedin.venice.meta.RoutingDataRepository;
import org.mockito.Mockito;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterCacheTest {
  @Test
  public void testCache() {
    RouterCache routerCache = new RouterCache(CacheType.ON_HEAP_CACHE, CacheEviction.LRU, 200, 2,
    -1, Mockito.mock(RoutingDataRepository.class));
    String storeName1 = "test_store_2";
    String storeName2 = "test_store_1";
    byte[] testKey1 = "test_key1".getBytes();
    byte[] testKey2 = "test_key2".getBytes();
    RouterCache.CacheValue cacheValue1 = new RouterCache.CacheValue("test_value1".getBytes(), 1);
    RouterCache.CacheValue cacheValue2 = new RouterCache.CacheValue("test_value2".getBytes(), 2);

    routerCache.put(storeName1, 1, testKey1, Optional.of(cacheValue1));
    routerCache.put(storeName1, 1, testKey2, Optional.of(cacheValue2));
    Assert.assertNull(routerCache.get(storeName1, 1, testKey1), "The old record should be evicted");
    Assert.assertEquals(routerCache.get(storeName1, 1, testKey2).get(), cacheValue2);

    routerCache.put(storeName2, 1, testKey1, Optional.of(cacheValue1));
    routerCache.put(storeName2, 1, testKey2, Optional.of(cacheValue2));
    Assert.assertEquals(routerCache.get(storeName2, 1, testKey2).get(), cacheValue2);
    Assert.assertNull(routerCache.get(storeName2, 1, testKey1), "The old record should be evicted");
    Assert.assertNull(routerCache.get(storeName1, 1, testKey2), "The old record should be evicted");
  }
}

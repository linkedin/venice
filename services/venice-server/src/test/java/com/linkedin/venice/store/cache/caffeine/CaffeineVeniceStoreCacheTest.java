package com.linkedin.venice.store.cache.caffeine;

import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.davinci.store.cache.caffeine.CaffeineVeniceStoreCache;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CaffeineVeniceStoreCacheTest {
  private static String NON_PRESENT_KEY = "Skyrgamur";
  private static String PRESENT_KEY = "Stekkjarstaur";
  private static String PRESENT_VALUE = "Giljagaur";
  private static String SECOND_PRESENT_VALUE = "Stufur";

  private CaffeineVeniceStoreCache cache;

  @BeforeMethod
  public void buildCache() {
    ObjectCacheConfig config = new ObjectCacheConfig();
    config.setMaxPerPartitionCacheSize(1000l);
    config.setTtlInMilliseconds(1000l);
    cache = new CaffeineVeniceStoreCache(config, (key, executor) -> {
      return null;
    });
  }

  @AfterMethod
  public void closeCache() {
    cache.close();
  }

  @Test
  public void testCRUDOperations() {
    // read something that isn't there
    Assert.assertNull(cache.getIfPresent(NON_PRESENT_KEY));

    // write something
    cache.insert(PRESENT_KEY, PRESENT_VALUE);

    // read it back
    Assert.assertEquals(cache.getIfPresent(PRESENT_KEY), PRESENT_VALUE);

    // update it
    cache.insert(PRESENT_KEY, SECOND_PRESENT_VALUE);

    // read it back again
    Assert.assertEquals(cache.getIfPresent(PRESENT_KEY), SECOND_PRESENT_VALUE);

    // invalidate it
    cache.invalidate(PRESENT_KEY);

    // make sure it's gone
    Assert.assertNull(cache.getIfPresent(PRESENT_KEY));

    // Write more things
    cache.insert(PRESENT_KEY, PRESENT_VALUE);
    cache.insert(NON_PRESENT_KEY, SECOND_PRESENT_VALUE);

    // check the size
    Assert.assertEquals(cache.size(), 2);

    // clear them
    cache.clear();

    // Make sure it's all gone
    Assert.assertNull(cache.getIfPresent(PRESENT_KEY));
    Assert.assertNull(cache.getIfPresent(NON_PRESENT_KEY));
    Assert.assertEquals(cache.size(), 0);

    // And check the metric
    Assert.assertEquals(cache.size(), 0);
  }
}

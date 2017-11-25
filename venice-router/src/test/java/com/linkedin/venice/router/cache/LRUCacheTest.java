package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LRUCacheTest {
  private static class MeasurableObject implements Measurable {
    private final String content;

    public MeasurableObject(String content) {
      this.content = content;
    }

    public String getContent() {
      return this.content;
    }

    @Override
    public int getSize() {
      return content.length();
    }

    @Override
    public String toString() {
      return this.content;
    }

    @Override
    public int hashCode() {
      return content.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MeasurableObject that = (MeasurableObject) o;

      return content.equals(that.content);
    }
  }

  @Test
  public void testSingleCache() {
    Cache<MeasurableObject, MeasurableObject> singleCache = new LRUCache<>(200, 1);
    String keyPrefix = "test_key_";
    String valuePrefix = "test_value_";
    for (int i = 0; i < 100; ++i) {
      singleCache.put(new MeasurableObject(keyPrefix + i), new MeasurableObject(valuePrefix + i));
    }
    for (int i = 0; i < 100; ++i) {
      Optional<MeasurableObject> value = singleCache.get(new MeasurableObject(keyPrefix + i));
      if (i <= 97) {
        Assert.assertNull(value, "Old objects should be removed because of capacity");
      } else {
        Assert.assertEquals(value.get().getContent(), valuePrefix + i);
      }
    }

    // Putting too large value should throw exception
    String largeKey = "aaaaaaaaa";
    String largeValue = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    try {
      singleCache.put(new MeasurableObject(largeKey), new MeasurableObject(largeValue));
      Assert.fail("An exception should be thrown when putting record, which is larger than capacity");
    } catch (Exception e) {

    }

    // Put one more record, then the eldest one should be evicted
    String newKey = keyPrefix + 100;
    String newValue = valuePrefix + 100;
    singleCache.put(new MeasurableObject(newKey), new MeasurableObject(newValue));
    Assert.assertNull(singleCache.get(new MeasurableObject(keyPrefix + 98)), "Eldest key should be evicted");
    // With the following access pattern, 100 should the eldest key
    Assert.assertEquals(singleCache.get(new MeasurableObject(newKey)).get(), new MeasurableObject(newValue));
    Assert.assertEquals(singleCache.get(new MeasurableObject(keyPrefix + 99)).get(), new MeasurableObject(valuePrefix + 99));

    // Right now, 99 and 100 are in the cache, and we will put a 'null' inside
    // 100 is the eldest key because of the previous access pattern
    String keyWithNull = keyPrefix + 101;
    singleCache.putNullValue(new MeasurableObject(keyWithNull));
    Assert.assertNull(singleCache.get(new MeasurableObject(keyPrefix + 100)), "Eldest key should be evicted");
    Optional<MeasurableObject> valueWithNull = singleCache.get(new MeasurableObject(keyWithNull));
    Assert.assertNotNull(valueWithNull, "NULL record should receive non-null object from cache");
    Assert.assertFalse(valueWithNull.isPresent(), "NULL record should receive Optional.empty() from cache");

    // Test with non-existing key
    String nonExistingKey = keyPrefix + 10000;
    Assert.assertNull(singleCache.get(new MeasurableObject(nonExistingKey)), "Non-existing key should receive null from cache");

    // Remove key
    singleCache.remove(new MeasurableObject(keyWithNull));
    Assert.assertNull(singleCache.get(new MeasurableObject(keyWithNull)), "Removed record should not exist any more");

    singleCache.clear();
  }

  @Test
  public void testConcurrentCache() {
    Cache concurrentCache = new LRUCache(400, 2);
    String keyPrefix = "test_key_";
    String valuePrefix = "test_value_";
    for (int i = 0; i < 100; ++i) {
      concurrentCache.put(new MeasurableObject(keyPrefix + i), new MeasurableObject(valuePrefix + i));
    }

    // The records are distributed in two caches, and we should have 4 records in the cache.
    for (int i = 0; i < 100; ++i) {
      Optional<MeasurableObject> value = concurrentCache.get(new MeasurableObject(keyPrefix + i));
      if (i <= 95) {
        Assert.assertNull(value, "Old objects should be removed because of capacity");
      } else {
        Assert.assertEquals(value.get().getContent(), valuePrefix + i);
      }
    }
    concurrentCache.clear();
  }
}

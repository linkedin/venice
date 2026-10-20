package com.linkedin.venice.fastclient.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class FastClientRecordCacheTest {
  private static final int ENTRY_OVERHEAD_IN_BYTES = 64;

  private static ByteBuffer key(int i) {
    return ByteBuffer.wrap(("key-" + i).getBytes());
  }

  @Test
  public void testPutAndGet() {
    FastClientRecordCache cache = new FastClientRecordCache(1024 * 1024);
    cache.put(key(1), new byte[] { 1, 2, 3 }, 7, CompressionStrategy.NO_OP, 1);

    FastClientRecordCache.CachedValue cached = cache.get(key(1));
    assertEquals(cached.getSchemaId(), 7);
    assertEquals(cached.getValue(), ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
  }

  @Test
  public void testMissReturnsNull() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    assertNull(cache.get(key(42)));
  }

  @Test(dataProvider = "nonPositiveSizes")
  public void testNonPositiveBudgetRejected(long size) {
    assertThrows(IllegalArgumentException.class, () -> new FastClientRecordCache(size));
  }

  @org.testng.annotations.DataProvider(name = "nonPositiveSizes")
  public Object[][] nonPositiveSizes() {
    return new Object[][] { { 0L }, { -1L }, { Long.MIN_VALUE } };
  }

  @Test
  public void testNullValueNotCached() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.put(key(1), null, 1, CompressionStrategy.NO_OP, 1);
    assertNull(cache.get(key(1)));
  }

  @Test
  public void testEmptyValueCached() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.put(key(1), new byte[0], 3, CompressionStrategy.NO_OP, 1);

    FastClientRecordCache.CachedValue cached = cache.get(key(1));
    assertEquals(cached.getSchemaId(), 3);
    assertEquals(cached.getValue().remaining(), 0);
  }

  /**
   * A reader that drains the returned buffer must not affect the next reader.
   */
  @Test
  public void testRepeatedReadsAreIndependent() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.put(key(1), new byte[] { 9, 8 }, 1, CompressionStrategy.NO_OP, 1);

    ByteBuffer first = cache.get(key(1)).getValue();
    first.get();
    first.get();
    assertEquals(first.remaining(), 0);

    assertEquals(cache.get(key(1)).getValue().remaining(), 2);
  }

  @Test
  public void testEvictionAtByteBound() {
    int valueSize = 100;
    int entries = 10;
    // Budget for 3 entries; the remaining writes must evict.
    long budget = 3L * (key(0).remaining() + valueSize + ENTRY_OVERHEAD_IN_BYTES);
    FastClientRecordCache cache = new FastClientRecordCache(budget);

    for (int i = 0; i < entries; i++) {
      cache.put(key(i), new byte[valueSize], 1, CompressionStrategy.NO_OP, 1);
    }
    cache.cleanUp();

    assertTrue(cache.getEntryCount() < entries, "Expected eviction, entry count: " + cache.getEntryCount());
  }

  /**
   * A single entry larger than the whole budget must not be retained.
   */
  @Test
  public void testOversizedEntryEvicted() {
    FastClientRecordCache cache = new FastClientRecordCache(128);
    cache.put(key(1), new byte[4096], 1, CompressionStrategy.NO_OP, 1);
    cache.cleanUp();

    assertEquals(cache.getEntryCount(), 0);
  }

  @Test
  public void testClear() {
    FastClientRecordCache cache = new FastClientRecordCache(1024 * 1024);
    cache.put(key(1), new byte[] { 1 }, 1, CompressionStrategy.NO_OP, 1);
    cache.put(key(2), new byte[] { 2 }, 1, CompressionStrategy.NO_OP, 1);

    cache.clear();

    assertEquals(cache.getEntryCount(), 0);
    assertNull(cache.get(key(1)));
    assertNull(cache.get(key(2)));
  }

  @Test
  public void testClearOnEmptyCacheIsSafe() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.clear();
    assertEquals(cache.getEntryCount(), 0);
  }

  @Test
  public void testPutOverwritesExistingKey() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.put(key(1), new byte[] { 1 }, 1, CompressionStrategy.NO_OP, 1);
    cache.put(key(1), new byte[] { 2 }, 5, CompressionStrategy.NO_OP, 1);

    FastClientRecordCache.CachedValue cached = cache.get(key(1));
    assertEquals(cached.getSchemaId(), 5);
    assertEquals(cached.getValue().get(), (byte) 2);
    assertEquals(cache.getEntryCount(), 1);
  }

  @Test
  public void testCompressionStrategyAndVersionAreRetained() {
    FastClientRecordCache cache = new FastClientRecordCache(1024);
    cache.put(key(1), new byte[] { 1, 2 }, 7, CompressionStrategy.ZSTD_WITH_DICT, 42);

    FastClientRecordCache.CachedValue cached = cache.get(key(1));
    assertEquals(cached.getCompressionStrategy(), CompressionStrategy.ZSTD_WITH_DICT);
    assertEquals(cached.getVersion(), 42);
    assertEquals(cached.getSchemaId(), 7);
  }

  @Test
  public void testWeightUsesStoredBytesNotDecompressedSize() {
    // A compressed payload is weighed at its compressed length, which is the whole point of caching before
    // decompression: more keys fit in the same budget.
    FastClientRecordCache cache = new FastClientRecordCache(600);
    for (int i = 0; i < 10; i++) {
      cache.put(key(i), new byte[64], 1, CompressionStrategy.GZIP, 1);
    }
    cache.cleanUp();
    assertTrue(cache.getEntryCount() <= 5, "entries: " + cache.getEntryCount());
  }
}

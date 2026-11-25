package com.linkedin.venice.fastclient.cache;

import static com.linkedin.venice.fastclient.cache.FastClientRecordCache.ENTRY_OVERHEAD_IN_BYTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.meta.CurrentVersionType;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class FastClientRecordCacheTest {
  private static byte[] key(int i) {
    return ("key-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static FastClientRecordCache newHybridCache(long maxSizeInBytes) {
    FastClientRecordCache cache =
        new FastClientRecordCache("cache_store", maxSizeInBytes, () -> CurrentVersionType.HYBRID, () -> 0);
    cache.onMetadataRefresh();
    return cache;
  }

  @Test
  public void testHybridExpiresAfterWriteWithoutRefreshAndHitsDoNotRenew() {
    AtomicLong nanos = new AtomicLong();
    FastClientRecordCache cache =
        new FastClientRecordCache("cache_store", 1024, () -> CurrentVersionType.HYBRID, nanos::get);
    cache.onMetadataRefresh();
    cache.put(key(1), new byte[] { 1 }, 1, 1);
    nanos.set(TimeUnit.SECONDS.toNanos(119));
    assertNotNull(cache.get(key(1), 1));
    nanos.set(TimeUnit.SECONDS.toNanos(120) - 1);
    assertNotNull(cache.get(key(1), 1));
    nanos.incrementAndGet();
    assertNull(cache.get(key(1), 1));
  }

  @Test
  public void testReplacementStartsNewInsertionAge() {
    AtomicLong nanos = new AtomicLong();
    FastClientRecordCache cache =
        new FastClientRecordCache("cache_store", 1024, () -> CurrentVersionType.HYBRID, nanos::get);
    cache.onMetadataRefresh();
    cache.put(key(1), new byte[] { 1 }, 1, 1);
    nanos.set(TimeUnit.SECONDS.toNanos(119));
    cache.put(key(1), new byte[] { 2 }, 1, 1);
    nanos.set(TimeUnit.SECONDS.toNanos(120));
    assertEquals(cache.get(key(1), 1).getValue().get(), (byte) 2);
    nanos.set(TimeUnit.SECONDS.toNanos(239));
    assertNull(cache.get(key(1), 1));
  }

  @Test
  public void testRefreshConfiguresExpirationAndClearsOnTypeChanges() {
    AtomicLong nanos = new AtomicLong();
    AtomicReference<CurrentVersionType> versionType = new AtomicReference<>(CurrentVersionType.BATCH_ONLY);
    FastClientRecordCache cache = new FastClientRecordCache("cache_store", 1024, versionType::get, nanos::get);
    cache.onMetadataRefresh();
    cache.put(key(1), new byte[] { 1 }, 1, 1);
    nanos.set(TimeUnit.DAYS.toNanos(1));
    cache.onMetadataRefresh();
    assertNotNull(cache.get(key(1), 1));
    versionType.set(CurrentVersionType.HYBRID);
    cache.onMetadataRefresh();
    assertNull(cache.get(key(1), 1));
    cache.put(key(1), new byte[] { 2 }, 1, 1);
    nanos.addAndGet(TimeUnit.SECONDS.toNanos(120));
    assertNull(cache.get(key(1), 1));
    cache.put(key(1), new byte[] { 3 }, 1, 1);
    versionType.set(CurrentVersionType.BATCH_ONLY);
    cache.onMetadataRefresh();
    assertNull(cache.get(key(1), 1));
  }

  @Test
  public void testReadsAndWritesBypassUntilRefreshConfiguresCurrentType() {
    AtomicReference<CurrentVersionType> versionType = new AtomicReference<>(CurrentVersionType.UNKNOWN);
    FastClientRecordCache cache = new FastClientRecordCache("cache_store", 1024, versionType::get, () -> 0);
    cache.onMetadataRefresh();
    cache.put(key(1), new byte[] { 1 }, 1, 1);
    assertNull(cache.get(key(1), 1));
    versionType.set(CurrentVersionType.BATCH_ONLY);
    cache.put(key(1), new byte[] { 2 }, 1, 1);
    assertNull(cache.get(key(1), 1));
    cache.onMetadataRefresh();
    assertNull(cache.get(key(1), 1));
    cache.put(key(1), new byte[] { 3 }, 1, 1);
    assertNotNull(cache.get(key(1), 1));
    versionType.set(CurrentVersionType.HYBRID);
    cache.put(key(2), new byte[] { 4 }, 1, 1);
    assertNull(cache.get(key(1), 1));
    assertNull(cache.get(key(2), 1));
    cache.onMetadataRefresh();
    assertNull(cache.get(key(1), 1));
    assertNull(cache.get(key(2), 1));
    cache.put(key(1), new byte[] { 5 }, 1, 1);
    versionType.set(CurrentVersionType.UNKNOWN);
    assertNull(cache.get(key(1), 1));
    cache.onMetadataRefresh();
    versionType.set(CurrentVersionType.HYBRID);
    cache.onMetadataRefresh();
    assertNull(cache.get(key(1), 1));
  }

  @Test
  public void testPutAndGetWithIndependentBufferPositions() {
    FastClientRecordCache cache = newHybridCache(1024 * 1024);
    assertNull(cache.get(key(1), 42));
    byte[] serializedKey = key(1);
    cache.put(serializedKey, new byte[] { 1, 2, 3 }, 7, 42);
    serializedKey[0] = 0;

    FastClientRecordCache.CachedValue cached = cache.get(key(1), 42);
    assertEquals(cached.getSchemaId(), 7);
    assertEquals(cached.getValue(), ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    ByteBuffer first = cached.getValue();
    first.get(new byte[first.remaining()]);
    assertEquals(first.remaining(), 0);
    assertEquals(cache.get(key(1), 42).getValue(), ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    assertNull(cache.get(key(42), 42));
    assertNull(cache.get(key(1), 43));
  }

  @Test(dataProvider = "nonPositiveSizes")
  public void testNonPositiveBudgetRejected(long size) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new FastClientRecordCache("cache_store", size, () -> CurrentVersionType.HYBRID, () -> 0));
  }

  @DataProvider(name = "nonPositiveSizes")
  public Object[][] nonPositiveSizes() {
    return new Object[][] { { 0L }, { -1L } };
  }

  @Test
  public void testEvictionAtByteBound() {
    int valueSize = 100;
    int entries = 10;
    // Budget for 3 entries; the remaining writes must evict.
    long budget = 3L * (key(0).length + valueSize + ENTRY_OVERHEAD_IN_BYTES);
    FastClientRecordCache cache = newHybridCache(budget);

    for (int i = 0; i < entries; i++) {
      cache.put(key(i), new byte[valueSize], 1, 1);
    }
    cache.cleanUp();

    assertTrue(cache.getEntryCount() < entries, "Expected eviction, entry count: " + cache.getEntryCount());
    assertTrue(cache.getEstimatedWeightInBytes() <= budget);
  }

  /**
   * A single entry larger than the whole budget must not be retained.
   */
  @Test
  public void testAdmissionAtByteBudgetBoundary() {
    int valueSize = 100;
    long budget = key(1).length + valueSize + ENTRY_OVERHEAD_IN_BYTES;
    FastClientRecordCache cache = newHybridCache(budget);
    assertTrue(cache.canAdmit(key(1).length, valueSize));
    cache.put(key(1), new byte[valueSize], 1, 1);
    cache.cleanUp();
    FastClientRecordCache.CachedValue admitted = cache.get(key(1), 1);
    assertNotNull(admitted);
    assertEquals(cache.getEstimatedWeightInBytes(), budget);
    assertFalse(cache.canAdmit(key(1).length, valueSize + 1));
    cache.put(key(1), new byte[valueSize + 1], 2, 1);
    assertEquals(cache.get(key(1), 1), admitted, "Oversized replacement must not evict a valid entry");
    assertFalse(cache.canAdmit(Integer.MAX_VALUE, Integer.MAX_VALUE));
    FastClientRecordCache hugeCache = newHybridCache(Long.MAX_VALUE);
    assertFalse(hugeCache.canAdmit(Integer.MAX_VALUE, 1), "Caffeine weights must fit an int without undercounting");
    cache.close();
    assertFalse(cache.canAdmit(0, 0));
  }

  @Test
  public void testAdmissionDoesNotSwallowErrors() {
    FastClientRecordCache cache = new FastClientRecordCache("cache_store", 1024, () -> {
      throw new AssertionError("Metadata type lookup failed");
    }, () -> 0);
    assertThrows(AssertionError.class, () -> cache.canAdmit(1, 1));
    assertThrows(AssertionError.class, () -> cache.put(key(1), new byte[1], 1, 1));
  }

  @Test
  public void testCloseClearsBatchCacheAndDisablesLateFillsAndRefreshes() {
    FastClientRecordCache cache =
        new FastClientRecordCache("cache_store", 1024, () -> CurrentVersionType.BATCH_ONLY, () -> 0);
    cache.onMetadataRefresh();
    cache.put(key(1), new byte[] { 1 }, 1, 1);
    assertNotNull(cache.get(key(1), 1));
    cache.close();
    assertEquals(cache.getEntryCount(), 0);
    cache.onMetadataRefresh();
    cache.put(key(2), new byte[] { 2 }, 1, 1);
    assertNull(cache.get(key(1), 1));
    assertNull(cache.get(key(2), 1));
    assertEquals(cache.getEntryCount(), 0);
    cache.close();
  }

  @Test(timeOut = 10000)
  public void testCloseRacingWithEligiblePutDoesNotRetainEntry() throws Exception {
    CountDownLatch checkingType = new CountDownLatch(1);
    CountDownLatch resumePut = new CountDownLatch(1);
    AtomicBoolean blockTypeRead = new AtomicBoolean();
    FastClientRecordCache cache = new FastClientRecordCache("cache_store", 1024, () -> {
      if (blockTypeRead.get()) {
        checkingType.countDown();
        try {
          assertTrue(resumePut.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(e);
        }
      }
      return CurrentVersionType.BATCH_ONLY;
    }, () -> 0);
    cache.onMetadataRefresh();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      blockTypeRead.set(true);
      Future<?> put = executor.submit(() -> cache.put(key(1), new byte[] { 1 }, 1, 1));
      assertTrue(checkingType.await(5, TimeUnit.SECONDS));
      cache.close();
      resumePut.countDown();
      put.get(5, TimeUnit.SECONDS);
      assertEquals(cache.getEntryCount(), 0);
      assertNull(cache.get(key(1), 1));
    } finally {
      resumePut.countDown();
      executor.shutdownNow();
      cache.close();
    }
  }

  @DataProvider
  public Object[][] replacementValues() {
    return new Object[][] { { new byte[0] }, { new byte[] { 3, 4, 5 } } };
  }

  @Test(dataProvider = "replacementValues")
  public void testReplacementAndExactInvalidationAreVersionScoped(byte[] replacement) {
    FastClientRecordCache cache = newHybridCache(1024);
    cache.put(key(1), new byte[] { 1, 2 }, 7, 42);
    FastClientRecordCache.CachedValue old = cache.get(key(1), 42);
    cache.put(key(1), replacement, 8, 42);
    cache.invalidate(key(1), 42, old);

    FastClientRecordCache.CachedValue cached = cache.get(key(1), 42);
    assertNotNull(cached);
    assertEquals(cached.getValue(), ByteBuffer.wrap(replacement));
    assertEquals(cached.size(), replacement.length);
    assertEquals(cached.getSchemaId(), 8);
    assertEquals(cache.getEntryCount(), 1);
    cache.put(key(1), new byte[] { 9 }, 9, 43);
    assertEquals(cache.getEntryCount(), 2);
    cache.invalidate(key(1), 43, cached);
    assertNotNull(cache.get(key(1), 43));
    cache.invalidate(key(1), 42, cached);
    assertNull(cache.get(key(1), 42));
    cache.put(key(1), new byte[] { 1, 2 }, 7, 42);
    assertEquals(cache.get(key(1), 43).getValue().get(), (byte) 9, "Late old-version fill must not replace v43");
  }

  @Test
  public void testSharedCacheGaugesFollowFillReplacementAndClear() {
    MetricsRepository repository =
        new MetricsRepository(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig());
    try {
      FastClientRecordCache cache = newHybridCache(1024);
      for (RequestType requestType: RequestType.values()) {
        FastClientStats stats =
            FastClientStats.getClientStats(repository, "client", "cache_store", requestType, false, false);
        if (requestType == RequestType.SINGLE_GET) {
          stats.registerCacheGauges(cache);
          stats.registerCacheGauges(cache);
        } else {
          assertThrows(IllegalStateException.class, () -> stats.registerCacheGauges(cache));
        }
      }
      String prefix = ".client_cache_store--cache_";
      assertEquals(
          repository.metrics()
              .keySet()
              .stream()
              .filter(
                  name -> name.endsWith(".Gauge")
                      && (name.contains("cache_estimated_") || name.contains("cache_configured_budget")))
              .count(),
          2L);
      assertEquals(repository.getMetric(prefix + "estimated_entry_count.Gauge").value(), 0.0);
      assertEquals(repository.getMetric(prefix + "estimated_weight_in_bytes.Gauge").value(), 0.0);
      assertNull(repository.getMetric(prefix + "configured_budget_in_bytes.Gauge"));

      cache.put(key(1), new byte[10], 1, 1);
      cache.put(key(2), new byte[20], 1, 1);
      cache.cleanUp();
      assertEquals(repository.getMetric(prefix + "estimated_entry_count.Gauge").value(), 2.0);
      assertEquals(
          repository.getMetric(prefix + "estimated_weight_in_bytes.Gauge").value(),
          (double) (2 * (key(1).length + ENTRY_OVERHEAD_IN_BYTES) + 30));

      cache.put(key(1), new byte[0], 1, 1);
      cache.cleanUp();
      assertEquals(repository.getMetric(prefix + "estimated_entry_count.Gauge").value(), 2.0);
      assertEquals(
          repository.getMetric(prefix + "estimated_weight_in_bytes.Gauge").value(),
          (double) (2 * (key(1).length + ENTRY_OVERHEAD_IN_BYTES) + 20));

      cache.clear();
      cache.cleanUp();
      assertEquals(repository.getMetric(prefix + "estimated_entry_count.Gauge").value(), 0.0);
      assertEquals(repository.getMetric(prefix + "estimated_weight_in_bytes.Gauge").value(), 0.0);
    } finally {
      repository.close();
    }
  }

  @Test
  public void testBypassLogsAreThrottledByReasonAndOrdinaryMissesAreSilent() {
    Logger logger = (Logger) LogManager.getLogger(FastClientRecordCache.class);
    Level previousLevel = logger.getLevel();
    InMemoryLogAppender appender = new InMemoryLogAppender.Builder().build();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      AtomicReference<CurrentVersionType> type = new AtomicReference<>(CurrentVersionType.BATCH_ONLY);
      AtomicReference<RuntimeException> eligibilityFailure = new AtomicReference<>();
      AtomicLong nanos = new AtomicLong();
      FastClientRecordCache cache = new FastClientRecordCache("bypass_logging_store", 1024, () -> {
        if (eligibilityFailure.get() != null) {
          throw eligibilityFailure.get();
        }
        return type.get();
      }, nanos::get);
      for (int i = 0; i < 2; i++) {
        assertNull(cache.get(key(i), 1));
      }
      type.set(CurrentVersionType.UNKNOWN);
      for (int i = 0; i < 2; i++) {
        assertNull(cache.get(key(i), 1));
      }
      type.set(CurrentVersionType.BATCH_ONLY);
      cache.onMetadataRefresh();
      type.set(CurrentVersionType.HYBRID);
      for (int i = 0; i < 2; i++) {
        assertNull(cache.get(key(i), 1));
      }
      cache.onMetadataRefresh();
      assertNull(cache.get(key(0), 1));
      cache.put(key(0), new byte[0], 1, 1);
      nanos.set(TimeUnit.SECONDS.toNanos(120));
      assertNull(cache.get(key(0), 1));

      assertEquals(appender.getLogs().size(), 3);
      assertTrue(appender.getLogs().get(0).contains("not_initialized"));
      assertTrue(appender.getLogs().get(1).contains("unknown_version_type"));
      assertTrue(appender.getLogs().get(2).contains("version_type_mismatch; configured BATCH_ONLY, current HYBRID"));
      eligibilityFailure.set(new IllegalStateException("Metadata type unavailable"));
      for (int i = 0; i < 2; i++) {
        assertFalse(cache.canAdmit(key(i).length, 1));
        cache.put(key(i), new byte[1], 1, 1);
      }
      assertEquals(appender.getLogs().size(), 4);
      assertTrue(appender.getLogs().get(3).contains("Record cache admission failed"));
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }
}

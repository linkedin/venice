package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider.LatencyType;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class LatencyPercentileProviderTest {
  @Test
  public void testInvalidCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new LatencyPercentileProvider(0));
    assertThrows(IllegalArgumentException.class, () -> new LatencyPercentileProvider(-1));
  }

  @Test
  public void testEmptyReservoirReturnsZero() {
    LatencyPercentileProvider provider = new LatencyPercentileProvider();
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 0.0);
    assertEquals(provider.getP99(LatencyType.MULTI_GET), 0.0);
    assertEquals(provider.getP99(LatencyType.READ_COMPUTE), 0.0);
  }

  @Test
  public void testP99WithUniformSamples() {
    LatencyPercentileProvider provider = new LatencyPercentileProvider(128);
    for (int i = 0; i < 128; i++) {
      provider.observe(LatencyType.SINGLE_GET, 42.0);
    }
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 42.0);
  }

  @Test
  public void testP99OrdersHighValuesIntoTail() {
    LatencyPercentileProvider provider = new LatencyPercentileProvider(100);
    for (int i = 1; i <= 100; i++) {
      provider.observe(LatencyType.SINGLE_GET, (double) i);
    }
    // With 100 samples, p99 index = round(0.99 * 99) = round(98.01) = 98 -> samples[98] = 99.0
    double p99 = provider.getP99(LatencyType.SINGLE_GET);
    assertTrue(p99 >= 98.0 && p99 <= 100.0, "Unexpected p99: " + p99);
  }

  @Test
  public void testTypesAreIndependent() {
    LatencyPercentileProvider provider = new LatencyPercentileProvider(32);
    for (int i = 0; i < 32; i++) {
      provider.observe(LatencyType.SINGLE_GET, 10.0);
    }
    // MULTI_GET and READ_COMPUTE remain empty -> 0.
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 10.0);
    assertEquals(provider.getP99(LatencyType.MULTI_GET), 0.0);
    assertEquals(provider.getP99(LatencyType.READ_COMPUTE), 0.0);
  }

  @Test
  public void testOverwriteWrapsAroundRing() {
    LatencyPercentileProvider provider = new LatencyPercentileProvider(4);
    // Fill with 1,2,3,4; overwrite with 100,100,100,100.
    provider.observe(LatencyType.SINGLE_GET, 1.0);
    provider.observe(LatencyType.SINGLE_GET, 2.0);
    provider.observe(LatencyType.SINGLE_GET, 3.0);
    provider.observe(LatencyType.SINGLE_GET, 4.0);
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 4.0);

    provider.observe(LatencyType.SINGLE_GET, 100.0);
    provider.observe(LatencyType.SINGLE_GET, 100.0);
    provider.observe(LatencyType.SINGLE_GET, 100.0);
    provider.observe(LatencyType.SINGLE_GET, 100.0);
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 100.0);
  }

  @Test
  public void testConcurrentObserve() throws Exception {
    LatencyPercentileProvider provider = new LatencyPercentileProvider(1024);
    int threads = 8;
    int perThread = 5_000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < perThread; i++) {
              provider.observe(LatencyType.SINGLE_GET, 50.0);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertTrue(done.await(30, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
    assertEquals(provider.getP99(LatencyType.SINGLE_GET), 50.0);
  }

  /*
   * Parity test: verify LatencyPercentileProvider.getP99() matches the canonical sorted-array
   * percentile algorithm. Tehuti's windowed Percentiles stat uses a different histogram approach
   * (exponential decay reservoir) so exact numerical parity with Tehuti is not guaranteed —
   * instead we validate against the sort-and-index definition that the ring-buffer promises.
   * Both are valid approximations of the true p99; the test confirms the ring-buffer's algorithm
   * is correctly implemented.
   */
  @Test
  public void testP99MatchesCanonicalSortedArrayPercentile() {
    int n = 200;
    LatencyPercentileProvider provider = new LatencyPercentileProvider(n);
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = i + 1.0; // 1.0, 2.0, ..., 200.0
      provider.observe(LatencyType.SINGLE_GET, values[i]);
    }

    // Canonical p99: sort the array and pick the index at round(0.99 * (n-1)).
    double[] sorted = Arrays.copyOf(values, n);
    Arrays.sort(sorted);
    int idx = (int) Math.min(n - 1L, Math.max(0L, Math.round(0.99 * (n - 1))));
    double canonicalP99 = sorted[idx];

    assertEquals(provider.getP99(LatencyType.SINGLE_GET), canonicalP99, 1e-9);
  }

  @Test
  public void testP99WithSkewedDistributionMatchesCanonical() {
    // Skewed: 95% of values are low latency (1ms), 5% are high latency (500ms).
    int total = 1000;
    int highLatencyCount = 50; // 5%
    LatencyPercentileProvider provider = new LatencyPercentileProvider(total);
    double[] values = new double[total];
    for (int i = 0; i < total - highLatencyCount; i++) {
      values[i] = 1.0;
      provider.observe(LatencyType.MULTI_GET, 1.0);
    }
    for (int i = total - highLatencyCount; i < total; i++) {
      values[i] = 500.0;
      provider.observe(LatencyType.MULTI_GET, 500.0);
    }

    double[] sorted = Arrays.copyOf(values, total);
    Arrays.sort(sorted);
    int idx = (int) Math.min(total - 1L, Math.max(0L, Math.round(0.99 * (total - 1))));
    double canonicalP99 = sorted[idx];

    assertEquals(provider.getP99(LatencyType.MULTI_GET), canonicalP99, 1e-9);
  }
}

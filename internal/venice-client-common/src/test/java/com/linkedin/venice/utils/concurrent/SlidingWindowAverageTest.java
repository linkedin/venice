package com.linkedin.venice.utils.concurrent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestMockTime;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.utils.SystemTime;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class SlidingWindowAverageTest {
  @Test
  public void testInvalidArgs() {
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindowAverage(0));
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindowAverage(-1));
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindowAverage(1000, null));
  }

  @Test
  public void testBasicAverage() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(10_000, time);

    assertTrue(Double.isNaN(avg.average()));
    avg.record(50);
    avg.record(100);
    avg.record(75);
    assertEquals(avg.average(), 75.0, 1e-9);
  }

  @Test
  public void testAverageCarriesAcrossOneBucketRotation() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(10_000, time);
    avg.record(10);
    avg.record(20);
    time.addMilliseconds(6_000);
    avg.record(30);
    // All three observations still visible across both buckets.
    assertEquals(avg.average(), 20.0, 1e-9);
  }

  @Test
  public void testWholeWindowExpiry() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(3_000, time);
    avg.record(100);
    time.addMilliseconds(10_000);
    assertTrue(Double.isNaN(avg.average()));
  }

  @Test
  public void testCacheServesStaleDuringTtl() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(10_000, time, SlidingWindowAverage.CACHE_MS);
    avg.record(100);
    assertEquals(avg.average(), 100.0, 1e-9); // primes cache

    avg.record(300); // new record; cache still live
    time.addMilliseconds(50); // within CACHE_MS
    assertEquals(avg.average(), 100.0, 1e-9, "should return cached value within TTL");

    time.addMilliseconds(60); // past CACHE_MS
    assertEquals(avg.average(), 200.0, 1e-9, "should recompute after TTL expires");
  }

  @Test
  public void testCacheRefreshesAfterWindowExpiry() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(10_000, time);
    avg.record(50);
    assertEquals(avg.average(), 50.0, 1e-9); // prime cache
    time.addMilliseconds(30_000); // both buckets expire + TTL expired
    assertTrue(Double.isNaN(avg.average()));
  }

  /** Parity: SlidingWindowAverage.average() matches Tehuti Avg for the same values. */
  @Test
  public void testAverageMatchesTehutiAvg() {
    TestMockTime time = new TestMockTime(0);
    SlidingWindowAverage avg = new SlidingWindowAverage(30_000, time);

    MetricConfig config = new MetricConfig().timeWindow(30, TimeUnit.SECONDS);
    MetricsRepository repo = new MetricsRepository(config);
    Avg tehutiAvg = new Avg();
    Sensor sensor = repo.sensor("avg_parity");
    sensor.add("avg_parity", tehutiAvg);

    double[] values = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    for (double v: values) {
      avg.record(v);
      sensor.record(v);
    }

    double expected = 55.0;
    assertEquals(avg.average(), expected, 1e-9);
    assertEquals(tehutiAvg.measure(config, new SystemTime().milliseconds()), expected, 1e-9);
  }
}

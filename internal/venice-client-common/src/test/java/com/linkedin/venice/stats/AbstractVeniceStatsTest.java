package com.linkedin.venice.stats;

import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AbstractVeniceStatsTest {
  static class StatsTestImpl extends AbstractVeniceStats {
    public StatsTestImpl(MetricsRepository metricsRepository, String name) {
      super(metricsRepository, name);
    }
  }

  /**
   * This test creates the same metric via many objects using multiple threads.
   * Without the synchronization in {@link AbstractVeniceStats#registerSensor(String, Optional, MetricConfig, Sensor[], MeasurableStat...)}
   * this test fails consistently. With the synchronization added the test passes.
   * @throws InterruptedException
   */
  @Test
  public void testNoDuplicateMultiThreaded() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    MetricsRepository repository = new MetricsRepository();
    AtomicBoolean exceptionReceived = new AtomicBoolean();
    for (int i = 0; i < 100; i++) {
      StatsTestImpl statsTest = new StatsTestImpl(repository, "testStatsContainer");
      for (int j = 0; j < 16; j++) {
        executorService.submit(() -> {
          try {
            statsTest.registerSensor("testGauge", new Gauge(() -> 1));
          } catch (Exception e) {
            exceptionReceived.set(true);
          }
        });
      }
    }
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    Assert.assertFalse(exceptionReceived.get(), "Exception received while registering metrics");
    Assert.assertEquals(repository.metrics().size(), 1, "More than one metric was registered");
  }

  @Test
  public void testRegisterSensor() {
    MetricsRepository metricsRepository = new MetricsRepository();
    AbstractVeniceStats stats = new AbstractVeniceStats(metricsRepository, "myMetric");
    stats.registerSensor("foo", new Gauge(() -> 1.0));
    Assert.assertEquals(metricsRepository.metrics().size(), 1);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--foo.Gauge").value(), 1.0);

    Sensor percentileSensor = stats.registerSensor("bar", TehutiUtils.getPercentileStat(".myMetric--bar"));
    Assert.assertEquals(metricsRepository.metrics().size(), 4);
    Metric percentileMetric = metricsRepository.getMetric(".myMetric--bar.50thPercentile");
    Assert.assertNotNull(percentileMetric);
    Assert.assertEquals(percentileMetric.value(), Double.NaN);
    percentileSensor.record(10.0);
    Assert.assertEquals(percentileMetric.value(), 10.0, 0.1);

    stats.registerSensor("baz", new LongAdderRateGauge());
    Assert.assertEquals(metricsRepository.metrics().size(), 5);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--baz.Rate").value(), 0.0);
  }

  @Test
  public void testRegisterSensorAttributeGauge() {
    MetricsRepository metricsRepository = new MetricsRepository();
    AbstractVeniceStats stats = new AbstractVeniceStats(metricsRepository, "myMetric");
    stats.registerSensorAttributeGauge("foo", "bar", new Gauge(() -> 1.0));
    stats.registerSensorAttributeGauge("foo", "bar2", new Gauge(() -> 2.0));
    // Duplicate registration will not count.
    stats.registerSensorAttributeGauge("foo", "bar2", new Gauge(() -> 3.0));
    Assert.assertEquals(metricsRepository.metrics().size(), 2);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--foo.bar").value(), 1.0);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--foo.bar2").value(), 2.0);
  }
}

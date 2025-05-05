package com.linkedin.venice.stats;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.stats.BasicClientStats;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.SystemTime;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.Mockito;
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
   * Without the synchronization in {@link AbstractVeniceStats#registerSensor(String, Sensor[], MeasurableStat...)}
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
            statsTest.registerSensor(new AsyncGauge((ignored, ignored2) -> 1, "testGauge"));
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
    stats.registerSensor(new AsyncGauge((ignored, ignored2) -> 1.0, "foo"));
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
    stats.registerSensorAttributeGauge("foo", "bar", new AsyncGauge((ignored, ignored2) -> 1.0, "foo"));
    stats.registerSensorAttributeGauge("foo", "bar2", new AsyncGauge((ignored, ignored2) -> 2.0, "foo"));
    // Duplicate registration will not count.
    stats.registerSensorAttributeGauge("foo", "bar2", new AsyncGauge((ignored, ignored2) -> 3.0, "foo"));
    Assert.assertEquals(metricsRepository.metrics().size(), 2);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--foo.bar").value(), 1.0);
    Assert.assertEquals(metricsRepository.getMetric(".myMetric--foo.bar2").value(), 2.0);
  }

  @Test
  public void testMetricPrefix() {
    String storeName = "test_store";
    MetricsRepository metricsRepository1 = new MetricsRepository();
    // Without prefix
    ClientConfig config1 = new ClientConfig(storeName);
    BasicClientStats.getClientStats(metricsRepository1, storeName, RequestType.SINGLE_GET, config1);
    // Check metric name
    assertTrue(metricsRepository1.metrics().size() > 0);

    String prefix = "venice_system_store_meta_store_abc";
    ClientConfig config2 = new ClientConfig(storeName).setStatsPrefix(prefix);
    ClientStats clientStats =
        ClientStats.getClientStats(new MetricsRepository(), storeName, RequestType.SINGLE_GET, config2);
    clientStats.recordRequestRetryCount();
  }

  @Test
  public void testParentStats() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MetricsReporter reporter = mock(MetricsReporter.class);
    metricsRepository.addReporter(reporter);
    AbstractVeniceStats avs = new AbstractVeniceStats(metricsRepository, "AVS");
    Count parentCount = new Count(), childCount1 = new Count(), childCount2 = new Count();
    OccurrenceRate parentOccurrenceRate = new OccurrenceRate(), childOccurrenceRate1 = new OccurrenceRate(),
        childOccurrenceRate2 = new OccurrenceRate();
    long now = System.currentTimeMillis();
    MetricConfig metricConfig = new MetricConfig();

    // Test initial state
    assertEquals(childCount1.measure(metricConfig, now), 0.0);
    assertEquals(childCount2.measure(metricConfig, now), 0.0);
    assertEquals(parentCount.measure(metricConfig, now), 0.0);
    assertEquals(childOccurrenceRate1.measure(metricConfig, now), 0.0);
    assertEquals(childOccurrenceRate2.measure(metricConfig, now), 0.0);
    assertEquals(parentOccurrenceRate.measure(metricConfig, now), 0.0);
    Mockito.verify(reporter).init(argThat(argument -> argument.size() == 0));
    Mockito.verify(reporter, never()).addMetric(any());

    // Register metrics
    Sensor parentSensor = avs.registerSensor("parent", parentCount, parentOccurrenceRate);
    Sensor[] parentSensorArray = new Sensor[] { parentSensor };
    Sensor childSensor1 = avs.registerSensor("child1", parentSensorArray, childCount1, childOccurrenceRate1);
    Sensor childSensor2 = avs.registerSensor("child2", parentSensorArray, childCount2, childOccurrenceRate2);

    // Test reporter
    Mockito.verify(reporter).init(argThat(argument -> argument.size() == 0));
    Mockito.verify(reporter, times(6)).addMetric(any());

    // Test that recording propagates from child to parent
    childSensor1.record(1);
    assertEquals(childCount1.measure(metricConfig, now), 1.0);
    assertEquals(childCount2.measure(metricConfig, now), 0.0);
    assertEquals(parentCount.measure(metricConfig, now), 1.0);
    assertTrue(childOccurrenceRate1.measure(metricConfig, now) > 0.0);
    assertEquals(childOccurrenceRate2.measure(metricConfig, now), 0.0);
    assertTrue(parentOccurrenceRate.measure(metricConfig, now) > 0.0);

    childSensor2.record(1);
    assertEquals(childCount1.measure(metricConfig, now), 1.0);
    assertEquals(childCount2.measure(metricConfig, now), 1.0);
    assertEquals(parentCount.measure(metricConfig, now), 2.0);
    assertTrue(childOccurrenceRate1.measure(metricConfig, now) > 0.0);
    assertTrue(childOccurrenceRate2.measure(metricConfig, now) > 0.0);
    assertTrue(parentOccurrenceRate.measure(metricConfig, now) > 0.0);

    // Test that recording does not propagate from parent to child
    parentSensor.record(1);
    assertEquals(childCount1.measure(metricConfig, now), 1.0);
    assertEquals(childCount2.measure(metricConfig, now), 1.0);
    assertEquals(parentCount.measure(metricConfig, now), 3.0);
    assertTrue(childOccurrenceRate1.measure(metricConfig, now) > 0.0);
    assertTrue(childOccurrenceRate2.measure(metricConfig, now) > 0.0);
    assertTrue(parentOccurrenceRate.measure(metricConfig, now) > 0.0);
  }

  @Test
  public void testRegisterPerStoreAndTotalSensor() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MetricConfig metricConfig = new MetricConfig();

    AbstractVeniceStats stats = new AbstractVeniceStats(metricsRepository, "testStore");
    AbstractVeniceStats totalStats = new AbstractVeniceStats(metricsRepository, "total");
    Gauge parentCount = new Gauge(), childCount1 = new Gauge(), childCount2 = new Gauge();
    Sensor parent = totalStats.registerSensor("parent", parentCount);
    // 1) total stats is not null, the parent is the total stats
    // Being a parent means, when the sensor is recorded, the parent sensor will also be recorded
    Sensor sensor = stats.registerPerStoreAndTotalSensor("testSensor1", totalStats, () -> parent, childCount1);
    sensor.record(10.0);
    long now = System.currentTimeMillis();

    // verify both store-level sensor and total-stat sensor are recorded
    double total_value = parentCount.measure(metricConfig, now);
    double value = childCount1.measure(metricConfig, now);
    Assert.assertEquals(total_value, 10.0);
    Assert.assertEquals(value, 10.0);

    // 2) total stats is null, the parent is also null
    Sensor another = stats.registerPerStoreAndTotalSensor("testSensor2", null, () -> null, childCount2);
    // let another sensor records a different value to verify the sensor is recorded and total stats is not recorded
    another.record(20.0);
    now = System.currentTimeMillis();
    total_value = parentCount.measure(metricConfig, now);
    value = childCount1.measure(metricConfig, now);
    double value2 = childCount2.measure(metricConfig, now);
    Assert.assertEquals(total_value, 10.0);
    Assert.assertEquals(value, 10.0);
    Assert.assertEquals(value2, 20.0);
  }

  @Test
  public void testRegisterOnlyTotalRate() {
    MetricsRepository metricsRepository = new MetricsRepository();

    AbstractVeniceStats stats = new AbstractVeniceStats(metricsRepository, "testStore");
    AbstractVeniceStats totalStats = new AbstractVeniceStats(metricsRepository, "total");
    LongAdderRateGauge parentCount = new LongAdderRateGauge();
    // 1) total stats is not null so use ths supplier
    LongAdderRateGauge sensor =
        stats.registerOnlyTotalRate("testSensor", totalStats, () -> parentCount, SystemTime.INSTANCE);
    Assert.assertEquals(sensor, parentCount);

    // 2) total stats is null, so created a new one
    sensor = stats.registerOnlyTotalRate("testSensor", null, () -> parentCount, SystemTime.INSTANCE);
    Assert.assertNotEquals(sensor, parentCount);
  }

  @Test
  public void testRegisterOnlyTotalSensor() {
    MetricsRepository metricsRepository = new MetricsRepository();

    AbstractVeniceStats stats = new AbstractVeniceStats(metricsRepository, "testStore");
    AbstractVeniceStats totalStats = new AbstractVeniceStats(metricsRepository, "total");
    Sensor totalSensor = totalStats.registerSensor("testSensor", new OccurrenceRate());
    // 1) total stats is not null so use ths supplier
    Sensor sensor = stats.registerOnlyTotalSensor("testSensor", totalStats, () -> totalSensor, new OccurrenceRate());
    assertEquals(sensor, totalSensor);

    // 2) total stats is null, so created a new one
    Sensor newTotalSensor = stats.registerOnlyTotalSensor("testSensor", null, () -> totalSensor, new OccurrenceRate());
    assertNotEquals(newTotalSensor, totalSensor);
  }
}

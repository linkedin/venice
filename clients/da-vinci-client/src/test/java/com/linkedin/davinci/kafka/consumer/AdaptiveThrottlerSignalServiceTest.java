package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.AdaptiveThrottlerSignalService.MULTI_GET_LATENCY_P99_METRIC_NAME;
import static com.linkedin.davinci.kafka.consumer.AdaptiveThrottlerSignalService.READ_COMPUTE_LATENCY_P99_METRIC_NAME;
import static com.linkedin.davinci.kafka.consumer.AdaptiveThrottlerSignalService.SINGLE_GET_LATENCY_P99_METRIC_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.ingestion.heartbeat.AggregatedHeartbeatLagEntry;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider.LatencyType;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdaptiveThrottlerSignalServiceTest {
  private static VeniceServerConfig baseConfig() {
    VeniceServerConfig cfg = mock(VeniceServerConfig.class);
    when(cfg.getAdaptiveThrottlerSingleGetLatencyThreshold()).thenReturn(10d);
    when(cfg.getAdaptiveThrottlerMultiGetLatencyThreshold()).thenReturn(100d);
    when(cfg.getAdaptiveThrottlerReadComputeLatencyThreshold()).thenReturn(150d);
    return cfg;
  }

  /** Mock MetricsRepository for the Tehuti (false) path — allows stubbing getMetric(). */
  private static MetricsRepository mockMetricsRepository() {
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    Sensor sensor = mock(Sensor.class);
    doReturn(sensor).when(metricsRepository).sensor(anyString(), any());
    return metricsRepository;
  }

  /** Real VeniceMetricsRepository with USE_SELF_CONTAINED_STATS=true for the independent path. */
  private static VeniceMetricsRepository selfContainedMetricsRepo() {
    return new VeniceMetricsRepository(new VeniceMetricsConfig.Builder().setUseSelfContainedStats(true).build());
  }

  // ---------- Tehuti read path (flag = false, default) ----------

  @Test
  public void testUpdateSignalTehutiPath() {
    MetricsRepository metricsRepository = mockMetricsRepository();
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    VeniceServerConfig veniceServerConfig = baseConfig();

    AdaptiveThrottlerSignalService service =
        new AdaptiveThrottlerSignalService(veniceServerConfig, metricsRepository, heartbeatMonitoringService);

    Assert.assertNull(service.getLatencyPercentileProvider(), "Legacy path should not allocate a provider");
    Assert.assertFalse(service.isReadLatencySignalActive());

    Metric singleGetMetric = mock(Metric.class);
    Metric multiGetMetric = mock(Metric.class);
    Metric readComputeMetric = mock(Metric.class);
    when(singleGetMetric.value()).thenReturn(20.0d); // > 10 ⇒ trips
    when(multiGetMetric.value()).thenReturn(90.0d); // < 100
    when(readComputeMetric.value()).thenReturn(40.0d); // < 150
    when(metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME)).thenReturn(singleGetMetric);
    when(metricsRepository.getMetric(MULTI_GET_LATENCY_P99_METRIC_NAME)).thenReturn(multiGetMetric);
    when(metricsRepository.getMetric(READ_COMPUTE_LATENCY_P99_METRIC_NAME)).thenReturn(readComputeMetric);
    service.refreshSignalAndThrottler();
    Assert.assertTrue(service.isReadLatencySignalActive());

    when(singleGetMetric.value()).thenReturn(1.0d);
    service.refreshSignalAndThrottler();
    Assert.assertFalse(service.isReadLatencySignalActive());

    // Heartbeat branch unaffected by the percentile flag.
    Assert.assertFalse(service.isCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(service.isCurrentLeaderMaxHeartbeatLagSignalActive());
    when(heartbeatMonitoringService.getMaxLeaderHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(100), TimeUnit.MINUTES.toMillis(1)));
    when(heartbeatMonitoringService.getMaxFollowerHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(100)));
    service.refreshSignalAndThrottler();
    Assert.assertTrue(service.isCurrentLeaderMaxHeartbeatLagSignalActive());
    Assert.assertTrue(service.isNonCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(service.isCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(service.isNonCurrentLeaderMaxHeartbeatLagSignalActive());
  }

  @Test
  public void testUpdateSignalTehutiPathMissingMetricsLeavesSignalOff() {
    MetricsRepository metricsRepository = mockMetricsRepository();
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    VeniceServerConfig veniceServerConfig = baseConfig();
    // When the metrics repo returns null (metrics not yet registered), the legacy path leaves the
    // last-known signal alone instead of tripping — original behavior.
    when(metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME)).thenReturn(null);
    when(metricsRepository.getMetric(MULTI_GET_LATENCY_P99_METRIC_NAME)).thenReturn(null);
    when(metricsRepository.getMetric(READ_COMPUTE_LATENCY_P99_METRIC_NAME)).thenReturn(null);

    AdaptiveThrottlerSignalService service =
        new AdaptiveThrottlerSignalService(veniceServerConfig, metricsRepository, heartbeatMonitoringService);
    service.refreshSignalAndThrottler();
    Assert.assertFalse(service.isReadLatencySignalActive());
  }

  // ---------- Independent provider path (flag = true) ----------

  @Test
  public void testUpdateSignalIndependentProviderPath() {
    VeniceMetricsRepository metricsRepository = selfContainedMetricsRepo();
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);

    AdaptiveThrottlerSignalService service =
        new AdaptiveThrottlerSignalService(baseConfig(), metricsRepository, heartbeatMonitoringService);

    LatencyPercentileProvider provider = service.getLatencyPercentileProvider();
    Assert.assertNotNull(provider, "Independent path must allocate a provider");
    Assert.assertFalse(service.isReadLatencySignalActive());

    // Seed singleGet above threshold (10ms), others below.
    for (int i = 0; i < 100; i++) {
      provider.observe(LatencyType.SINGLE_GET, 20.0);
      provider.observe(LatencyType.MULTI_GET, 90.0);
      provider.observe(LatencyType.READ_COMPUTE, 40.0);
    }
    service.refreshSignalAndThrottler();
    Assert.assertTrue(service.isReadLatencySignalActive());

    // Drive singleGet back under threshold by flooding the reservoir with sub-threshold samples.
    for (int i = 0; i < LatencyPercentileProvider.DEFAULT_RESERVOIR_CAPACITY; i++) {
      provider.observe(LatencyType.SINGLE_GET, 1.0);
    }
    service.refreshSignalAndThrottler();
    Assert.assertFalse(service.isReadLatencySignalActive());

    // Heartbeat branch unaffected by the percentile flag.
    when(heartbeatMonitoringService.getMaxLeaderHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(100), TimeUnit.MINUTES.toMillis(1)));
    when(heartbeatMonitoringService.getMaxFollowerHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(100)));
    service.refreshSignalAndThrottler();
    Assert.assertTrue(service.isCurrentLeaderMaxHeartbeatLagSignalActive());
    Assert.assertTrue(service.isNonCurrentFollowerMaxHeartbeatLagSignalActive());
  }

  @Test
  public void testUpdateSignalIndependentProviderEmptyReservoirLeavesSignalOff() {
    VeniceMetricsRepository metricsRepository = selfContainedMetricsRepo();
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    AdaptiveThrottlerSignalService service =
        new AdaptiveThrottlerSignalService(baseConfig(), metricsRepository, heartbeatMonitoringService);
    // No observations — provider returns 0 for p99 ⇒ signal off.
    service.refreshSignalAndThrottler();
    Assert.assertFalse(service.isReadLatencySignalActive());
  }

  // ---------- Parity: both paths produce the same signal for equivalent input ----------

  /**
   * Parity: feed identical p99 values (via Tehuti mocks to the legacy path, and via
   * {@link LatencyPercentileProvider#observe} to the independent path) and assert both paths
   * produce the same {@code isReadLatencySignalActive} at each threshold crossing. Ensures
   * flipping the flag preserves external behavior.
   */
  @Test
  public void testBothPathsProduceSameSignalAtThresholdCrossings() {
    HeartbeatMonitoringService heartbeat = mock(HeartbeatMonitoringService.class);

    // Legacy path — build the service, stub the three p99 Metrics.
    MetricsRepository tehutiRepo = mockMetricsRepository();
    AdaptiveThrottlerSignalService tehutiService =
        new AdaptiveThrottlerSignalService(baseConfig(), tehutiRepo, heartbeat);
    Metric singleGetMetric = mock(Metric.class);
    Metric multiGetMetric = mock(Metric.class);
    Metric readComputeMetric = mock(Metric.class);
    when(tehutiRepo.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME)).thenReturn(singleGetMetric);
    when(tehutiRepo.getMetric(MULTI_GET_LATENCY_P99_METRIC_NAME)).thenReturn(multiGetMetric);
    when(tehutiRepo.getMetric(READ_COMPUTE_LATENCY_P99_METRIC_NAME)).thenReturn(readComputeMetric);

    // Independent path — build the service, feed its provider.
    AdaptiveThrottlerSignalService independentService =
        new AdaptiveThrottlerSignalService(baseConfig(), selfContainedMetricsRepo(), heartbeat);
    LatencyPercentileProvider provider = independentService.getLatencyPercentileProvider();

    // Scenario 1: all under threshold ⇒ signal off on both paths.
    driveP99(singleGetMetric, multiGetMetric, readComputeMetric, provider, 1.0, 50.0, 20.0);
    tehutiService.refreshSignalAndThrottler();
    independentService.refreshSignalAndThrottler();
    Assert.assertEquals(tehutiService.isReadLatencySignalActive(), independentService.isReadLatencySignalActive());
    Assert.assertFalse(tehutiService.isReadLatencySignalActive());

    // Scenario 2: singleGet trips (> 10) ⇒ both on.
    driveP99(singleGetMetric, multiGetMetric, readComputeMetric, provider, 20.0, 50.0, 20.0);
    tehutiService.refreshSignalAndThrottler();
    independentService.refreshSignalAndThrottler();
    Assert.assertEquals(tehutiService.isReadLatencySignalActive(), independentService.isReadLatencySignalActive());
    Assert.assertTrue(tehutiService.isReadLatencySignalActive());

    // Scenario 3: multiGet trips (> 100), singleGet recovers ⇒ both on.
    driveP99(singleGetMetric, multiGetMetric, readComputeMetric, provider, 1.0, 150.0, 20.0);
    tehutiService.refreshSignalAndThrottler();
    independentService.refreshSignalAndThrottler();
    Assert.assertEquals(tehutiService.isReadLatencySignalActive(), independentService.isReadLatencySignalActive());
    Assert.assertTrue(tehutiService.isReadLatencySignalActive());

    // Scenario 4: readCompute trips (> 150) ⇒ both on.
    driveP99(singleGetMetric, multiGetMetric, readComputeMetric, provider, 1.0, 50.0, 200.0);
    tehutiService.refreshSignalAndThrottler();
    independentService.refreshSignalAndThrottler();
    Assert.assertEquals(tehutiService.isReadLatencySignalActive(), independentService.isReadLatencySignalActive());
    Assert.assertTrue(tehutiService.isReadLatencySignalActive());

    // Scenario 5: everything back under threshold ⇒ both off.
    driveP99(singleGetMetric, multiGetMetric, readComputeMetric, provider, 1.0, 50.0, 20.0);
    tehutiService.refreshSignalAndThrottler();
    independentService.refreshSignalAndThrottler();
    Assert.assertEquals(tehutiService.isReadLatencySignalActive(), independentService.isReadLatencySignalActive());
    Assert.assertFalse(tehutiService.isReadLatencySignalActive());
  }

  /**
   * Stubs Tehuti mock metric values to the target p99s AND fills the reservoir densely enough that
   * {@code getP99} returns the same target within rounding. Uses a fresh fill for each scenario so
   * the provider's ring buffer reflects only the current p99 target.
   */
  private static void driveP99(
      Metric singleGetMetric,
      Metric multiGetMetric,
      Metric readComputeMetric,
      LatencyPercentileProvider provider,
      double singleGet,
      double multiGet,
      double readCompute) {
    when(singleGetMetric.value()).thenReturn(singleGet);
    when(multiGetMetric.value()).thenReturn(multiGet);
    when(readComputeMetric.value()).thenReturn(readCompute);
    // Fill the provider's ring buffer with the target value so p99 converges to it.
    for (int i = 0; i < LatencyPercentileProvider.DEFAULT_RESERVOIR_CAPACITY; i++) {
      provider.observe(LatencyType.SINGLE_GET, singleGet);
      provider.observe(LatencyType.MULTI_GET, multiGet);
      provider.observe(LatencyType.READ_COMPUTE, readCompute);
    }
  }

  @Test
  public void testRegisterThrottler() {
    MetricsRepository metricsRepository = mockMetricsRepository();
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    VeniceServerConfig veniceServerConfig = baseConfig();
    when(veniceServerConfig.getAdaptiveThrottlerSignalRefreshIntervalInSeconds()).thenReturn(10);
    AdaptiveThrottlerSignalService service =
        new AdaptiveThrottlerSignalService(veniceServerConfig, metricsRepository, heartbeatMonitoringService);
    VeniceAdaptiveIngestionThrottler throttler = mock(VeniceAdaptiveIngestionThrottler.class);
    Assert.assertEquals(service.getAdaptiveThrottlerSignalRefreshIntervalInSeconds(), 10);
    service.registerThrottler(throttler);
    Assert.assertEquals(service.getThrottlerList().size(), 1);
    Assert.assertEquals(service.getThrottlerList().get(0), throttler);
    service.refreshSignalAndThrottler();
    Mockito.verify(throttler, times(1)).checkSignalAndAdjustThrottler();
  }
}

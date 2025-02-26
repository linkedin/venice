package com.linkedin.davinci.kafka.consumer;

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
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdaptiveThrottlerSingalServiceTest {
  @Test
  public void testUpdateSignal() {
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    when(veniceServerConfig.getAdaptiveThrottlerSingleGetLatencyThreshold()).thenReturn(10d);
    AdaptiveThrottlerSignalService adaptiveThrottlerSignalService =
        new AdaptiveThrottlerSignalService(veniceServerConfig, metricsRepository, heartbeatMonitoringService);

    // Single Get Signal
    Assert.assertFalse(adaptiveThrottlerSignalService.isSingleGetLatencySignalActive());
    Metric singleGetMetric = mock(Metric.class);
    when(singleGetMetric.value()).thenReturn(20.0d);
    when(metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME)).thenReturn(singleGetMetric);
    adaptiveThrottlerSignalService.refreshSignalAndThrottler();
    Assert.assertTrue(adaptiveThrottlerSignalService.isSingleGetLatencySignalActive());
    when(singleGetMetric.value()).thenReturn(1.0d);
    Assert.assertTrue(adaptiveThrottlerSignalService.isSingleGetLatencySignalActive());
    adaptiveThrottlerSignalService.refreshSignalAndThrottler();
    Assert.assertFalse(adaptiveThrottlerSignalService.isSingleGetLatencySignalActive());

    // Heartbeat signal
    Assert.assertFalse(adaptiveThrottlerSignalService.isCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(adaptiveThrottlerSignalService.isCurrentLeaderMaxHeartbeatLagSignalActive());
    Assert.assertFalse(adaptiveThrottlerSignalService.isNonCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(adaptiveThrottlerSignalService.isNonCurrentLeaderMaxHeartbeatLagSignalActive());

    when(heartbeatMonitoringService.getMaxLeaderHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(100), TimeUnit.MINUTES.toMillis(1)));
    when(heartbeatMonitoringService.getMaxFollowerHeartbeatLag())
        .thenReturn(new AggregatedHeartbeatLagEntry(TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(100)));
    adaptiveThrottlerSignalService.refreshSignalAndThrottler();
    Assert.assertFalse(adaptiveThrottlerSignalService.isCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertTrue(adaptiveThrottlerSignalService.isCurrentLeaderMaxHeartbeatLagSignalActive());
    Assert.assertTrue(adaptiveThrottlerSignalService.isNonCurrentFollowerMaxHeartbeatLagSignalActive());
    Assert.assertFalse(adaptiveThrottlerSignalService.isNonCurrentLeaderMaxHeartbeatLagSignalActive());
  }

  @Test
  public void testRegisterThrottler() {
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    Sensor sensor = mock(Sensor.class);
    doReturn(sensor).when(metricsRepository).sensor(anyString(), any());
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    when(veniceServerConfig.getAdaptiveThrottlerSingleGetLatencyThreshold()).thenReturn(10d);
    AdaptiveThrottlerSignalService adaptiveThrottlerSignalService =
        new AdaptiveThrottlerSignalService(veniceServerConfig, metricsRepository, heartbeatMonitoringService);
    VeniceAdaptiveIngestionThrottler adaptiveIngestionThrottler = mock(VeniceAdaptiveIngestionThrottler.class);
    adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    Assert.assertEquals(adaptiveThrottlerSignalService.getThrottlerList().size(), 1);
    Assert.assertEquals(adaptiveThrottlerSignalService.getThrottlerList().get(0), adaptiveIngestionThrottler);
    adaptiveThrottlerSignalService.refreshSignalAndThrottler();
    Mockito.verify(adaptiveIngestionThrottler, times(1)).checkSignalAndAdjustThrottler();
  }
}

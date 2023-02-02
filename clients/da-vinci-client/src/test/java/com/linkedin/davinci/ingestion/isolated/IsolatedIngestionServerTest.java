package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer.METRIC_BATCH_COUNT_LIMIT;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionServerTest {
  @Test
  public void testUpdateMetric() {
    IsolatedIngestionServer isolatedIngestionServer = mock(IsolatedIngestionServer.class);
    MetricsRepository metricsRepository = new MetricsRepository();
    metricsRepository.addMetric("foo", (x, y) -> 1.0);
    when(isolatedIngestionServer.getMetricsRepository()).thenReturn(metricsRepository);
    RedundantExceptionFilter redundantExceptionFilter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
    when(isolatedIngestionServer.getRedundantExceptionFilter()).thenReturn(redundantExceptionFilter);
    IsolatedIngestionRequestClient metricClient = mock(IsolatedIngestionRequestClient.class);
    when(isolatedIngestionServer.getMetricClient()).thenReturn(metricClient);

    Map<String, Double> metricMap = new HashMap<>();
    when(isolatedIngestionServer.getMetricsMap()).thenReturn(metricMap);
    doCallRealMethod().when(isolatedIngestionServer).reportMetricsUpdateToMainProcess();
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    metricsRepository.addMetric("bar.MAX", (x, y) -> 2.0);
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    metricsRepository.addMetric("car", (x, y) -> {
      throw new VeniceException("Metric fetching bug");
    });
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    ArgumentCaptor<IngestionMetricsReport> argumentCaptor = ArgumentCaptor.forClass(IngestionMetricsReport.class);
    verify(metricClient, times(3)).reportMetricUpdate(argumentCaptor.capture());
    Assert.assertEquals(argumentCaptor.getAllValues().get(0).aggregatedMetrics.size(), 1);
    Assert.assertEquals(argumentCaptor.getAllValues().get(1).aggregatedMetrics.size(), 1);
    Assert.assertEquals(argumentCaptor.getAllValues().get(2).aggregatedMetrics.size(), 0);
  }

  @Test
  public void testSendLargeMetricUpdate() {
    IsolatedIngestionServer isolatedIngestionServer = mock(IsolatedIngestionServer.class);
    MetricsRepository metricsRepository = new MetricsRepository();
    for (int i = 0; i <= METRIC_BATCH_COUNT_LIMIT; i++) {
      metricsRepository.addMetric("foo_" + i, (x, y) -> 1.0);
    }
    when(isolatedIngestionServer.getMetricsRepository()).thenReturn(metricsRepository);
    IsolatedIngestionRequestClient metricClient = mock(IsolatedIngestionRequestClient.class);
    when(isolatedIngestionServer.getMetricClient()).thenReturn(metricClient);

    Map<String, Double> metricMap = new HashMap<>();
    when(isolatedIngestionServer.getMetricsMap()).thenReturn(metricMap);
    doCallRealMethod().when(isolatedIngestionServer).reportMetricsUpdateToMainProcess();
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    ArgumentCaptor<IngestionMetricsReport> argumentCaptor = ArgumentCaptor.forClass(IngestionMetricsReport.class);
    verify(metricClient, times(2)).reportMetricUpdate(argumentCaptor.capture());
    Assert.assertEquals(argumentCaptor.getAllValues().get(0).aggregatedMetrics.size(), METRIC_BATCH_COUNT_LIMIT);
    Assert.assertEquals(argumentCaptor.getAllValues().get(1).aggregatedMetrics.size(), 1);
  }
}

package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;


public class ServerNettyThreadStatsTest {
  @Test
  public void testRequestThreadDistributionEmitsOnlyOpenTelemetryMetrics() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(
                ModuleMetricEntityInterface
                    .getUniqueMetricEntities(Collections.singletonList(ThreadPoolOtelMetricEntity.class)))
            .setEmitOtelMetrics(true)
            .setUseOtelExponentialHistogram(false)
            .setOtelAdditionalMetricsReader(reader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    try {
      ServerNettyThreadStats stats = new ServerNettyThreadStats(repository);
      stats.recordActiveRequestThreadCount(1);
      stats.recordActiveRequestThreadCount(3);

      assertNull(repository.getMetric(".netty_worker_thread_pool--active_request_thread_count.Avg"));
      assertNull(repository.getMetric(".netty_worker_thread_pool--active_request_thread_count.Max"));
      Attributes attributes = Attributes.builder()
          .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), "netty_worker_thread_pool")
          .build();
      String metricName =
          ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION.getMetricEntity().getMetricName();
      OpenTelemetryDataTestUtils.validateHistogramPointData(reader, 1, 3, 2, 4, attributes, metricName, "server");
      HistogramPointData histogram = OpenTelemetryDataTestUtils
          .getHistogramPointData(reader.collectAllMetrics(), metricName, "server", attributes);
      assertEquals(
          histogram.getBoundaries(),
          Arrays.asList(
              0.0,
              5.0,
              10.0,
              25.0,
              50.0,
              75.0,
              100.0,
              250.0,
              500.0,
              750.0,
              1000.0,
              2500.0,
              5000.0,
              7500.0,
              10000.0));
      assertEquals(
          histogram.getCounts(),
          Arrays.asList(0L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    } finally {
      repository.close();
    }
  }
}

package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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
            .setOtelAdditionalMetricsReader(reader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    try {
      ServerNettyThreadStats stats = new ServerNettyThreadStats(repository);
      stats.recordActiveRequestThreadCount(1);
      stats.recordActiveRequestThreadCount(3);

      assertNull(repository.getMetric(".netty_worker_thread_pool--active_request_thread_count.Avg"));
      assertNull(repository.getMetric(".netty_worker_thread_pool--active_request_thread_count.Max"));
      OpenTelemetryDataTestUtils.validateHistogramPointData(
          reader,
          1,
          3,
          2,
          4,
          Attributes.builder()
              .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), "netty_worker_thread_pool")
              .build(),
          ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION.getMetricEntity().getMetricName(),
          "server");
    } finally {
      repository.close();
    }
  }
}

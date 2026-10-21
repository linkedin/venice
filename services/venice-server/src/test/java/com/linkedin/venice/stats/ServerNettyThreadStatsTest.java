package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collections;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ServerNettyThreadStatsTest {
  private static final Attributes THREAD_POOL_ATTRIBUTES = Attributes.builder()
      .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), "netty_worker_thread_pool")
      .build();

  @DataProvider
  public Object[][] otelEnabled() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "otelEnabled")
  public void testRequestAndEventLoopMetrics(boolean otelEnabled) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(
                ModuleMetricEntityInterface
                    .getUniqueMetricEntities(Collections.singletonList(ThreadPoolOtelMetricEntity.class)))
            .setEmitOtelMetrics(otelEnabled)
            .setOtelAdditionalMetricsReader(reader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    try {
      ServerNettyThreadStats stats = new ServerNettyThreadStats(repository);
      stats.recordActiveRequestThreadCount(1);
      stats.recordActiveRequestThreadCount(3);
      ServerNettyEventLoopStats eventLoopStats = new ServerNettyEventLoopStats(repository, 4);
      eventLoopStats.recordPendingTasks(12, 10);
      eventLoopStats.recordSchedulingDelay(2.5);
      eventLoopStats.recordSchedulingDelay(7.5);

      assertEquals(metric(repository, "active_request_thread_count.Avg"), 2.0);
      assertEquals(metric(repository, "active_request_thread_count.Max"), 3.0);
      assertEquals(metric(repository, "max_thread_number.LambdaStat"), 4.0);
      assertEquals(metric(repository, "queued_task_count_gauge.LambdaStat"), 12.0);
      assertEquals(metric(repository, "max_queued_task_count_gauge.LambdaStat"), 10.0);
      assertEquals(metric(repository, "event_loop_scheduling_delay.Avg"), 5.0);
      assertEquals(metric(repository, "event_loop_scheduling_delay.Max"), 7.5);
      if (otelEnabled) {
        validateGauge(reader, ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT, 4);
        validateGauge(reader, ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT, 12);
        validateGauge(reader, ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_MAX_TASK_COUNT, 10);
        OpenTelemetryDataTestUtils.validateHistogramPointData(
            reader,
            1,
            3,
            2,
            4,
            THREAD_POOL_ATTRIBUTES,
            ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION.getMetricEntity().getMetricName(),
            "server");
        OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
            reader,
            2.5,
            7.5,
            2,
            10,
            THREAD_POOL_ATTRIBUTES,
            ThreadPoolOtelMetricEntity.THREAD_POOL_EVENT_LOOP_SCHEDULING_DELAY.getMetricEntity().getMetricName(),
            "server");
      }
      eventLoopStats.recordPendingTasks(0, 0);
      assertEquals(metric(repository, "queued_task_count_gauge.LambdaStat"), 0.0);
      assertEquals(metric(repository, "max_queued_task_count_gauge.LambdaStat"), 0.0);
      if (otelEnabled) {
        validateGauge(reader, ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT, 0);
        validateGauge(reader, ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_MAX_TASK_COUNT, 0);
      }
    } finally {
      repository.close();
    }
  }

  private void validateGauge(InMemoryMetricReader reader, ThreadPoolOtelMetricEntity metric, long expected) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        reader,
        expected,
        THREAD_POOL_ATTRIBUTES,
        metric.getMetricEntity().getMetricName(),
        "server");
  }

  private double metric(VeniceMetricsRepository repository, String name) {
    return repository.getMetric(".netty_worker_thread_pool--" + name).value();
  }
}

package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.stats.RouterHttpRequestStats.RouterTehutiMetricNameEnum.HEALTHY_REQUEST;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collection;
import org.testng.annotations.Test;


public class RouterHttpRequestStatsTest {
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void routerMetricsTest(boolean useVeniceMetricRepository, boolean isOtelEnabled) {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    MetricsRepository metricsRepository;
    if (useVeniceMetricRepository) {
      Collection<MetricEntity> metricEntities = new ArrayList<>();
      metricEntities
          .add(new MetricEntity("test_metric", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Test description"));
      metricsRepository = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository(
          isOtelEnabled,
          isOtelEnabled ? PASCAL_CASE : VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat(),
          metricEntities);
    } else {
      metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    }
    metricsRepository.addReporter(new MockTehutiReporter());

    RouterHttpRequestStats routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        false,
        null);

    if (useVeniceMetricRepository && isOtelEnabled) {
      assertTrue(routerHttpRequestStats.emitOpenTelemetryMetrics(), "Otel should be enabled");
      assertEquals(routerHttpRequestStats.getOpenTelemetryMetricsFormat(), PASCAL_CASE);
      Attributes attributes = routerHttpRequestStats.getCommonMetricDimensions();
      assertNotNull(attributes);
      attributes.forEach((key, value) -> {
        if (key.getKey().equals(VENICE_STORE_NAME.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, storeName);
        } else if (key.getKey().equals(VENICE_REQUEST_METHOD.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, RequestType.SINGLE_GET.name().toLowerCase());
        } else if (key.getKey().equals(VENICE_CLUSTER_NAME.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, clusterName);
        }
      });
    } else {
      assertFalse(routerHttpRequestStats.emitOpenTelemetryMetrics(), "Otel should not be enabled");
      assertEquals(
          routerHttpRequestStats.getOpenTelemetryMetricsFormat(),
          VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat());
      assertNull(routerHttpRequestStats.getCommonMetricDimensions());
    }

    routerHttpRequestStats.recordHealthyRequest(1.0, HttpResponseStatus.OK, 1);
    assertEquals(
        metricsRepository.getMetric("." + storeName + "--" + HEALTHY_REQUEST.getMetricName() + ".Count").value(),
        1.0);
  }
}

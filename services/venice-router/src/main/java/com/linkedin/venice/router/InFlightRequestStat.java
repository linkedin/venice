package com.linkedin.venice.router;

import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.TimeUnit;


public class InFlightRequestStat {
  public static final String TOTAL_INFLIGHT_REQUEST_COUNT = "total_inflight_request_count";
  private final MetricConfig metricConfig;
  private final VeniceMetricsRepository localMetricRepo;

  private final Sensor totalInflightRequestSensor;
  private final Metric metric;

  public InFlightRequestStat(VeniceRouterConfig config) {
    metricConfig = new MetricConfig().timeWindow(config.getRouterInFlightMetricWindowSeconds(), TimeUnit.SECONDS);
    localMetricRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(ROUTER_SERVICE_NAME)
            .setMetricPrefix(ROUTER_SERVICE_METRIC_PREFIX)
            .setTehutiMetricConfig(metricConfig)
            .build());
    totalInflightRequestSensor = localMetricRepo.sensor("total_inflight_request");
    totalInflightRequestSensor.add(TOTAL_INFLIGHT_REQUEST_COUNT, new Rate());
    metric = localMetricRepo.getMetric(TOTAL_INFLIGHT_REQUEST_COUNT);
  }

  public Sensor getTotalInflightRequestSensor() {
    return totalInflightRequestSensor;
  }

  public double getInFlightRequestRate() {
    // max return -infinity when there are no samples. validate only against finite value
    return Double.isFinite(metric.value()) ? metric.value() : 0.0;
  }
}

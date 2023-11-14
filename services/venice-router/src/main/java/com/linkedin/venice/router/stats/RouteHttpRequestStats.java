package com.linkedin.venice.router.stats;

import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.SampledTotal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class stores the stats for combined all type(SINGLE/MULTI/COMPUTE etc) of Router requests to the HttpAsync client compared
 * to {@link RouteHttpStats} which stores only per type stats.
 */
public class RouteHttpRequestStats {
  private final MetricsRepository metricsRepository;
  private final StorageNodeClient storageNodeClient;
  private final Map<String, InternalHostStats> routeStatsMap = new VeniceConcurrentHashMap<>();

  public RouteHttpRequestStats(MetricsRepository metricsRepository, StorageNodeClient storageNodeClient) {
    this.metricsRepository = metricsRepository;
    this.storageNodeClient = storageNodeClient;
  }

  public void recordPendingRequest(String hostName) {
    InternalHostStats stats = routeStatsMap.computeIfAbsent(hostName, h -> new InternalHostStats(metricsRepository, h));
    stats.recordPendingRequestCount();
  }

  public void recordFinishedRequest(String hostName) {
    InternalHostStats stats = routeStatsMap.computeIfAbsent(hostName, h -> new InternalHostStats(metricsRepository, h));
    stats.recordFinishedRequestCount();
  }

  public void recordUnhealthyQueueDuration(String hostName, double duration) {
    InternalHostStats stats = routeStatsMap.computeIfAbsent(hostName, h -> new InternalHostStats(metricsRepository, h));
    stats.recordUnhealthyQueueDuration(duration);
  }

  public long getPendingRequestCount(String hostName) {
    InternalHostStats stat = routeStatsMap.get(hostName);
    if (stat == null) {
      return 0;
    }
    return stat.pendingRequestCount.get();
  }

  static class InternalHostStats extends AbstractVeniceStats {
    // private final Sensor pendingRequestCountSensor;
    private final Sensor unhealthyPendingQueueDuration;
    private final Sensor unhealthyPendingRateSensor;
    private AtomicLong pendingRequestCount;

    public InternalHostStats(MetricsRepository metricsRepository, String hostName) {
      super(metricsRepository, StatsUtils.convertHostnameToMetricName(hostName));
      pendingRequestCount = new AtomicLong();
      // pendingRequestCountSensor =
      // registerSensor(new AsyncGauge((c, t) -> pendingRequestCount.get(), "pending_request_count"));

      unhealthyPendingQueueDuration = registerSensor(
          "unhealthy_pending_queue_duration_per_route",
          new Avg(),
          new Min(),
          new Max(),
          new SampledTotal());
      ;
      unhealthyPendingRateSensor = registerSensor("unhealthy_pending_queue_per_route", new OccurrenceRate());
    }

    public void recordPendingRequestCount() {
      pendingRequestCount.incrementAndGet();
    }

    public void recordFinishedRequestCount() {
      pendingRequestCount.decrementAndGet();
    }

    public void recordUnhealthyQueueDuration(double duration) {
      unhealthyPendingRateSensor.record();
      unhealthyPendingQueueDuration.record(duration);
    }
  }
}

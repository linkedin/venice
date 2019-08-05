package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class stores the stats for combined all type(SINGLE/MULTI/COMPUTE etc) of Router requests to the HttpAsync client compared
 * to {@link RouteHttpStats} which stores only per type stats.
 */
public class RouteHttpRequestStats {
  private final MetricsRepository metricsRepository;
  private final Map<String, InternalHostStats>
        routeStatsMap = new VeniceConcurrentHashMap<>();

    public RouteHttpRequestStats(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
    }

    public void recordPendingRequest(String hostName) {
      InternalHostStats
          stats = routeStatsMap.computeIfAbsent(hostName, h -> new InternalHostStats(metricsRepository, h));
      stats.recordPendingRequestCount();
    }

    public void recordFinishedRequest(String hostName) {
      InternalHostStats
          stats = routeStatsMap.computeIfAbsent(hostName, h -> new InternalHostStats(metricsRepository, h));
      stats.recordFinishedRequestCount();
    }

    class InternalHostStats extends AbstractVeniceStats  {
      private final Sensor pendingRequestCountSensor;
      private AtomicLong pendingRequestCount;


      public InternalHostStats(MetricsRepository metricsRepository, String hostName) {
        super(metricsRepository, hostName.replace('.', '_'));
        pendingRequestCount = new AtomicLong();
        pendingRequestCountSensor = registerSensor("pending_request_count", new Gauge(() -> pendingRequestCount.get()));
      }

      public void recordPendingRequestCount() {
        pendingRequestCount.incrementAndGet();
      }

      public void recordFinishedRequestCount() {
        pendingRequestCount.decrementAndGet();
      }
    }
  }

package com.linkedin.venice.router.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import java.util.Map;


public class RouteHttpStats {
  private final Map<String, InternalRouteHttpStats> routeStatsMap = new VeniceConcurrentHashMap<>();
  private final MetricsRepository metricsRepository;
  private final RequestType requestType;

  public RouteHttpStats(MetricsRepository metricsRepository, RequestType requestType) {
    this.metricsRepository = metricsRepository;
    this.requestType = requestType;
  }

  public void recordResponseWaitingTime(String hostName, double waitingTime) {
    InternalRouteHttpStats routeStats =
        routeStatsMap.computeIfAbsent(hostName, h -> new InternalRouteHttpStats(metricsRepository, h, requestType));
    routeStats.recordResponseWaitingTime(waitingTime);
  }

  /**
   * Get the average response waiting time (latency) for a given host for this specific request type.
   * Returns -1 if no data is available or the value is NaN.
   * This is used for latency-based least-loaded routing decisions.
   * @param hostName the hostname
   * @return average latency in milliseconds, or -1 if not available
   */
  public double getHostResponseWaitingTimeAvg(String hostName) {
    InternalRouteHttpStats routeStats = routeStatsMap.get(hostName);
    if (routeStats == null) {
      return -1;
    }

    // Get the Metric for the Avg stat
    double avgLatency;
    avgLatency = routeStats.responseWaitingTimeAvgStat.value();
    if (Double.isNaN(avgLatency)) {
      return -1;
    }
    return avgLatency;
  }

  static class InternalRouteHttpStats extends AbstractVeniceHttpStats {
    private final Sensor responseWaitingTimeSensor;
    private final Sensor requestSensor;
    private final Metric responseWaitingTimeAvgStat;

    public InternalRouteHttpStats(MetricsRepository metricsRepository, String hostName, RequestType requestType) {
      super(metricsRepository, hostName.replace('.', '_'), requestType);

      requestSensor = registerSensor("request", new Count());
      MeasurableStat avgStat = new Avg();
      responseWaitingTimeSensor = registerSensor(
          "response_waiting_time",
          TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")),
          new Max(),
          avgStat);
      responseWaitingTimeAvgStat = metricsRepository.getMetric(getMetricFullName(responseWaitingTimeSensor, avgStat));
    }

    public void recordResponseWaitingTime(double waitingTime) {
      requestSensor.record();
      responseWaitingTimeSensor.record(waitingTime);
    }
  }
}

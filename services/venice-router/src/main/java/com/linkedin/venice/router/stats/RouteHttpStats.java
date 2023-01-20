package com.linkedin.venice.router.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
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

  static class InternalRouteHttpStats extends AbstractVeniceHttpStats {
    private final Sensor responseWaitingTimeSensor;
    private final Sensor requestSensor;

    public InternalRouteHttpStats(MetricsRepository metricsRepository, String hostName, RequestType requestType) {
      super(metricsRepository, hostName.replace('.', '_'), requestType);

      requestSensor = registerSensor("request", new Count());
      responseWaitingTimeSensor = registerSensor(
          "response_waiting_time",
          TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")),
          new Max());
    }

    public void recordResponseWaitingTime(double waitingTime) {
      requestSensor.record();
      responseWaitingTimeSensor.record(waitingTime);
    }
  }
}

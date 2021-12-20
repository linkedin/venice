package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class includes the metrics in the cluster-level.
 * So far it is per store.
 */
public class ClusterStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(ClusterStats.class);

  private final String storeName;
  private final Map<String, RouteStats> perRouteStats = new VeniceConcurrentHashMap<>();
  private final Sensor blockedInstanceCount;
  private final Sensor unhealthyInstanceCount;

  public ClusterStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    this.storeName = storeName;
    this.blockedInstanceCount = registerSensor("blocked_instance_count", new Avg(), new Max());
    this.unhealthyInstanceCount = registerSensor("unhealthy_instance_count", new Avg(), new Max());
  }

  public void recordBlockedInstanceCount(int count) {
    this.blockedInstanceCount.record(count);
  }

  public void recordUnhealthyInstanceCount(int count) {
    this.unhealthyInstanceCount.record(count);
  }

  public void recordPendingRequestCount(String instance, int count) {
    getRouteStats(instance).recordPendingRequestCount(count);
  }

  private RouteStats getRouteStats(String instanceUrl) {
    return perRouteStats.computeIfAbsent(instanceUrl, k -> {
      String instanceName = instanceUrl;
      try {
        URL url = new URL(instanceUrl);
        instanceName = url.getHost()  + "_" + url.getPort();
      } catch (MalformedURLException e) {
        LOGGER.error("Invalid instance url: " + instanceUrl);
      }
      return new RouteStats(getMetricsRepository(), storeName, instanceName);
    });
  }

  private static class RouteStats extends AbstractVeniceStats {
    private final Sensor pendingRequestCounterSensor;

    public RouteStats(MetricsRepository metricsRepository, String storeName, String instanceName) {
      super(metricsRepository, storeName + "." + StatsUtils.convertHostnameToMetricName(instanceName));

      this.pendingRequestCounterSensor = registerSensor("pending_request_count", new Avg(), new Max());
    }

    public void recordPendingRequestCount(int count) {
      pendingRequestCounterSensor.record(count);
    }
  }
}

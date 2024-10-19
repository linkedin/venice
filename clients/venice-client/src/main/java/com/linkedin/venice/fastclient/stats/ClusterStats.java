package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class includes the metrics in the cluster-level.
 * So far it is per store.
 */
public class ClusterStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(ClusterStats.class);

  private final String storeName;
  private final Sensor blockedInstanceCount;
  private final Sensor unhealthyInstanceCount;
  private final Sensor versionUpdateFailureSensor;
  /* This sensor tracks the version number that the client is at. This will help in case some clients are not able
  to switch to the latest version*/
  private final Sensor currentVersionNumberSensor;
  private int currentVersion = -1;

  public ClusterStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    this.storeName = storeName;
    this.blockedInstanceCount = registerSensor("blocked_instance_count", new Avg(), new Max());
    this.unhealthyInstanceCount = registerSensor("unhealthy_instance_count", new Avg(), new Max());
    this.versionUpdateFailureSensor = registerSensor("version_update_failure", new OccurrenceRate());
    this.currentVersionNumberSensor =
        registerSensor(new AsyncGauge((ignored, ignored2) -> this.currentVersion, "current_version"));
  }

  public void recordBlockedInstanceCount(int count) {
    this.blockedInstanceCount.record(count);
  }

  public void recordUnhealthyInstanceCount(int count) {
    this.unhealthyInstanceCount.record(count);
  }

  public void updateCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  public void recordVersionUpdateFailure() {
    versionUpdateFailureSensor.record();
  }

  public List<Double> getMetricValues(String sensorName, String... stats) {
    String sensorFullName = getSensorFullName(sensorName);
    List<Double> collect = Arrays.stream(stats).map((stat) -> {
      Metric metric = getMetricsRepository().getMetric(sensorFullName + "." + stat);
      return (metric != null ? metric.value() : Double.NaN);
    }).collect(Collectors.toList());
    return collect;
  }
}

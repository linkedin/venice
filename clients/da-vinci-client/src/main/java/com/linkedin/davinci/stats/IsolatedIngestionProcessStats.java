package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * IsolatedIngestionProcessStats is a metrics collecting class that aims to collect metrics from isolated ingestion process.
 * It maintains a map of String to Double to keep the collected metrics from forked process. updateMetricMap() will be called
 * by a dedicated thread which collects metrics from child process at a fixed rate.
 * Metrics collected will be registered to main process's MetricRepository and be reported to metrics reporters(like InGraph).
 * In order to distinguish the collected metrics from the main process metrics, prefix "ingestion_isolation" is added to every metric names.
 */
public class IsolatedIngestionProcessStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionProcessStats.class);
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final String METRIC_PREFIX = "ingestion_isolation";

  private final Map<String, Double> metricValueMap = new HashMap<>();

  public IsolatedIngestionProcessStats(MetricsRepository metricsRepository) {
    super(metricsRepository, METRIC_PREFIX);
  }

  public void updateMetricMap(Map<CharSequence, Double> updateMetricValueMap) {
    Set<String> newMetricNameSet = new HashSet<>();
    try {
      updateMetricValueMap.forEach((name, value) -> {
        String originalMetricName = name.toString();
        if (!metricValueMap.containsKey(originalMetricName)) {
          /**
           * Although different metrics might contain different attributes (like AVG, MAX, COUNT), the calculation
           * is done in child process side, so we don't know how they are calculated, and we don't need to know as
           * all we need here is just to add it to the value map for reporter retrieval, so a Gauge lambda expression
           * is enough.
           * Here we split the sensor name and attribute name, so we can override the stat name in registerSensor()
           * method, so it won't show sensor_attribute.Gauge but instead sensor.attribute.
           */
          String[] sensorAndAttributeName = getSensorAndAttributeName(originalMetricName);
          if (sensorAndAttributeName.length == 1) {
            registerSensor(
                new AsyncGauge((c, t) -> this.metricValueMap.get(originalMetricName), sensorAndAttributeName[0]));
          } else if (sensorAndAttributeName.length == 2) {
            registerSensorAttributeGauge(
                sensorAndAttributeName[0],
                sensorAndAttributeName[1],
                new AsyncGauge((c, t) -> this.metricValueMap.get(originalMetricName), sensorAndAttributeName[0]));
          } else {
            /**
             * In theory due to Tehuti metric naming pattern this won't happen, but we add it as defensive
             * coding to avoid potential metric errors.
             */
            String correctedMetricName = name.toString().replace('.', '_');
            registerSensor(new AsyncGauge((c, t) -> this.metricValueMap.get(originalMetricName), correctedMetricName));
          }
          newMetricNameSet.add(originalMetricName);
        }
        metricValueMap.put(originalMetricName, value);
      });
      if (!newMetricNameSet.isEmpty()) {
        LOGGER.info("Registered {} new metrics.", newMetricNameSet.size());
        LOGGER.debug("New metrics list: {}", newMetricNameSet);
      }
    } catch (Exception e) {
      if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(e.getMessage())) {
        LOGGER.warn("Caught exception when updating metrics collected from isolated process.", e);
      }
    }
  }

  private String[] getSensorAndAttributeName(String originalMetricName) {
    // Remove the leading dot to accommodate the Tehuti metric parsing rule.
    String correctedMetricName =
        originalMetricName.startsWith(".") ? originalMetricName.substring(1) : originalMetricName;
    return correctedMetricName.split("\\.");
  }
}

package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public abstract class AbstractVeniceHttpStats extends AbstractVeniceStats {
  private RequestType requestType;

  public AbstractVeniceHttpStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName);
    this.requestType = requestType;
  }

  protected RequestType getRequestType() {
    return this.requestType;
  }

  protected String getFullMetricName(String metricName) {
    return requestType.getMetricPrefix() + metricName;
  }

  /**
   * By default, this function will prepend the request type to the sensor name.
   * @param sensorName
   * @param stats
   * @return
   */
  @Override
  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return registerSensor(getFullMetricName(sensorName), null, stats);
  }

  /**
   * By default, this function will prepend the request type to the sensor name, and register percentiles with the same name.
   */
  protected Sensor registerSensorWithDetailedPercentiles(String sensorName, MeasurableStat... stats) {
    MeasurableStat[] newStats = new MeasurableStat[stats.length + 1];
    for (int i = 0; i < stats.length; i++) {
      newStats[i] = stats[i];
    }
    newStats[stats.length] = TehutiUtils.getPercentileStatForNetworkLatency(getName(), getFullMetricName(sensorName));
    return registerSensor(getFullMetricName(sensorName), stats);
  }
}

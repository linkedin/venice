package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public abstract class AbstractVeniceHttpStats extends AbstractVeniceStats {
  private final RequestType requestType;

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
    return super.registerSensor(getFullMetricName(sensorName), null, stats);
  }

  protected Sensor registerSensor(String sensorName, Sensor[] parents, MeasurableStat... stats) {
    return super.registerSensor(getFullMetricName(sensorName), parents, stats);
  }

  /**
   * By default, this function will prepend the request type to the sensor name, and register percentiles with the same name.
   *
   * TODO: Make all uses of percentiles do this, instead of calling directly :
   * {@link TehutiUtils#getPercentileStatForNetworkLatency(String, String)}
   */
  protected Sensor registerSensorWithDetailedPercentiles(String sensorName, MeasurableStat... stats) {
    MeasurableStat[] newStats = new MeasurableStat[stats.length + 1];
    System.arraycopy(stats, 0, newStats, 0, stats.length);
    newStats[stats.length] = TehutiUtils.getPercentileStatForNetworkLatency(getName(), getFullMetricName(sensorName));
    return registerSensor(sensorName, newStats);
  }
}

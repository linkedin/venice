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
   * By default, this function will pre-append the request type to the sensor name.
   * @param sensorName
   * @param stats
   * @return
   */
  @Override
  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return registerSensor(getFullMetricName(sensorName), null, stats);
  }
}

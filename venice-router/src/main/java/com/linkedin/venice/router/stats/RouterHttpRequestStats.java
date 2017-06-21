package com.linkedin.venice.router.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;

public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  final private Sensor requestSensor;
  final private Sensor healthySensor;
  final private Sensor unhealthySensor;
  final private Sensor throttleSensor;
  final private Sensor latencySensor;
  final private Sensor requestSizeSensor;
  final private Sensor responseSizeSensor;
  final private Sensor badRequestSensor;
  final private Sensor routerResponseWaitingTimeSensor;
  final private Sensor fanoutRequestCountSensor;

  //QPS metrics
  public RouterHttpRequestStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    requestSensor = registerSensor("request", new Count(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new Count());
    unhealthySensor = registerSensor("unhealthy_request", new Count());
    throttleSensor = registerSensor("throttled_request", new Count());
    badRequestSensor = registerSensor("bad_request", new Count());
    fanoutRequestCountSensor = registerSensor("fanout_request_count", new Avg(), new Max());

    //we have to explicitly pass the name again for PercentilesStat here.
    //TODO: remove the redundancy once Tehuti library is updated.
    latencySensor = registerSensor("latency", TehutiUtils.getPercentileStat(getName(), getFullMetricName("latency")));
    routerResponseWaitingTimeSensor = registerSensor("response_waiting_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")));
    requestSizeSensor = registerSensor("request_size", TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_size")));
    responseSizeSensor = registerSensor("response_size", TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_size")));
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    unhealthySensor.record();
  }

  public void recordThrottledRequest() {
    throttleSensor.record();
  }

  public void recordBadRequest() {
    badRequestSensor.record();
  }

  public void recordFanoutRequestCount(int count) {
    if (getRequestType().equals(RequestType.MULTI_GET)) {
      fanoutRequestCountSensor.record(count);
    }
  }

  public void recordLatency(double latency) {
    latencySensor.record(latency);
  }

  public void recordResponseWaitingTime(double waitingTime) {
    routerResponseWaitingTimeSensor.record(waitingTime);
  }

  public void recordRequestSize(double requestSize) {
    requestSizeSensor.record(requestSize);
  }

  public void recordResponseSize(double responseSize) {
    responseSizeSensor.record(responseSize);
  };
}

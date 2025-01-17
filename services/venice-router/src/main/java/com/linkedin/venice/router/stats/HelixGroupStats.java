package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.OccurrenceRate;


public class HelixGroupStats extends AbstractVeniceStats {
  private final VeniceConcurrentHashMap<Integer, Sensor> groupCounterSensorMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, Sensor> pendingRequestSensorMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, Sensor> groupResponseWaitingTimeSensorMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, Metric> groupResponseWaitingTimeAvgMap =
      new VeniceConcurrentHashMap<>();
  private final Sensor groupCountSensor;

  public HelixGroupStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "HelixGroupStats");

    this.groupCountSensor = registerSensor("group_count", new Avg());
  }

  public void recordGroupNum(int groupNum) {
    groupCountSensor.record(groupNum);
  }

  public void recordGroupRequest(int groupId) {
    Sensor groupSensor = groupCounterSensorMap
        .computeIfAbsent(groupId, id -> registerSensor("group_" + groupId + "_request", new OccurrenceRate()));
    groupSensor.record();
  }

  public void recordGroupPendingRequest(int groupId, int pendingRequest) {
    Sensor pendingRequestSensor = pendingRequestSensorMap
        .computeIfAbsent(groupId, id -> registerSensor("group_" + groupId + "_pending_request", new Avg()));
    pendingRequestSensor.record(pendingRequest);
  }

  public void recordGroupResponseWaitingTime(int groupId, double responseWaitingTime) {
    Sensor groupResponseWaitingTimeSensor = groupResponseWaitingTimeSensorMap.computeIfAbsent(groupId, id -> {
      MeasurableStat avg = new Avg();
      Sensor sensor = registerSensor("group_" + groupId + "_response_waiting_time", avg);
      groupResponseWaitingTimeAvgMap.put(groupId, getMetricsRepository().getMetric(getMetricFullName(sensor, avg)));

      return sensor;
    });
    groupResponseWaitingTimeSensor.record(responseWaitingTime);
  }

  public double getGroupResponseWaitingTimeAvg(int groupId) {
    Metric groupResponseWaitingTimeAvgMetric = groupResponseWaitingTimeAvgMap.get(groupId);
    if (groupResponseWaitingTimeAvgMetric == null) {
      return -1;
    }
    double avgLatency = groupResponseWaitingTimeAvgMetric.value();
    if (Double.isNaN(avgLatency)) {
      return -1;
    }
    return avgLatency;
  }
}

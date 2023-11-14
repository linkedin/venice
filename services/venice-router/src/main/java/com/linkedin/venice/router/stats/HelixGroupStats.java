package com.linkedin.venice.router.stats;

import com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategy;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.OccurrenceRate;


public class HelixGroupStats extends AbstractVeniceStats {
  private final VeniceConcurrentHashMap<Integer, Sensor> groupCounterSensorMap = new VeniceConcurrentHashMap<>();
  private final HelixGroupSelectionStrategy strategy;

  private final Sensor groupCountSensor;
  private final Sensor maxGroupPendingRequest;
  private final Sensor minGroupPendingRequest;
  private final Sensor avgGroupPendingRequest;

  public HelixGroupStats(MetricsRepository metricsRepository, HelixGroupSelectionStrategy strategy) {
    super(metricsRepository, "HelixGroupStats");
    this.strategy = strategy;

    this.groupCountSensor = registerSensor("group_count", new Avg());
    this.maxGroupPendingRequest =
        registerSensor(new AsyncGauge((c, t) -> strategy.getMaxGroupPendingRequest(), "max_group_pending_request"));
    this.minGroupPendingRequest =
        registerSensor(new AsyncGauge((c, t) -> strategy.getMinGroupPendingRequest(), "min_group_pending_request"));
    this.avgGroupPendingRequest =
        registerSensor(new AsyncGauge((c, t) -> strategy.getAvgGroupPendingRequest(), "avg_group_pending_request"));
  }

  public void recordGroupNum(int groupNum) {
    groupCountSensor.record(groupNum);
  }

  public void recordGroupRequest(int groupId) {
    Sensor groupSensor = groupCounterSensorMap
        .computeIfAbsent(groupId, id -> registerSensor("group_" + groupId + "_request", new OccurrenceRate()));
    groupSensor.record();
  }
}

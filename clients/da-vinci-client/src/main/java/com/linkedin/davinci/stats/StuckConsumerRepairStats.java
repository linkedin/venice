package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


public class StuckConsumerRepairStats extends AbstractVeniceStats {
  private Sensor stuckConsumerFound;
  private Sensor ingestionTaskRepair;
  private Sensor repairFailure;

  public StuckConsumerRepairStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "StuckConsumerRepair");

    this.stuckConsumerFound = registerSensor("stuck_consumer_found", new OccurrenceRate());
    this.ingestionTaskRepair = registerSensor("ingestion_task_repair", new OccurrenceRate());
    this.repairFailure = registerSensor("repair_failure", new OccurrenceRate());
  }

  public void recordStuckConsumerFound() {
    stuckConsumerFound.record();
  }

  public void recordIngestionTaskRepair() {
    ingestionTaskRepair.record();
  }

  public void recordRepairFailure() {
    repairFailure.record();
  }
}

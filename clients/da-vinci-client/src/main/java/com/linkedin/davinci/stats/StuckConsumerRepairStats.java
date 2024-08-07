package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


public class StuckConsumerRepairStats extends AbstractVeniceStats {
  private Lazy<Sensor> stuckConsumerFound;
  private Lazy<Sensor> ingestionTaskRepair;
  private Lazy<Sensor> repairFailure;

  public StuckConsumerRepairStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "StuckConsumerRepair");

    this.stuckConsumerFound = Lazy.of(() -> registerSensor("stuck_consumer_found", new OccurrenceRate()));
    this.ingestionTaskRepair = Lazy.of(() -> registerSensor("ingestion_task_repair", new OccurrenceRate()));
    this.repairFailure = Lazy.of(() -> registerSensor("repair_failure", new OccurrenceRate()));
  }

  public void recordStuckConsumerFound() {
    stuckConsumerFound.get().record();
  }

  public void recordIngestionTaskRepair() {
    ingestionTaskRepair.get().record();
  }

  public void recordRepairFailure() {
    repairFailure.get().record();
  }
}

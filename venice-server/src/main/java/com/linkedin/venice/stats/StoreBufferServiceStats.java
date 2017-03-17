package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreBufferService;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;

public class StoreBufferServiceStats extends AbstractVeniceStats {
  private StoreBufferService workerService = null;
  private Sensor totalMemoryUsageSensor;
  private Sensor maxMemoryUsagePerWriterSensor;
  private Sensor minMemoryUsagePerWriterSensor;

  public StoreBufferServiceStats(MetricsRepository metricsRepository, StoreBufferService workerService) {
    super(metricsRepository, "StoreBufferService");
    this.workerService = workerService;
    totalMemoryUsageSensor = registerSensor("total_memory_usage", new LambdaStat(
        () -> this.workerService.getTotalMemoryUsage()
    ));
    maxMemoryUsagePerWriterSensor = registerSensor("max_memory_usage_per_writer", new LambdaStat(
        () -> this.workerService.getMaxMemoryUsagePerDrainer()
    ));
    minMemoryUsagePerWriterSensor = registerSensor("min_memory_usage_per_writer", new LambdaStat(
        () -> this.workerService.getMinMemoryUsagePerDrainer()
    ));
  }
}

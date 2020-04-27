package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreBufferService;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;

public class StoreBufferServiceStats extends AbstractVeniceStats {
  private StoreBufferService workerService = null;
  private Sensor totalMemoryUsageSensor;
  private Sensor totalRemainingMemorySensor;
  private Sensor maxMemoryUsagePerWriterSensor;
  private Sensor minMemoryUsagePerWriterSensor;

  public StoreBufferServiceStats(MetricsRepository metricsRepository, StoreBufferService workerService) {
    super(metricsRepository, "StoreBufferService");
    this.workerService = workerService;
    totalMemoryUsageSensor = registerSensor("total_memory_usage", new Gauge(
        () -> this.workerService.getTotalMemoryUsage()
    ));
    totalRemainingMemorySensor = registerSensor("total_remaining_memory", new Gauge(
        () -> this.workerService.getTotalRemainingMemory()
    ));
    maxMemoryUsagePerWriterSensor = registerSensor("max_memory_usage_per_writer", new Gauge(
        () -> this.workerService.getMaxMemoryUsagePerDrainer()
    ));
    minMemoryUsagePerWriterSensor = registerSensor("min_memory_usage_per_writer", new Gauge(
        () -> this.workerService.getMinMemoryUsagePerDrainer()
    ));
  }
}

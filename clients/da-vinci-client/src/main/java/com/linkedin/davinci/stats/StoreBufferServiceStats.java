package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.AbstractStoreBufferService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.ArrayList;
import java.util.List;


public class StoreBufferServiceStats extends AbstractVeniceStats {
  private AbstractStoreBufferService workerService = null;

  private Sensor totalMemoryUsageSensor;
  private Sensor totalRemainingMemorySensor;
  private Sensor maxMemoryUsagePerWriterSensor;
  private Sensor minMemoryUsagePerWriterSensor;
  private List<Sensor> preDrainerSensors = new ArrayList<>(2);

  public StoreBufferServiceStats(MetricsRepository metricsRepository, AbstractStoreBufferService workerService) {
    super(metricsRepository, "StoreBufferService");
    this.workerService = workerService;
    totalMemoryUsageSensor =
        registerSensor(new AsyncGauge((c, t) -> this.workerService.getTotalMemoryUsage(), "total_memory_usage"));
    totalRemainingMemorySensor = registerSensor(
        new AsyncGauge((c, t) -> this.workerService.getTotalRemainingMemory(), "total_remaining_memory"));
    maxMemoryUsagePerWriterSensor = registerSensor(
        new AsyncGauge((c, t) -> this.workerService.getMaxMemoryUsagePerDrainer(), "max_memory_usage_per_writer"));
    minMemoryUsagePerWriterSensor = registerSensor(
        new AsyncGauge((c, t) -> this.workerService.getMinMemoryUsagePerDrainer(), "min_memory_usage_per_writer"));

    for (int i = 0; i < this.workerService.getDrainerCount(); i++) {
      int finalIndex = i;
      registerSensor(
          new AsyncGauge(
              (c, t) -> this.workerService.getDrainerQueueMemoryUsage(finalIndex),
              "memory_usage_for_writer_num_" + i));
    }
  }
}

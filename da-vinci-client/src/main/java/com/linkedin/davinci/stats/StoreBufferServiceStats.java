package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.AbstractStoreBufferService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
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
        registerSensor("total_memory_usage", new Gauge(() -> this.workerService.getTotalMemoryUsage()));
    totalRemainingMemorySensor =
        registerSensor("total_remaining_memory", new Gauge(() -> this.workerService.getTotalRemainingMemory()));
    maxMemoryUsagePerWriterSensor = registerSensor(
        "max_memory_usage_per_writer",
        new Gauge(() -> this.workerService.getMaxMemoryUsagePerDrainer()));
    minMemoryUsagePerWriterSensor = registerSensor(
        "min_memory_usage_per_writer",
        new Gauge(() -> this.workerService.getMinMemoryUsagePerDrainer()));

    for (int i = 0; i < this.workerService.getDrainerCount(); i++) {
      int finalIndex = i;
      registerSensor(
          "memory_usage_for_writer_num_" + i,
          new Gauge(() -> this.workerService.getDrainerQueueMemoryUsage(finalIndex)));
    }
  }
}

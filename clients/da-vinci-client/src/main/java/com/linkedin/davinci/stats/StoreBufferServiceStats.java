package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.function.LongSupplier;


public class StoreBufferServiceStats extends AbstractVeniceStats {
  private final Lazy<Sensor> totalMemoryUsageSensor;
  private final Lazy<Sensor> totalRemainingMemorySensor;
  private final Lazy<Sensor> maxMemoryUsagePerWriterSensor;
  private final Lazy<Sensor> minMemoryUsagePerWriterSensor;
  private final Lazy<Sensor> internalProcessingLatencySensor;
  private final Lazy<Sensor> internalProcessingErrorSensor;

  public StoreBufferServiceStats(
      MetricsRepository metricsRepository,
      LongSupplier totalMemoryUsageSupplier,
      LongSupplier totalRemainingMemorySupplier,
      LongSupplier maxMemoryUsagePerDrainerSupplier,
      LongSupplier minMemoryUsagePerDrainerSupplier) {
    super(metricsRepository, "StoreBufferService");
    totalMemoryUsageSensor = Lazy.of(
        () -> registerSensor(
            new AsyncGauge((ignored, ignored2) -> totalMemoryUsageSupplier.getAsLong(), "total_memory_usage")));
    totalRemainingMemorySensor = Lazy.of(
        () -> registerSensor(
            new AsyncGauge((ignored, ignored2) -> totalRemainingMemorySupplier.getAsLong(), "total_remaining_memory")));
    maxMemoryUsagePerWriterSensor = Lazy.of(
        () -> registerSensor(
            new AsyncGauge(
                (ignored, ignored2) -> maxMemoryUsagePerDrainerSupplier.getAsLong(),
                "max_memory_usage_per_writer")));
    minMemoryUsagePerWriterSensor = Lazy.of(
        () -> registerSensor(
            new AsyncGauge(
                (ignored, ignored2) -> minMemoryUsagePerDrainerSupplier.getAsLong(),
                "min_memory_usage_per_writer")));

    internalProcessingLatencySensor =
        Lazy.of(() -> registerSensor("internal_processing_latency", new Avg(), new Max()));
    internalProcessingErrorSensor = Lazy.of(() -> registerSensor("internal_processing_error", new OccurrenceRate()));
  }

  public void recordInternalProcessingError() {
    internalProcessingErrorSensor.get().record();
  }

  public void recordInternalProcessingLatency(long latency) {
    internalProcessingLatencySensor.get().record(latency);
  }
}

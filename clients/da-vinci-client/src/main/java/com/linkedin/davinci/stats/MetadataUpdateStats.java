package com.linkedin.davinci.stats;

import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;


/**
 * MetadataUpdateStats records metrics related to storage metadata update via {@link MainIngestionStorageMetadataService}
 */
public class MetadataUpdateStats extends AbstractVeniceStats {
  private static final String METRICS_PREFIX = "ingestion_isolation_metadata_updates";

  // Number of remaining elements inside metadata update queue.
  private final Lazy<Sensor> metadataUpdateQueueLengthSensor;
  // If we encountered unknown exception during metadata update, we will set the Gauge value to 1
  private final Lazy<Sensor> metadataUpdateQueueErrorSensor;

  public MetadataUpdateStats(MetricsRepository metricsRepository) {
    super(metricsRepository, METRICS_PREFIX);
    metadataUpdateQueueLengthSensor = Lazy.of(() -> registerSensor("queue_length", new Gauge()));
    metadataUpdateQueueErrorSensor = Lazy.of(() -> registerSensor("queue_update_error", new Gauge()));
    // Reset metadata update queue error Gauge.
    recordMetadataQueueUpdateError(0.0);
  }

  public void recordMetadataUpdateQueueLength(int queueLength) {
    metadataUpdateQueueLengthSensor.get().record(queueLength);
  }

  public void recordMetadataQueueUpdateError(double value) {
    metadataUpdateQueueErrorSensor.get().record(value);
  }
}

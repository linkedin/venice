package com.linkedin.venice.stats;

import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


/**
 * A host level metric to track number of active venice writers.
 */
public class VeniceWriterStats extends AbstractVeniceStats {
  /**
   * Metric for active VeniceWriter numbers.
   * When shared producer service is on, the number of VeniceWriter is not equal to number of producer instances.
   */
  private final Sensor openVeniceWriterCount;

  /**
   * Metric to track number of the VeniceWriter that fails to close in a host.
   */
  private final Sensor veniceWriterFailedToCloseCount;

  public VeniceWriterStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "VeniceWriterStats");
    openVeniceWriterCount =
        registerSensor("open_venice_writer_count", new Gauge(() -> VeniceWriter.OPEN_VENICE_WRITER_COUNT.get()));
    veniceWriterFailedToCloseCount = registerSensor(
        "venice_writer_failed_to_close_count",
        new Gauge(() -> VeniceWriter.VENICE_WRITER_CLOSE_FAILED_COUNT.get()));
  }
}

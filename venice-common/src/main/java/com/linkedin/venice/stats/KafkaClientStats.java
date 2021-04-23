package com.linkedin.venice.stats;

import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


/**
 * This stats should work in the host level to measure stats of Kafka clients, like number of active clients.
 */
public class KafkaClientStats extends AbstractVeniceStats {
  /**
   * Metric for active VeniceWriter numbers; once shared producer service is on, the number of VeniceWriter is not equal
   * to number of Kafka producer anymore.
   */
  private final Sensor openVeniceWriterCount;

  /**
   * Metric to track number of the VeniceWriter that fails to close in a host.
   */
  private final Sensor veniceWriterFailedToCloseCount;

  public KafkaClientStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    openVeniceWriterCount = registerSensor("open_venice_writer_count",
        new Gauge(() -> VeniceWriter.OPEN_VENICE_WRITER_COUNT.get()));
    veniceWriterFailedToCloseCount = registerSensor("venice_writer_failed_to_close_count",
        new Gauge(() -> VeniceWriter.VENICE_WRITER_CLOSE_FAILED_COUNT.get()));
  }
}

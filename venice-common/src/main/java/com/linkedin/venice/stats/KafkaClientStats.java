package com.linkedin.venice.stats;

import com.linkedin.venice.writer.SharedKafkaProducerService;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.Optional;


/**
 * This stats should work in the host level to measure stats of Kafka clients, like number of active clients.
 */
public class KafkaClientStats extends AbstractVeniceStats {
  private Optional<SharedKafkaProducerService> sharedKafkaProducerService;

  /**
   * Metric for active VeniceWriter numbers; once shared producer service is on, the number of VeniceWriter is not equal
   * to number of Kafka producer anymore.
   */
  private final Sensor openVeniceWriterCount;

  /**
   * Metric to track number of the VeniceWriter that fails to close in a host.
   */
  private final Sensor veniceWriterFailedToCloseCount;

  /**
   * Metric to keep track of number of currently active ingestion tasks that is using a shared producer instance.
   */
  private Sensor sharedProducerActiveTasksCountSensor;

  /**
   * Metric to keep track of number of open shared producer instance.
   */
  private Sensor sharedProducerActiveCountSensor;

  private KafkaClientStats(
      MetricsRepository metricsRepository,
      String name,
      Optional<SharedKafkaProducerService> sharedKafkaProducerService) {
    super(metricsRepository, name);
    this.sharedKafkaProducerService = sharedKafkaProducerService;

    openVeniceWriterCount =
        registerSensor("open_venice_writer_count", new Gauge(() -> VeniceWriter.OPEN_VENICE_WRITER_COUNT.get()));
    veniceWriterFailedToCloseCount = registerSensor(
        "venice_writer_failed_to_close_count",
        new Gauge(() -> VeniceWriter.VENICE_WRITER_CLOSE_FAILED_COUNT.get()));

    if (sharedKafkaProducerService.isPresent()) {
      sharedProducerActiveTasksCountSensor = registerSensor(
          "shared_producer_active_task_count",
          new Gauge(() -> sharedKafkaProducerService.get().getActiveSharedProducerTasksCount()));
      sharedProducerActiveCountSensor = registerSensor(
          "shared_producer_active_count",
          new Gauge(() -> sharedKafkaProducerService.get().getActiveSharedProducerCount()));
    }
  }

  public static void registerKafkaClientStats(
      MetricsRepository metricsRepository,
      String name,
      Optional<SharedKafkaProducerService> sharedKafkaProducerService) {
    new KafkaClientStats(metricsRepository, name, sharedKafkaProducerService);
  }
}

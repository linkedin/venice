package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.atomic.AtomicLong;


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

  /**
   * Metric to keep track of number of currently active ingestion tasks that is using a shared producer instance.
   */
  private final Sensor activeSharedProducerTasksCountSensor;

  /**
   * Metric to keep track of number of open shared producer instance.
   */
  private final Sensor activeSharedProducerCountSensor;


  private final AtomicLong activeSharedProducerTasksCount;
  private final AtomicLong activeSharedProducerCount;

  public KafkaClientStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    activeSharedProducerTasksCount = new AtomicLong(0);
    activeSharedProducerCount = new AtomicLong(0);
    openVeniceWriterCount = registerSensor("open_venice_writer_count",
        new Gauge(() -> VeniceWriter.OPEN_VENICE_WRITER_COUNT.get()));
    veniceWriterFailedToCloseCount = registerSensor("venice_writer_failed_to_close_count",
        new Gauge(() -> VeniceWriter.VENICE_WRITER_CLOSE_FAILED_COUNT.get()));
    activeSharedProducerTasksCountSensor = registerSensor("active_shared_producer_task_count",
        new Gauge(() -> getActiveSharedProducerTasksCount()));
    activeSharedProducerCountSensor = registerSensor("active_shared_producer_count",
        new Gauge(() -> getActiveSharedProducerCount()));
  }

  public long getActiveSharedProducerTasksCount() {
    return activeSharedProducerTasksCount.get();
  }

  public void incrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.incrementAndGet();
  }

  public void decrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.decrementAndGet();
  }

  public long getActiveSharedProducerCount() {
    return activeSharedProducerCount.get();
  }

  public void incrActiveSharedProducerCount() {
    activeSharedProducerCount.incrementAndGet();
  }

  public void decrActiveSharedProducerCount() {
    activeSharedProducerCount.decrementAndGet();
  }
}

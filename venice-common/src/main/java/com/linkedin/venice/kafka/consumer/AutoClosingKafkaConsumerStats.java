package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.SampledCount;
import java.util.Map;
import java.util.WeakHashMap;


public class AutoClosingKafkaConsumerStats extends AbstractVeniceStats {
  private final Sensor autoClose;
  private final Sensor autoCreate;
  private final Sensor durationFromCloseToCreate;
  private final Sensor durationFromCreateToClose;
  private final Sensor createdConsumersCount;
  private final Sensor closedConsumersCount;

  /**
   * Here we are using a {@link WeakHashMap} to hang on to references of the consumers so that we don't
   * impede their GC, and thus only keep track of those which were still reachable as of the last GC.
   */
  private Map<AutoClosingKafkaConsumer, Boolean> autoClosingKafkaConsumerCollection = new WeakHashMap<>();

  public AutoClosingKafkaConsumerStats(MetricsRepository metricsRepository, String name) {
    // The replace is to avoid: javax.management.MalformedObjectNameException: Invalid character ':' in value part of
    // property
    super(metricsRepository, name.replace(':', '_'));
    this.autoClose = registerSensor("auto_close", new OccurrenceRate(), new Count(), new SampledCount());
    this.autoCreate = registerSensor("auto_create", new OccurrenceRate(), new Count(), new SampledCount());

    this.durationFromCloseToCreate = registerSensor(
        "duration_from_close_to_create_ms",
        new Sensor[] { this.autoCreate },
        new Min(),
        new Max(),
        new Avg());
    this.durationFromCreateToClose = registerSensor(
        "duration_from_create_to_close_ms",
        new Sensor[] { this.autoClose },
        new Min(),
        new Max(),
        new Avg());

    this.createdConsumersCount = registerSensor(
        "created_consumers_count",
        new Gauge(
            () -> autoClosingKafkaConsumerCollection.keySet()
                .stream()
                .filter(consumer -> !consumer.assignment().isEmpty())
                .count()));

    this.closedConsumersCount = registerSensor(
        "closed_consumers_count",
        new Gauge(
            () -> autoClosingKafkaConsumerCollection.keySet()
                .stream()
                .filter(consumer -> consumer.assignment().isEmpty())
                .count()));
  }

  public void recordAutoCreate() {
    autoCreate.record();
  }

  public void recordAutoClose() {
    autoClose.record();
  }

  /**
   * Note, this also increments {@link #autoCreate} since it is a parent to this metric.
   */
  public void recordDurationFromCloseToCreate(long durationMs) {
    durationFromCloseToCreate.record(durationMs);
  }

  /**
   * Note, this also increments {@link #autoClose} since it is a parent to this metric.
   */
  public void recordDurationFromCreateToClose(long durationMs) {
    durationFromCreateToClose.record(durationMs);
  }

  public void addConsumerToTrack(AutoClosingKafkaConsumer consumer) {
    autoClosingKafkaConsumerCollection.put(consumer, true);
  }
}

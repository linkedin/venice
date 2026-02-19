package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import java.util.function.Supplier;


public class DictionaryRetrievalServiceStats extends AbstractVeniceStats {
  private final Sensor downloadSuccessSensor;
  private final Sensor downloadFailureSensor;
  private final Sensor versionQueuedSensor;
  private final Sensor versionSkippedByStatusSensor;
  private final Sensor versionSkippedByExistingFutureSensor;
  private final Sensor consumerPollSensor;

  public DictionaryRetrievalServiceStats(
      MetricsRepository metricsRepository,
      String name,
      Supplier<Integer> pendingCandidatesSupplier,
      Supplier<Integer> downloadingFuturesCountSupplier,
      Supplier<Boolean> retrieverThreadAliveSupplier) {
    super(metricsRepository, name);

    // Gauges
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored1, ignored2) -> pendingCandidatesSupplier.get(),
            "dictionary_download_candidates_pending"));
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored1, ignored2) -> downloadingFuturesCountSupplier.get(),
            "dictionary_downloading_futures_count"));
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored1, ignored2) -> retrieverThreadAliveSupplier.get() ? 1 : 0,
            "dictionary_retriever_thread_alive"));

    // Count sensors
    downloadSuccessSensor = registerSensorIfAbsent("dictionary_download_success", new Count());
    downloadFailureSensor = registerSensorIfAbsent("dictionary_download_failure", new Count());
    versionQueuedSensor = registerSensorIfAbsent("dictionary_version_queued", new Count());
    versionSkippedByStatusSensor = registerSensorIfAbsent("dictionary_version_skipped_status", new Count());
    versionSkippedByExistingFutureSensor =
        registerSensorIfAbsent("dictionary_version_skipped_existing_future", new Count());
    consumerPollSensor = registerSensorIfAbsent("dictionary_consumer_poll", new Count());
  }

  public void recordDownloadSuccess() {
    downloadSuccessSensor.record();
  }

  public void recordDownloadFailure() {
    downloadFailureSensor.record();
  }

  public void recordVersionQueued() {
    versionQueuedSensor.record();
  }

  public void recordVersionSkippedByStatus() {
    versionSkippedByStatusSensor.record();
  }

  public void recordVersionSkippedByExistingFuture() {
    versionSkippedByExistingFutureSensor.record();
  }

  public void recordConsumerPoll() {
    consumerPollSensor.record();
  }
}

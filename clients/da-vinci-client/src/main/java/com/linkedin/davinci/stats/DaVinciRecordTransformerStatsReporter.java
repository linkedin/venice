package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_RECOVERY_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.function.DoubleSupplier;
import java.util.function.Function;


/**
 * This class is the reporting class for stats class {@link DaVinciRecordTransformerStats}
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
public class DaVinciRecordTransformerStatsReporter extends AbstractVeniceStatsReporter<DaVinciRecordTransformerStats> {
  public DaVinciRecordTransformerStatsReporter(
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerLatencySensor(
        RECORD_TRANSFORMER_PUT_LATENCY,
        DaVinciRecordTransformerStats::getRecordTransformerPutLatencySensor);
    registerLatencySensor(
        RECORD_TRANSFORMER_DELETE_LATENCY,
        DaVinciRecordTransformerStats::getRecordTransformerDeleteLatencySensor);
    registerLatencySensor(
        RECORD_TRANSFORMER_ON_RECOVERY_LATENCY,
        DaVinciRecordTransformerStats::getRecordTransformerOnRecoveryLatencySensor);
    registerLatencySensor(
        RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY,
        DaVinciRecordTransformerStats::getRecordTransformerOnStartVersionIngestionLatencySensor);
    registerLatencySensor(
        RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY,
        DaVinciRecordTransformerStats::getRecordTransformerOnEndVersionIngestionLatencySensor);

    registerSensor(
        new DaVinciRecordTransformerStatsReporter.DaVinciRecordTransformerStatsGauge(
            this,
            () -> getStats().getTransformerPutErrorCount(),
            0,
            RECORD_TRANSFORMER_PUT_ERROR_COUNT));
  }

  // Assumed time unit is in microseconds (µ)
  protected void registerLatencySensor(
      String sensorBaseName,
      Function<DaVinciRecordTransformerStats, WritePathLatencySensor> sensorFunction) {
    registerSensor(
        new DaVinciRecordTransformerStatsReporter.DaVinciRecordTransformerStatsGauge(
            this,
            () -> sensorFunction.apply(getStats()).getAvg(),
            sensorBaseName + "_avg_µs"));
    registerSensor(
        new DaVinciRecordTransformerStatsReporter.DaVinciRecordTransformerStatsGauge(
            this,
            () -> sensorFunction.apply(getStats()).getMax(),
            sensorBaseName + "_max_µs"));
  }

  protected static class DaVinciRecordTransformerStatsGauge extends AsyncGauge {
    DaVinciRecordTransformerStatsGauge(
        AbstractVeniceStatsReporter reporter,
        DoubleSupplier supplier,
        String metricName) {
      this(reporter, supplier, NULL_INGESTION_STATS.code, metricName);
    }

    DaVinciRecordTransformerStatsGauge(
        AbstractVeniceStatsReporter reporter,
        DoubleSupplier supplier,
        int defaultValue,
        String metricName) {
      /**
       * If a version doesn't exist, the corresponding reporter stat doesn't exist after the host restarts,
       * which is not an error. The users of the stats should decide whether it's reasonable to emit an error
       * code simply because the version is not created yet.
       */
      super((ignored, ignored2) -> reporter.getStats() == null ? defaultValue : supplier.getAsDouble(), metricName);
    }
  }
}

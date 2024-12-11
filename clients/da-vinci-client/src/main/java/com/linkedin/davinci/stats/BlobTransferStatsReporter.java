package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.function.DoubleSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the reporting class for stats class {@link BlobTransferStats}
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
public class BlobTransferStatsReporter extends AbstractVeniceStatsReporter<BlobTransferStats> {
  private static final Logger LOGGER = LogManager.getLogger(IngestionStatsReporter.class);

  public BlobTransferStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getBlobTransferTotalNumResponses(),
            BlobTransferStats.BLOB_TRANSFER_TOTAL_NUM_RESPONSES));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getBlobTransferSuccessNumResponses(),
            BlobTransferStats.BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getBlobTransferFailedNumResponses(),
            BlobTransferStats.BLOB_TRANSFER_FAILED_NUM_RESPONSES));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getBlobTransferFileReceiveThroughput(),
            BlobTransferStats.BLOB_TRANSFER_THROUGHPUT));
    registerSensor(
        new IngestionStatsGauge(this, () -> getStats().getBlobTransferTime(), BlobTransferStats.BLOB_TRANSFER_TIME));
  }

  protected static class IngestionStatsGauge extends AsyncGauge {
    IngestionStatsGauge(AbstractVeniceStatsReporter reporter, DoubleSupplier supplier, String metricName) {
      this(reporter, supplier, NULL_INGESTION_STATS.code, metricName);
    }

    IngestionStatsGauge(
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

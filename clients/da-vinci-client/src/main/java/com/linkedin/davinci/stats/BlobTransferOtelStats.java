package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.BYTES_RECEIVED;
import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.BYTES_SENT;
import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.KAFKA_FALLBACK_COUNT;
import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.REQUEST_COUNT;
import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.RESPONSE_COUNT;
import static com.linkedin.davinci.stats.BlobTransferOtelMetricEntity.TIME;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferFallbackReason;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * OpenTelemetry metrics for blob transfer statistics.
 *
 * <p>This is a versioned OTel stats class — each instance is per-store. A {@code volatile VersionInfo}
 * field tracks the current and future version numbers so recording methods can classify the version
 * being recorded as CURRENT, FUTURE, or BACKUP.
 *
 * <p>Note: Tehuti metrics are managed separately in {@link BlobTransferStats} and
 * {@link BlobTransferStatsReporter}. This class handles only OTel metrics.
 */
public class BlobTransferOtelStats {
  private final boolean emitOtelMetrics;

  private volatile VersionInfo versionInfo = VersionInfo.NON_EXISTING;

  /** Attempted transfer count with VersionRole + source + status dimensions. */
  private final MetricEntityStateThreeEnums<VersionRole, VeniceBlobTransferSource, VeniceResponseStatusCategory> requestCountMetric;

  /** Kafka fallback count with VersionRole + fallback-reason dimensions. */
  private final MetricEntityStateTwoEnums<VersionRole, VeniceBlobTransferFallbackReason> kafkaFallbackCountMetric;

  /** Response count with VersionRole + VeniceResponseStatusCategory dimensions. */
  private final MetricEntityStateTwoEnums<VersionRole, VeniceResponseStatusCategory> responseCountMetric;

  /** Blob transfer time histogram with VersionRole dimension. */
  private final MetricEntityStateOneEnum<VersionRole> timeMetric;

  /** Bytes received counter with VersionRole dimension. */
  private final MetricEntityStateOneEnum<VersionRole> bytesReceivedMetric;

  /** Bytes sent counter with VersionRole dimension. */
  private final MetricEntityStateOneEnum<VersionRole> bytesSentMetric;

  /**
   * Constructs a BlobTransferOtelStats instance for a specific store.
   *
   * <p>OTel emission is determined solely by the global OTel flag in the
   * {@link com.linkedin.venice.stats.VeniceMetricsRepository}. A plain {@link MetricsRepository}
   * disables OTel emission.
   *
   * @param metricsRepository the metrics repository
   * @param storeName the name of the Venice store
   * @param clusterName the Venice cluster name
   */
  public BlobTransferOtelStats(MetricsRepository metricsRepository, String storeName, String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();

    this.emitOtelMetrics = otelSetup.emitOpenTelemetryMetrics();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelSetup.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelSetup.getBaseDimensionsMap();

    requestCountMetric = MetricEntityStateThreeEnums.create(
        REQUEST_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        VeniceBlobTransferSource.class,
        VeniceResponseStatusCategory.class);

    kafkaFallbackCountMetric = MetricEntityStateTwoEnums.create(
        KAFKA_FALLBACK_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        VeniceBlobTransferFallbackReason.class);

    responseCountMetric = MetricEntityStateTwoEnums.create(
        RESPONSE_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        VeniceResponseStatusCategory.class);

    timeMetric = createOneEnumMetric(TIME.getMetricEntity(), otelRepository, baseDimensionsMap);
    bytesReceivedMetric = createOneEnumMetric(BYTES_RECEIVED.getMetricEntity(), otelRepository, baseDimensionsMap);
    bytesSentMetric = createOneEnumMetric(BYTES_SENT.getMetricEntity(), otelRepository, baseDimensionsMap);
  }

  private static MetricEntityStateOneEnum<VersionRole> createOneEnumMetric(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    return MetricEntityStateOneEnum.create(metricEntity, otelRepository, baseDimensionsMap, VersionRole.class);
  }

  public boolean emitOtelMetrics() {
    return emitOtelMetrics;
  }

  public void updateVersionInfo(int currentVersion, int futureVersion) {
    this.versionInfo = new VersionInfo(currentVersion, futureVersion);
  }

  /**
   * Records a blob transfer response count with SUCCESS or FAIL status.
   *
   * <p>Consolidates the 3 Tehuti sensors (total/success/fail) into 1 OTel COUNTER
   * with a {@link VeniceResponseStatusCategory} dimension. The total is the sum of
   * SUCCESS and FAIL in OTel, so there is no separate total recording.
   */
  public void recordResponseCount(int version, VeniceResponseStatusCategory status) {
    responseCountMetric.record(1, OtelVersionedStatsUtils.classifyVersion(version, versionInfo), status);
  }

  /**
   * Records an attempted blob transfer, attributed to the source that served it.
   *
   * <p>Only transfers that were actually attempted are counted; hosts filtered out by the
   * pre-transfer connectability check never reach this method.
   */
  public void recordRequestCount(int version, VeniceBlobTransferSource source, VeniceResponseStatusCategory status) {
    requestCountMetric.record(1, OtelVersionedStatsUtils.classifyVersion(version, versionInfo), source, status);
  }

  /**
   * Records that a replica fell back to Kafka bootstrapping instead of blob transfer.
   */
  public void recordKafkaFallback(int version, VeniceBlobTransferFallbackReason reason) {
    kafkaFallbackCountMetric.record(1, OtelVersionedStatsUtils.classifyVersion(version, versionInfo), reason);
  }

  /**
   * Records blob transfer time in seconds.
   *
   * @param version the store version
   * @param timeInSec the blob transfer time in seconds
   */
  public void recordTime(int version, double timeInSec) {
    timeMetric.record(timeInSec, OtelVersionedStatsUtils.classifyVersion(version, versionInfo));
  }

  /**
   * Records bytes received via blob transfer.
   *
   * @param version the store version
   * @param bytes the number of bytes received
   */
  public void recordBytesReceived(int version, long bytes) {
    bytesReceivedMetric.record(bytes, OtelVersionedStatsUtils.classifyVersion(version, versionInfo));
  }

  /**
   * Records bytes sent via blob transfer.
   *
   * @param version the store version
   * @param bytes the number of bytes sent
   */
  public void recordBytesSent(int version, long bytes) {
    bytesSentMetric.record(bytes, OtelVersionedStatsUtils.classifyVersion(version, versionInfo));
  }
}

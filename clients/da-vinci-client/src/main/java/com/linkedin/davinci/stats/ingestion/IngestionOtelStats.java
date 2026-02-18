package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.CONSUMER_IDLE_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.DISK_QUOTA_USED;
import static com.linkedin.davinci.stats.ServerMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TASK_ERROR_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.IngestionStatsUtils;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceDCREvent;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceIngestionDestinationComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * OpenTelemetry metrics for ingestion statistics.
 * Note: Tehuti metrics are managed separately in {@link com.linkedin.davinci.stats.IngestionStatsReporter}.
 */
public class IngestionOtelStats {
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private volatile VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

  // Store ingestion tasks by version for ASYNC_GAUGE callbacks
  private final Map<Integer, StoreIngestionTask> ingestionTasksByVersion;

  // Push timeout gauge values by version
  private final Map<Integer, Integer> pushTimeoutByVersion;

  // Idle time values by version
  private final Map<Integer, AtomicLong> idleTimeByVersion;

  // ASYNC_GAUGE metrics per VersionRole
  private final AsyncMetricEntityStateOneEnum<VersionRole> taskErrorCountByRole;
  private final AsyncMetricEntityStateOneEnum<VersionRole> pushTimeoutCountByRole;
  private final AsyncMetricEntityStateOneEnum<VersionRole> diskQuotaUsedByRole;
  private final AsyncMetricEntityStateOneEnum<VersionRole> consumerIdleTimeByRole;

  // Non-ASYNC_GAUGE metrics
  // Metrics with only VersionRole dimension
  private final MetricEntityStateOneEnum<VersionRole> subscribePrepTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> ingestionTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> preprocessingLeaderTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> preprocessingInternalTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> producerTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> batchProcessingRequestCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> batchProcessingRequestRecordCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> batchProcessingRequestErrorCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> batchProcessingRequestTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> dcrTotalCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> duplicateKeyUpdateCountMetric;

  // Metrics with VersionRole + ReplicaType dimensions
  private final MetricEntityStateTwoEnums<VersionRole, ReplicaType> recordsConsumedMetric;
  private final MetricEntityStateTwoEnums<VersionRole, ReplicaType> recordsProducedMetric;
  private final MetricEntityStateTwoEnums<VersionRole, ReplicaType> bytesConsumedMetric;
  private final MetricEntityStateTwoEnums<VersionRole, ReplicaType> bytesProducedMetric;
  private final MetricEntityStateTwoEnums<VersionRole, ReplicaType> producerCallbackTimeMetric;

  // Metrics with VersionRole + VeniceDCREvent dimensions
  private final MetricEntityStateTwoEnums<VersionRole, VeniceDCREvent> dcrEventCountMetric;

  // Metrics with VersionRole + SourceComponent + DestinationComponent dimensions
  private final MetricEntityStateThreeEnums<VersionRole, VeniceIngestionSourceComponent, VeniceIngestionDestinationComponent> timeBetweenComponentsMetric;

  public IngestionOtelStats(MetricsRepository metricsRepository, String storeName, String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();

    this.emitOtelMetrics = otelSetup.emitOpenTelemetryMetrics();
    this.otelRepository = otelSetup.getOtelRepository();
    this.baseDimensionsMap = otelSetup.getBaseDimensionsMap();

    // Initialize per-version state maps
    this.ingestionTasksByVersion = new VeniceConcurrentHashMap<>();
    this.pushTimeoutByVersion = new VeniceConcurrentHashMap<>();
    this.idleTimeByVersion = new VeniceConcurrentHashMap<>();

    // Initialize ASYNC_GAUGE metrics per VersionRole
    taskErrorCountByRole = AsyncMetricEntityStateOneEnum.create(
        INGESTION_TASK_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        role -> () -> getTaskErrorCountForRole(role));

    pushTimeoutCountByRole = AsyncMetricEntityStateOneEnum.create(
        INGESTION_TASK_PUSH_TIMEOUT_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        role -> () -> getPushTimeoutCountForRole(role));

    diskQuotaUsedByRole = AsyncMetricEntityStateOneEnum.create(
        DISK_QUOTA_USED.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        role -> () -> getDiskQuotaUsedForRole(role));

    consumerIdleTimeByRole = AsyncMetricEntityStateOneEnum.create(
        CONSUMER_IDLE_TIME.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        role -> () -> getIdleTimeForRole(role));

    // Initialize metrics with only VersionRole dimension
    subscribePrepTimeMetric = createOneEnumMetric(INGESTION_SUBSCRIBE_PREP_TIME.getMetricEntity());
    ingestionTimeMetric = createOneEnumMetric(INGESTION_TIME.getMetricEntity());
    preprocessingLeaderTimeMetric = createOneEnumMetric(INGESTION_PREPROCESSING_LEADER_TIME.getMetricEntity());
    preprocessingInternalTimeMetric = createOneEnumMetric(INGESTION_PREPROCESSING_INTERNAL_TIME.getMetricEntity());
    producerTimeMetric = createOneEnumMetric(INGESTION_PRODUCER_TIME.getMetricEntity());
    batchProcessingRequestCountMetric = createOneEnumMetric(BATCH_PROCESSING_REQUEST_COUNT.getMetricEntity());
    batchProcessingRequestRecordCountMetric =
        createOneEnumMetric(BATCH_PROCESSING_REQUEST_RECORD_COUNT.getMetricEntity());
    batchProcessingRequestErrorCountMetric =
        createOneEnumMetric(BATCH_PROCESSING_REQUEST_ERROR_COUNT.getMetricEntity());
    batchProcessingRequestTimeMetric = createOneEnumMetric(BATCH_PROCESSING_REQUEST_TIME.getMetricEntity());
    dcrTotalCountMetric = createOneEnumMetric(DCR_TOTAL_COUNT.getMetricEntity());
    duplicateKeyUpdateCountMetric = createOneEnumMetric(DUPLICATE_KEY_UPDATE_COUNT.getMetricEntity());

    // Initialize metrics with VersionRole + ReplicaType dimensions
    recordsConsumedMetric = createTwoEnumMetric(INGESTION_RECORDS_CONSUMED.getMetricEntity(), ReplicaType.class);
    recordsProducedMetric = createTwoEnumMetric(INGESTION_RECORDS_PRODUCED.getMetricEntity(), ReplicaType.class);
    bytesConsumedMetric = createTwoEnumMetric(INGESTION_BYTES_CONSUMED.getMetricEntity(), ReplicaType.class);
    bytesProducedMetric = createTwoEnumMetric(INGESTION_BYTES_PRODUCED.getMetricEntity(), ReplicaType.class);
    producerCallbackTimeMetric =
        createTwoEnumMetric(INGESTION_PRODUCER_CALLBACK_TIME.getMetricEntity(), ReplicaType.class);

    // Initialize metrics with VersionRole + VeniceDCREvent dimensions
    dcrEventCountMetric = createTwoEnumMetric(DCR_EVENT_COUNT.getMetricEntity(), VeniceDCREvent.class);

    // Initialize metrics with VersionRole + SourceComponent + DestinationComponent dimensions
    timeBetweenComponentsMetric = MetricEntityStateThreeEnums.create(
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        VeniceIngestionSourceComponent.class,
        VeniceIngestionDestinationComponent.class);

  }

  /**
   * Gets the version number for a given VersionRole. Used only for async metrics.
   * For BACKUP, returns the smallest version that is neither current nor future,
   * ensuring deterministic behavior when multiple backup versions exist.
   *
   * @return The version number, or NON_EXISTING_VERSION if not found
   */
  private int getVersionForRole(VersionRole role) {
    VersionInfo info = this.versionInfo;
    switch (role) {
      case CURRENT:
        return info.getCurrentVersion();
      case FUTURE:
        return info.getFutureVersion();
      case BACKUP:
        int backupVersion = NON_EXISTING_VERSION;
        for (Integer version: ingestionTasksByVersion.keySet()) {
          if (version != info.getCurrentVersion() && version != info.getFutureVersion()) {
            if (backupVersion == NON_EXISTING_VERSION || version < backupVersion) {
              backupVersion = version;
            }
          }
        }
        return backupVersion;
      default:
        return NON_EXISTING_VERSION;
    }
  }

  private StoreIngestionTask getTaskForRole(VersionRole role) {
    int version = getVersionForRole(role);
    if (version == NON_EXISTING_VERSION) {
      return null;
    }
    return ingestionTasksByVersion.get(version);
  }

  // ASYNC_GAUGE callbacks

  private long getTaskErrorCountForRole(VersionRole role) {
    return IngestionStatsUtils.getIngestionTaskErroredGauge(getTaskForRole(role));
  }

  private long getPushTimeoutCountForRole(VersionRole role) {
    int version = getVersionForRole(role);
    if (version == NON_EXISTING_VERSION) {
      return 0;
    }
    return pushTimeoutByVersion.getOrDefault(version, 0);
  }

  private long getDiskQuotaUsedForRole(VersionRole role) {
    return (long) (IngestionStatsUtils.getStorageQuotaUsed(getTaskForRole(role)) * 100);
  }

  private long getIdleTimeForRole(VersionRole role) {
    int version = getVersionForRole(role);
    if (version == NON_EXISTING_VERSION) {
      return 0;
    }
    AtomicLong idleTime = idleTimeByVersion.get(version);
    return idleTime != null ? idleTime.get() : 0;
  }

  // Task management methods

  /**
   * Sets the StoreIngestionTask for a specific version.
   * This enables async gauge metrics to access task data.
   */
  public void setIngestionTask(int version, StoreIngestionTask task) {
    if (task != null) {
      ingestionTasksByVersion.put(version, task);
    }
  }

  /**
   * Removes the StoreIngestionTask and associated per-version state for a specific version.
   */
  public void removeIngestionTask(int version) {
    ingestionTasksByVersion.remove(version);
    pushTimeoutByVersion.remove(version);
    idleTimeByVersion.remove(version);
  }

  /**
   * Cleans up all per-version state for this store.
   * Call this when the store is being deleted.
   *
   * <p>Note: OTel instruments (counters, histograms, async gauges) are NOT deregistered here.
   * OpenTelemetry SDK does not support deregistering individual instruments from a Meter.
   * The instruments will remain registered but will report zero/stale values until the
   * MeterProvider is shut down.
   */
  public void close() {
    ingestionTasksByVersion.clear();
    pushTimeoutByVersion.clear();
    idleTimeByVersion.clear();
  }

  public void setIngestionTaskPushTimeoutGauge(int version, int value) {
    pushTimeoutByVersion.put(version, value);
  }

  public void recordIdleTime(int version, long idleTimeMs) {
    idleTimeByVersion.computeIfAbsent(version, k -> new AtomicLong(0)).set(idleTimeMs);
  }

  // Helper methods

  private MetricEntityStateOneEnum<VersionRole> createOneEnumMetric(MetricEntity metricEntity) {
    return MetricEntityStateOneEnum.create(metricEntity, otelRepository, baseDimensionsMap, VersionRole.class);
  }

  private <E extends Enum<E> & VeniceDimensionInterface> MetricEntityStateTwoEnums<VersionRole, E> createTwoEnumMetric(
      MetricEntity metricEntity,
      Class<E> enumClass) {
    return MetricEntityStateTwoEnums
        .create(metricEntity, otelRepository, baseDimensionsMap, VersionRole.class, enumClass);
  }

  public boolean emitOtelMetrics() {
    return emitOtelMetrics;
  }

  public void updateVersionInfo(int currentVersion, int futureVersion) {
    this.versionInfo = new VersionInfo(currentVersion, futureVersion);
  }

  @VisibleForTesting
  VersionInfo getVersionInfo() {
    return versionInfo;
  }

  static VersionRole classifyVersion(int version, VersionInfo versionInfo) {
    return OtelVersionedStatsUtils.classifyVersion(version, versionInfo);
  }

  // Recording methods

  public void recordSubscribePrepTime(int version, double latencyMs) {
    subscribePrepTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordIngestionTime(int version, double latencyMs) {
    ingestionTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordPreprocessingLeaderTime(int version, double latencyMs) {
    preprocessingLeaderTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordPreprocessingInternalTime(int version, double latencyMs) {
    preprocessingInternalTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordProducerTime(int version, double latencyMs) {
    producerTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordBatchProcessingRequestCount(int version, long value) {
    batchProcessingRequestCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordBatchProcessingRequestRecordCount(int version, long value) {
    batchProcessingRequestRecordCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordBatchProcessingRequestErrorCount(int version, long value) {
    batchProcessingRequestErrorCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordBatchProcessingRequestTime(int version, double latencyMs) {
    batchProcessingRequestTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordDcrTotalCount(int version, long value) {
    dcrTotalCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordDuplicateKeyUpdateCount(int version, long value) {
    duplicateKeyUpdateCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordRecordsConsumed(int version, ReplicaType replicaType, long value) {
    recordsConsumedMetric.record(value, classifyVersion(version, versionInfo), replicaType);
  }

  public void recordRecordsProduced(int version, ReplicaType replicaType, long value) {
    recordsProducedMetric.record(value, classifyVersion(version, versionInfo), replicaType);
  }

  public void recordBytesConsumed(int version, ReplicaType replicaType, long value) {
    bytesConsumedMetric.record(value, classifyVersion(version, versionInfo), replicaType);
  }

  public void recordBytesProduced(int version, ReplicaType replicaType, long value) {
    bytesProducedMetric.record(value, classifyVersion(version, versionInfo), replicaType);
  }

  public void recordProducerCallbackTime(int version, ReplicaType replicaType, double latencyMs) {
    producerCallbackTimeMetric.record(latencyMs, classifyVersion(version, versionInfo), replicaType);
  }

  public void recordDcrEventCount(int version, VeniceDCREvent event, long value) {
    dcrEventCountMetric.record(value, classifyVersion(version, versionInfo), event);
  }

  public void recordTimeBetweenComponents(
      int version,
      VeniceIngestionSourceComponent sourceComponent,
      VeniceIngestionDestinationComponent destComponent,
      double latencyMs) {
    timeBetweenComponentsMetric
        .record(latencyMs, classifyVersion(version, versionInfo), sourceComponent, destComponent);
  }

}

package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.CONSUMER_IDLE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DISK_QUOTA_USED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TASK_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DESTINATION_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SOURCE_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

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
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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

  /**
   * Package-private no-arg constructor for {@link NoOpIngestionOtelStats}.
   * Initializes all final fields to null/empty defaults. Safe because the no-op subclass
   * overrides every public method, so these null fields are never dereferenced.
   */
  IngestionOtelStats() {
    this.emitOtelMetrics = false;
    this.otelRepository = null;
    this.baseDimensionsMap = null;
    this.ingestionTasksByVersion = Collections.emptyMap();
    this.pushTimeoutByVersion = Collections.emptyMap();
    this.idleTimeByVersion = Collections.emptyMap();
    this.taskErrorCountByRole = null;
    this.pushTimeoutCountByRole = null;
    this.diskQuotaUsedByRole = null;
    this.consumerIdleTimeByRole = null;
    this.subscribePrepTimeMetric = null;
    this.ingestionTimeMetric = null;
    this.preprocessingLeaderTimeMetric = null;
    this.preprocessingInternalTimeMetric = null;
    this.producerTimeMetric = null;
    this.batchProcessingRequestCountMetric = null;
    this.batchProcessingRequestRecordCountMetric = null;
    this.batchProcessingRequestErrorCountMetric = null;
    this.batchProcessingRequestTimeMetric = null;
    this.dcrTotalCountMetric = null;
    this.duplicateKeyUpdateCountMetric = null;
    this.recordsConsumedMetric = null;
    this.recordsProducedMetric = null;
    this.bytesConsumedMetric = null;
    this.bytesProducedMetric = null;
    this.producerCallbackTimeMetric = null;
    this.dcrEventCountMetric = null;
    this.timeBetweenComponentsMetric = null;
  }

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

  // Fully-qualified name required: JDK 8 javac cannot resolve imported types in inner enum
  // implements clauses when the interface contains static methods (fixed in JDK 9+).
  public enum IngestionOtelMetricEntity implements com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface {
    INGESTION_TASK_ERROR_COUNT(
        "ingestion.task.error_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Count of ingestion tasks in error state", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_TASK_PUSH_TIMEOUT_COUNT(
        "ingestion.task.push_timeout_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Count of ingestion tasks timed out during push operation",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    DISK_QUOTA_USED(
        "ingestion.disk_quota.used", MetricType.ASYNC_GAUGE, MetricUnit.RATIO, "Disk quota used for the store version",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_RECORDS_CONSUMED(
        "ingestion.records.consumed", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Records consumed from remote/local topic",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
    ),

    INGESTION_RECORDS_PRODUCED(
        "ingestion.records.produced", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Records produced to local topic",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
    ),

    INGESTION_BYTES_CONSUMED(
        "ingestion.bytes.consumed", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.BYTES,
        "Bytes consumed from remote/local topic that are successfully processed excluding control/DIV messages",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
    ),

    INGESTION_BYTES_PRODUCED(
        "ingestion.bytes.produced", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.BYTES,
        "Bytes produced to local topic",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
    ),

    INGESTION_SUBSCRIBE_PREP_TIME(
        "ingestion.subscribe.prep.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Subscription preparation latency", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_TIME(
        "ingestion.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "End-to-end processing time from topic poll to storage write",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_PRODUCER_CALLBACK_TIME(
        "ingestion.producer.callback.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Producer callback latency (ack wait time)",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE)
    ),

    INGESTION_PREPROCESSING_LEADER_TIME(
        "ingestion.preprocessing.leader.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Leader-side preprocessing latency during ingestion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_PREPROCESSING_INTERNAL_TIME(
        "ingestion.preprocessing.internal.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Internal preprocessing latency during ingestion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    INGESTION_TIME_BETWEEN_COMPONENTS(
        "ingestion.time_between_components", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Ingestion latency between different components of the flow",
        setOf(
            VENICE_STORE_NAME,
            VENICE_CLUSTER_NAME,
            VENICE_VERSION_ROLE,
            VENICE_INGESTION_SOURCE_COMPONENT,
            VENICE_INGESTION_DESTINATION_COMPONENT)
    ),

    INGESTION_PRODUCER_TIME(
        "ingestion.producer.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Latency from leader producing to producer completion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    BATCH_PROCESSING_REQUEST_COUNT(
        "ingestion.batch_processing.request.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Count of batch processing requests during ingestion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    BATCH_PROCESSING_REQUEST_RECORD_COUNT(
        "ingestion.batch_processing.request.record.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER, "Total records across batch-processing requests",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    BATCH_PROCESSING_REQUEST_ERROR_COUNT(
        "ingestion.batch_processing.request.error_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER, "Count of failed batch processing requests during ingestion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    BATCH_PROCESSING_REQUEST_TIME(
        "ingestion.batch_processing.request.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Batch processing latency", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    DCR_EVENT_COUNT(
        "ingestion.dcr.event_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Count of DCR outcomes per event type (e.g., PUT, DELETE, UPDATE)",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_EVENT)
    ),

    DCR_TOTAL_COUNT(
        "ingestion.dcr.total_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Deterministic Conflict Resolution (DCR) total count",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    CONSUMER_IDLE_TIME(
        "ingestion.consumer.idle_time", MetricType.ASYNC_GAUGE, MetricUnit.MILLISECOND,
        "Time the ingestion consumer has been idle without polling records",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    DUPLICATE_KEY_UPDATE_COUNT(
        "ingestion.key.update.duplicate_count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
        "Count of duplicate-key updates during ingestion",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
    ),

    RT_RECORDS_CONSUMED(
        "ingestion.records.consumed_from_real_time_topic", MetricType.COUNTER, MetricUnit.NUMBER,
        "Records consumed from local/remote region real-time topics",
        setOf(
            VENICE_STORE_NAME,
            VENICE_CLUSTER_NAME,
            VENICE_VERSION_ROLE,
            VENICE_SOURCE_REGION,
            VENICE_DESTINATION_REGION,
            VENICE_REGION_LOCALITY)
    ),

    RT_BYTES_CONSUMED(
        "ingestion.bytes.consumed_from_real_time_topic", MetricType.COUNTER, MetricUnit.BYTES,
        "Bytes consumed from local/remote region real-time topics",
        setOf(
            VENICE_STORE_NAME,
            VENICE_CLUSTER_NAME,
            VENICE_VERSION_ROLE,
            VENICE_SOURCE_REGION,
            VENICE_DESTINATION_REGION,
            VENICE_REGION_LOCALITY)
    );

    private final MetricEntity metricEntity;

    IngestionOtelMetricEntity(
        String name,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(name, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }

}

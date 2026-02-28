package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BYTES_CONSUMED_AS_UNCOMPRESSED_SIZE;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.CHECKSUM_VERIFICATION_FAILURE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.CONSUMER_ACTION_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.CONSUMER_IDLE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.CONSUMER_QUEUE_PUT_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_LOOKUP_CACHE_HIT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_LOOKUP_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_MERGE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DISK_QUOTA_USED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_FAILURE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.LONG_RUNNING_TASK_CHECK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.PRODUCER_COMPRESS_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.PRODUCER_ENQUEUE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.PRODUCER_SYNCHRONIZE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RECORD_ASSEMBLED_SIZE;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RECORD_ASSEMBLED_SIZE_RATIO;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RECORD_KEY_SIZE;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RECORD_VALUE_SIZE;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RESUBSCRIPTION_FAILURE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RT_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RT_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.STORAGE_ENGINE_DELETE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.STORAGE_ENGINE_PUT_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.STORE_METADATA_INCONSISTENT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.UNEXPECTED_MESSAGE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.VIEW_WRITER_ACK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.VIEW_WRITER_PRODUCE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.WRITE_COMPUTE_CACHE_HIT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.WRITE_COMPUTE_TIME;
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
import com.linkedin.venice.stats.dimensions.VeniceDCROperation;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceIngestionDestinationComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionFailureReason;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceWriteComputeOperation;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
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

  // RT region metrics: keyed by sourceRegion string, following HeartbeatOtelStats.metricsByRegion pattern.
  // destRegion is always the local server's region (constant per process), stored in localRegionName.
  private final Map<String, MetricEntityStateTwoEnums<VersionRole, VeniceRegionLocality>> rtRecordsConsumedByRegion;
  private final Map<String, MetricEntityStateTwoEnums<VersionRole, VeniceRegionLocality>> rtBytesConsumedByRegion;
  private final String localRegionName;

  // --- HostLevelIngestionStats OTel metrics ---

  // Simple latency metrics (VersionRole only)
  private final MetricEntityStateOneEnum<VersionRole> consumerQueuePutTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> storageEnginePutTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> storageEngineDeleteTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> consumerActionTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> longRunningTaskCheckTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> viewWriterProduceTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> viewWriterAckTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> producerEnqueueTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> producerCompressTimeMetric;
  private final MetricEntityStateOneEnum<VersionRole> producerSynchronizeTimeMetric;

  // Latency metrics with 2nd enum dimension
  private final MetricEntityStateTwoEnums<VersionRole, VeniceWriteComputeOperation> writeComputeTimeMetric;
  private final MetricEntityStateTwoEnums<VersionRole, VeniceRecordType> dcrLookupTimeMetric;
  private final MetricEntityStateTwoEnums<VersionRole, VeniceDCROperation> dcrMergeTimeMetric;

  // Simple counter metrics (VersionRole only)
  private final MetricEntityStateOneEnum<VersionRole> unexpectedMessageCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> storeMetadataInconsistentCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> resubscriptionFailureCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> writeComputeCacheHitCountMetric;
  private final MetricEntityStateOneEnum<VersionRole> checksumVerificationFailureCountMetric;

  // Counter metrics with 2nd enum dimension
  private final MetricEntityStateTwoEnums<VersionRole, VeniceIngestionFailureReason> ingestionFailureCountMetric;
  private final MetricEntityStateTwoEnums<VersionRole, VeniceRecordType> dcrLookupCacheHitCountMetric;

  // Size/rate metrics
  private final MetricEntityStateOneEnum<VersionRole> bytesConsumedAsUncompressedSizeMetric;
  private final MetricEntityStateOneEnum<VersionRole> recordKeySizeMetric;
  private final MetricEntityStateOneEnum<VersionRole> recordValueSizeMetric;
  private final MetricEntityStateTwoEnums<VersionRole, VeniceRecordType> recordAssembledSizeMetric;
  private final MetricEntityStateOneEnum<VersionRole> recordAssembledSizeRatioMetric;

  // Async gauge metric
  private final AsyncMetricEntityStateOneEnum<VersionRole> ingestionTaskCountByRole;

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
    this.rtRecordsConsumedByRegion = Collections.emptyMap();
    this.rtBytesConsumedByRegion = Collections.emptyMap();
    this.localRegionName = null;
    this.consumerQueuePutTimeMetric = null;
    this.storageEnginePutTimeMetric = null;
    this.storageEngineDeleteTimeMetric = null;
    this.consumerActionTimeMetric = null;
    this.longRunningTaskCheckTimeMetric = null;
    this.viewWriterProduceTimeMetric = null;
    this.viewWriterAckTimeMetric = null;
    this.producerEnqueueTimeMetric = null;
    this.producerCompressTimeMetric = null;
    this.producerSynchronizeTimeMetric = null;
    this.writeComputeTimeMetric = null;
    this.dcrLookupTimeMetric = null;
    this.dcrMergeTimeMetric = null;
    this.unexpectedMessageCountMetric = null;
    this.storeMetadataInconsistentCountMetric = null;
    this.resubscriptionFailureCountMetric = null;
    this.writeComputeCacheHitCountMetric = null;
    this.checksumVerificationFailureCountMetric = null;
    this.ingestionFailureCountMetric = null;
    this.dcrLookupCacheHitCountMetric = null;
    this.bytesConsumedAsUncompressedSizeMetric = null;
    this.recordKeySizeMetric = null;
    this.recordValueSizeMetric = null;
    this.recordAssembledSizeMetric = null;
    this.recordAssembledSizeRatioMetric = null;
    this.ingestionTaskCountByRole = null;
  }

  public IngestionOtelStats(
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      String localRegionName,
      boolean ingestionOtelStatsEnabled) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setOtelEnabledOverride(ingestionOtelStatsEnabled)
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

    // Initialize RT region metric maps
    this.rtRecordsConsumedByRegion = new VeniceConcurrentHashMap<>();
    this.rtBytesConsumedByRegion = new VeniceConcurrentHashMap<>();
    this.localRegionName = localRegionName;

    // Initialize HostLevelIngestionStats OTel metrics - simple latency
    consumerQueuePutTimeMetric = createOneEnumMetric(CONSUMER_QUEUE_PUT_TIME.getMetricEntity());
    storageEnginePutTimeMetric = createOneEnumMetric(STORAGE_ENGINE_PUT_TIME.getMetricEntity());
    storageEngineDeleteTimeMetric = createOneEnumMetric(STORAGE_ENGINE_DELETE_TIME.getMetricEntity());
    consumerActionTimeMetric = createOneEnumMetric(CONSUMER_ACTION_TIME.getMetricEntity());
    longRunningTaskCheckTimeMetric = createOneEnumMetric(LONG_RUNNING_TASK_CHECK_TIME.getMetricEntity());
    viewWriterProduceTimeMetric = createOneEnumMetric(VIEW_WRITER_PRODUCE_TIME.getMetricEntity());
    viewWriterAckTimeMetric = createOneEnumMetric(VIEW_WRITER_ACK_TIME.getMetricEntity());
    producerEnqueueTimeMetric = createOneEnumMetric(PRODUCER_ENQUEUE_TIME.getMetricEntity());
    producerCompressTimeMetric = createOneEnumMetric(PRODUCER_COMPRESS_TIME.getMetricEntity());
    producerSynchronizeTimeMetric = createOneEnumMetric(PRODUCER_SYNCHRONIZE_TIME.getMetricEntity());

    // Initialize HostLevelIngestionStats OTel metrics - latency with 2nd enum dimension
    writeComputeTimeMetric =
        createTwoEnumMetric(WRITE_COMPUTE_TIME.getMetricEntity(), VeniceWriteComputeOperation.class);
    dcrLookupTimeMetric = createTwoEnumMetric(DCR_LOOKUP_TIME.getMetricEntity(), VeniceRecordType.class);
    dcrMergeTimeMetric = createTwoEnumMetric(DCR_MERGE_TIME.getMetricEntity(), VeniceDCROperation.class);

    // Initialize HostLevelIngestionStats OTel metrics - simple counters
    unexpectedMessageCountMetric = createOneEnumMetric(UNEXPECTED_MESSAGE_COUNT.getMetricEntity());
    storeMetadataInconsistentCountMetric = createOneEnumMetric(STORE_METADATA_INCONSISTENT_COUNT.getMetricEntity());
    resubscriptionFailureCountMetric = createOneEnumMetric(RESUBSCRIPTION_FAILURE_COUNT.getMetricEntity());
    writeComputeCacheHitCountMetric = createOneEnumMetric(WRITE_COMPUTE_CACHE_HIT_COUNT.getMetricEntity());
    checksumVerificationFailureCountMetric = createOneEnumMetric(CHECKSUM_VERIFICATION_FAILURE_COUNT.getMetricEntity());

    // Initialize HostLevelIngestionStats OTel metrics - counters with 2nd enum dimension
    ingestionFailureCountMetric =
        createTwoEnumMetric(INGESTION_FAILURE_COUNT.getMetricEntity(), VeniceIngestionFailureReason.class);
    dcrLookupCacheHitCountMetric =
        createTwoEnumMetric(DCR_LOOKUP_CACHE_HIT_COUNT.getMetricEntity(), VeniceRecordType.class);

    // Initialize HostLevelIngestionStats OTel metrics - size/rate
    bytesConsumedAsUncompressedSizeMetric = createOneEnumMetric(BYTES_CONSUMED_AS_UNCOMPRESSED_SIZE.getMetricEntity());
    recordKeySizeMetric = createOneEnumMetric(RECORD_KEY_SIZE.getMetricEntity());
    recordValueSizeMetric = createOneEnumMetric(RECORD_VALUE_SIZE.getMetricEntity());
    recordAssembledSizeMetric = createTwoEnumMetric(RECORD_ASSEMBLED_SIZE.getMetricEntity(), VeniceRecordType.class);
    recordAssembledSizeRatioMetric = createOneEnumMetric(RECORD_ASSEMBLED_SIZE_RATIO.getMetricEntity());

    // Initialize HostLevelIngestionStats OTel metrics - async gauge
    ingestionTaskCountByRole = AsyncMetricEntityStateOneEnum.create(
        INGESTION_TASK_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VersionRole.class,
        role -> () -> getTaskCountForRole(role));
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
    rtRecordsConsumedByRegion.clear();
    rtBytesConsumedByRegion.clear();
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

  // RT region metric helpers

  private MetricEntityStateTwoEnums<VersionRole, VeniceRegionLocality> getOrCreateRtRecordsMetric(String sourceRegion) {
    return rtRecordsConsumedByRegion.computeIfAbsent(sourceRegion, src -> {
      Map<VeniceMetricsDimensions, String> dims = new HashMap<>(baseDimensionsMap);
      dims.put(VeniceMetricsDimensions.VENICE_SOURCE_REGION, src);
      dims.put(VeniceMetricsDimensions.VENICE_DESTINATION_REGION, localRegionName);
      return MetricEntityStateTwoEnums.create(
          RT_RECORDS_CONSUMED.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          VeniceRegionLocality.class);
    });
  }

  private MetricEntityStateTwoEnums<VersionRole, VeniceRegionLocality> getOrCreateRtBytesMetric(String sourceRegion) {
    return rtBytesConsumedByRegion.computeIfAbsent(sourceRegion, src -> {
      Map<VeniceMetricsDimensions, String> dims = new HashMap<>(baseDimensionsMap);
      dims.put(VeniceMetricsDimensions.VENICE_SOURCE_REGION, src);
      dims.put(VeniceMetricsDimensions.VENICE_DESTINATION_REGION, localRegionName);
      return MetricEntityStateTwoEnums.create(
          RT_BYTES_CONSUMED.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          VeniceRegionLocality.class);
    });
  }

  // RT region recording methods

  public void recordRtRecordsConsumed(
      int version,
      String sourceRegion,
      VeniceRegionLocality regionLocality,
      long count) {
    if (!emitOtelMetrics) {
      return;
    }
    getOrCreateRtRecordsMetric(sourceRegion).record(count, classifyVersion(version, versionInfo), regionLocality);
  }

  public void recordRtBytesConsumed(int version, String sourceRegion, VeniceRegionLocality regionLocality, long bytes) {
    if (!emitOtelMetrics) {
      return;
    }
    getOrCreateRtBytesMetric(sourceRegion).record(bytes, classifyVersion(version, versionInfo), regionLocality);
  }

  // HostLevelIngestionStats OTel recording methods

  // Simple latency methods

  public void recordConsumerQueuePutTime(int version, double latencyMs) {
    consumerQueuePutTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordStorageEnginePutTime(int version, double latencyMs) {
    storageEnginePutTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordStorageEngineDeleteTime(int version, double latencyMs) {
    storageEngineDeleteTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordConsumerActionTime(int version, double latencyMs) {
    consumerActionTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordLongRunningTaskCheckTime(int version, double latencyMs) {
    longRunningTaskCheckTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordViewWriterProduceTime(int version, double latencyMs) {
    viewWriterProduceTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordViewWriterAckTime(int version, double latencyMs) {
    viewWriterAckTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordProducerEnqueueTime(int version, double latencyMs) {
    producerEnqueueTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordProducerCompressTime(int version, double latencyMs) {
    producerCompressTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  public void recordProducerSynchronizeTime(int version, double latencyMs) {
    producerSynchronizeTimeMetric.record(latencyMs, classifyVersion(version, versionInfo));
  }

  // Latency methods with 2nd enum dimension

  public void recordWriteComputeTime(int version, VeniceWriteComputeOperation op, double latencyMs) {
    writeComputeTimeMetric.record(latencyMs, classifyVersion(version, versionInfo), op);
  }

  public void recordDcrLookupTime(int version, VeniceRecordType recordType, double latencyMs) {
    dcrLookupTimeMetric.record(latencyMs, classifyVersion(version, versionInfo), recordType);
  }

  public void recordDcrMergeTime(int version, VeniceDCROperation op, double latencyMs) {
    dcrMergeTimeMetric.record(latencyMs, classifyVersion(version, versionInfo), op);
  }

  // Simple count methods

  public void recordUnexpectedMessageCount(int version, long value) {
    unexpectedMessageCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordStoreMetadataInconsistentCount(int version, long value) {
    storeMetadataInconsistentCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordResubscriptionFailureCount(int version, long value) {
    resubscriptionFailureCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordWriteComputeCacheHitCount(int version, long value) {
    writeComputeCacheHitCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  public void recordChecksumVerificationFailureCount(int version, long value) {
    checksumVerificationFailureCountMetric.record(value, classifyVersion(version, versionInfo));
  }

  // Count methods with 2nd enum dimension

  public void recordIngestionFailureCount(int version, VeniceIngestionFailureReason reason, long value) {
    ingestionFailureCountMetric.record(value, classifyVersion(version, versionInfo), reason);
  }

  public void recordDcrLookupCacheHitCount(int version, VeniceRecordType recordType, long value) {
    dcrLookupCacheHitCountMetric.record(value, classifyVersion(version, versionInfo), recordType);
  }

  // Size/rate methods

  public void recordBytesConsumedAsUncompressedSize(int version, long bytes) {
    bytesConsumedAsUncompressedSizeMetric.record(bytes, classifyVersion(version, versionInfo));
  }

  public void recordKeySize(int version, long bytes) {
    recordKeySizeMetric.record(bytes, classifyVersion(version, versionInfo));
  }

  public void recordValueSize(int version, long bytes) {
    recordValueSizeMetric.record(bytes, classifyVersion(version, versionInfo));
  }

  public void recordAssembledSize(int version, VeniceRecordType recordType, long bytes) {
    recordAssembledSizeMetric.record(bytes, classifyVersion(version, versionInfo), recordType);
  }

  public void recordAssembledSizeRatio(int version, double ratio) {
    recordAssembledSizeRatioMetric.record(ratio, classifyVersion(version, versionInfo));
  }

  // Async gauge callback

  private long getTaskCountForRole(VersionRole role) {
    int version = getVersionForRole(role);
    if (version == NON_EXISTING_VERSION) {
      return 0;
    }
    return ingestionTasksByVersion.containsKey(version) ? 1 : 0;
  }

}

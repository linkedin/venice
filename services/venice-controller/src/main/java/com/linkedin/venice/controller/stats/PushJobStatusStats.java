package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.stats.ControllerStatsDimensionUtils.dimensionMapBuilder;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VenicePushJobDataWriterSink;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.CountSinceLastMeasurement;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PushJobStatusStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(PushJobStatusStats.class);
  private final MetricEntityStateGeneric batchPushSuccessMetric;
  private final MetricEntityStateGeneric batchPushFailureDueToUserErrorMetric;
  private final MetricEntityStateGeneric batchPushFailureDueToNonUserErrorMetric;
  private final MetricEntityStateGeneric incrementalPushSuccessMetric;
  private final MetricEntityStateGeneric incrementalPushFailureDueToUserErrorMetric;
  private final MetricEntityStateGeneric incrementalPushFailureDueToNonUserErrorMetric;
  /**
   * Duration a push's data-writer tasks spent writing to the external storage sink and to Venice. Both share
   * one OTel metric entity and are told apart by the {@link VenicePushJobDataWriterSink} dimension, while
   * Tehuti gets one sensor per sink because Tehuti sensors carry no dimensions.
   */
  private final MetricEntityStateGeneric externalStorageWriteTimeMetric;
  private final MetricEntityStateGeneric veniceWriteTimeMetric;
  /** Alertable per-region signal that a push gave up writing to external storage. */
  private final MetricEntityStateGeneric externalStorageWriteFailureMetric;

  public PushJobStatusStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    batchPushSuccessMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushSuccessMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    externalStorageWriteTimeMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.PUSH_JOB_EXTERNAL_STORAGE_WRITE_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap);

    veniceWriteTimeMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.PUSH_JOB_VENICE_WRITE_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap);

    externalStorageWriteFailureMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);
  }

  public void recordBatchPushSuccessSensor(String storeName) {
    batchPushSuccessMetric.record(1, pushJobDimensions(storeName, PushType.BATCH, VenicePushJobStatus.SUCCESS));
  }

  public void recordBatchPushFailureDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToUserErrorMetric
        .record(1, pushJobDimensions(storeName, PushType.BATCH, VenicePushJobStatus.USER_ERROR));
  }

  public void recordBatchPushFailureNotDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToNonUserErrorMetric
        .record(1, pushJobDimensions(storeName, PushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR));
  }

  public void recordIncrementalPushSuccessSensor(String storeName) {
    incrementalPushSuccessMetric
        .record(1, pushJobDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.SUCCESS));
  }

  public void recordIncrementalPushFailureDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToUserErrorMetric
        .record(1, pushJobDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.USER_ERROR));
  }

  public void recordIncrementalPushFailureNotDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToNonUserErrorMetric
        .record(1, pushJobDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.SYSTEM_ERROR));
  }

  /**
   * Record how long a terminal push's data-writer tasks spent writing to one of the two sinks.
   *
   * <p>{@code timeMs} is the sum of the per-task wall-clock durations reported by the push job, not the
   * push's own wall-clock duration. Negative values mean the push did not report the duration (older push job
   * or no dual write configured) and are dropped rather than recorded as a bogus observation. Callers are
   * responsible for invoking this at most once per push per sink; see the dedup in
   * {@code VeniceHelixAdmin#emitPushJobStatusMetrics}.
   */
  public void recordDataWriterSinkWriteTime(
      String storeName,
      PushType pushType,
      VenicePushJobDataWriterSink sink,
      long timeMs) {
    if (timeMs < 0) {
      return;
    }
    Map<VeniceMetricsDimensions, String> dimensions = dimensionMapBuilder().store(storeName)
        .add(VENICE_PUSH_JOB_TYPE, pushType.getDimensionValue())
        .add(VENICE_PUSH_JOB_DATA_WRITER_SINK, sink.getDimensionValue())
        .build();
    switch (sink) {
      case EXTERNAL_STORAGE:
        externalStorageWriteTimeMetric.record(timeMs, dimensions);
        break;
      case VENICE:
        veniceWriteTimeMetric.record(timeMs, dimensions);
        break;
      default:
        // Metrics recording should never fail the caller; a future sink value that isn't wired up here should
        // be dropped with a loud warning instead of throwing on a hot metrics path.
        LOGGER.warn("Unsupported data writer sink: {}, dropping data writer sink write time observation", sink);
    }
  }

  /**
   * Record that a push exhausted its external-storage write retries in {@code regionName} and that the region's
   * version storage mode was consequently failed open to {@code INTERNAL}. This is a counter rather than a
   * duration: it exists to be alerted on, since the push itself still succeeds and would otherwise look healthy.
   *
   * <p>Emitted by the controller of the affected region, once per region per accepted downgrade. Push type is
   * deliberately not a dimension because the controller applying the downgrade does not know it, and neither push
   * id nor version number are dimensions because they are unbounded.
   */
  public void recordExternalStorageWriteFailure(String storeName, String regionName) {
    externalStorageWriteFailureMetric
        .record(1, dimensionMapBuilder().store(storeName).add(VENICE_REGION_NAME, regionName).build());
  }

  private static Map<VeniceMetricsDimensions, String> pushJobDimensions(
      String storeName,
      PushType pushType,
      VenicePushJobStatus status) {
    return dimensionMapBuilder().store(storeName)
        .add(VENICE_PUSH_JOB_TYPE, pushType.getDimensionValue())
        .add(VENICE_PUSH_JOB_STATUS, status.getDimensionValue())
        .build();
  }

  enum PushJobTehutiMetricNameEnum implements TehutiMetricNameEnum {
    BATCH_PUSH_JOB_SUCCESS, BATCH_PUSH_JOB_FAILED_USER_ERROR, BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
    INCREMENTAL_PUSH_JOB_SUCCESS, INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR, INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
    PUSH_JOB_EXTERNAL_STORAGE_WRITE_TIME, PUSH_JOB_VENICE_WRITE_TIME, PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE
  }

  public enum PushJobOtelMetricEntity implements ModuleMetricEntityInterface {
    /** PushJobStatusStats: Push job completions */
    PUSH_JOB_COUNT(
        "push_job.count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Push job completions, differentiated by push type and status",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_STATUS)
    ),

    /**
     * PushJobStatusStats: Time a terminal push's data writer tasks spent writing to each sink. This is the
     * sum of the per-task wall-clock durations reported by the push job, so it is not bounded by the push's
     * own duration; a push with N parallel tasks can report up to N times its wall-clock time. One
     * observation is recorded per terminal push per sink.
     */
    PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME(
        "push_job.data_writer.sink_write_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
        "Summed data writer task duration spent writing to a push job sink, differentiated by push type and sink",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_DATA_WRITER_SINK)
    ),

    /**
     * PushJobStatusStats: Pushes that gave up writing to external storage in a region. The push still succeeds,
     * so this counter is the alertable signal that the region's version holds no external-storage copy. Counted
     * once per region per accepted downgrade by the controller of the affected region.
     */
    PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT(
        "push_job.external_storage_write_failure.count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Pushes that exhausted external storage write retries and failed the region's version storage mode open to internal, differentiated by region",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_REGION_NAME)
    );

    private final MetricEntity metricEntity;

    PushJobOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}

package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.StoreRepushTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * This class is used to track the metrics for log compaction.
 */
public class LogCompactionStats extends AbstractVeniceStats {
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** metrics */
  private final MetricEntityStateGeneric repushCallCountMetric;
  private final MetricEntityStateGeneric compactionEligibleMetric;
  private final MetricEntityStateGeneric storeNominatedForCompactionCountMetric;
  private final MetricEntityStateGeneric storeCompactionTriggeredCountMetric;

  public LogCompactionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, "LogCompactionStats");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            // set all base dimensions for this stats class and build
            .setClusterName(clusterName)
            .build();

    this.emitOpenTelemetryMetrics = otelData.emitOpenTelemetryMetrics();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();

    repushCallCountMetric = MetricEntityStateGeneric.create(
        LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.REPUSH_CALL_COUNT,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    compactionEligibleMetric = MetricEntityStateGeneric.create(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.COMPACTION_ELIGIBLE_STATE,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap);

    storeNominatedForCompactionCountMetric = MetricEntityStateGeneric.create(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_NOMINATED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.STORE_NOMINATED_FOR_COMPACTION_COUNT,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    storeCompactionTriggeredCountMetric = MetricEntityStateGeneric.create(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.STORE_COMPACTION_TRIGGERED_COUNT,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);
  }

  public void recordRepushStoreCall(
      String storeName,
      StoreRepushTriggerSource triggerSource,
      VeniceResponseStatusCategory executionStatus) {
    repushCallCountMetric.record(
        1,
        getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName)
            .put(VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE, triggerSource.getDimensionValue())
            .put(VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY, executionStatus.getDimensionValue())
            .build());
    if (triggerSource == StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION) {
      storeCompactionTriggeredCountMetric.record(
          1,
          getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName)
              .put(VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY, executionStatus.getDimensionValue())
              .build());
    }
  }

  public void setCompactionEligible(String storeName) {
    compactionEligibleMetric
        .record(1, getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName).build());
  }

  public void setCompactionComplete(String storeName) {
    compactionEligibleMetric
        .record(0, getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName).build());
  }

  public void recordStoreNominatedForCompactionCount(String storeName) {
    storeNominatedForCompactionCountMetric
        .record(1, getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName).build());
  }

  private ImmutableMap.Builder<VeniceMetricsDimensions, String> getDimensionsBuilder() {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = ImmutableMap.builder();

    if (baseDimensionsMap != null) {
      builder.putAll(baseDimensionsMap);
    }

    return builder;
  }

  enum ControllerTehutiMetricNameEnum implements TehutiMetricNameEnum {
    /** for {@link LogCompactionOtelMetricEntity#STORE_REPUSH_CALL_COUNT} */
    REPUSH_CALL_COUNT,
    /** for {@link LogCompactionOtelMetricEntity#STORE_COMPACTION_ELIGIBLE_STATE} */
    COMPACTION_ELIGIBLE_STATE,
    /** for {@link LogCompactionOtelMetricEntity#STORE_COMPACTION_NOMINATED_COUNT} */
    STORE_NOMINATED_FOR_COMPACTION_COUNT,
    /** for {@link LogCompactionOtelMetricEntity#STORE_COMPACTION_TRIGGERED_COUNT} */
    STORE_COMPACTION_TRIGGERED_COUNT;

    private final String metricName;

    ControllerTehutiMetricNameEnum() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  public enum LogCompactionOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Count of all requests to repush a store */
    STORE_REPUSH_CALL_COUNT(
        "store.repush.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all requests to repush a store",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME, STORE_REPUSH_TRIGGER_SOURCE)
    ),
    /** Count of stores nominated for scheduled compaction */
    STORE_COMPACTION_NOMINATED_COUNT(
        "store.compaction.nominated_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of stores nominated for scheduled compaction", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
    ),
    /**
     * Track the state from the time a store is nominated for compaction to the
     * time the repush is completed to finish compaction. stays 1 after nomination
     * and becomes 0 when the compaction is compacted
     */
    STORE_COMPACTION_ELIGIBLE_STATE(
        "store.compaction.eligible_state", MetricType.GAUGE, MetricUnit.NUMBER,
        "Track the state from the time a store is nominated for compaction to the time the repush is completed",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
    ),
    /** Count of log compaction repush triggered for a store after it becomes eligible */
    STORE_COMPACTION_TRIGGERED_COUNT(
        "store.compaction.triggered_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of log compaction repush triggered for a store after it becomes eligible",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    LogCompactionOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}

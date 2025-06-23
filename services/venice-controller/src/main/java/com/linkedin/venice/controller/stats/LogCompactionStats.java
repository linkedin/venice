package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.stats.ControllerMetricEntity.REPUSH_STORE_ENDPOINT_CALL_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class LogCompactionStats extends AbstractVeniceStats {
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Attributes baseAttributes;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** metrics */
  private final MetricEntityStateGeneric repushStoreCallCountMetric;
  private final MetricEntityStateGeneric storeNominatedForScheduledCompactionMetric;

  public LogCompactionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, "LogCompactionStats");
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      if (emitOpenTelemetryMetrics) {
        this.otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        this.baseDimensionsMap = new HashMap<>();
        this.baseDimensionsMap.put(VENICE_CLUSTER_NAME, clusterName);
        AttributesBuilder baseAttributesBuilder = Attributes.builder();
        baseAttributesBuilder.put(this.otelRepository.getDimensionName(VENICE_CLUSTER_NAME), clusterName);
        this.baseAttributes = baseAttributesBuilder.build();
      } else {
        this.otelRepository = null;
        this.baseAttributes = null;
        this.baseDimensionsMap = null;
      }
    } else {
      this.emitOpenTelemetryMetrics = false;
      this.otelRepository = null;
      this.baseAttributes = null;
      this.baseDimensionsMap = null;
    }

    repushStoreCallCountMetric = MetricEntityStateGeneric.create(
        REPUSH_STORE_ENDPOINT_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.REPUSH_STORE_ENDPOINT_CALL_COUNT,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    storeNominatedForScheduledCompactionMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.STORE_NOMINATED_FOR_SCHEDULED_COMPACTION.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.STORE_NOMINATED_FOR_SCHEDULED_COMPACTION,
        Collections.singletonList(new Count()),
        baseDimensionsMap);
  }

  public void recordRepushStoreCall(
      String storeName,
      RepushStoreTriggerSource triggerSource,
      VeniceResponseStatusCategory executionStatus) {
    repushStoreCallCountMetric.record(1, new HashMap<VeniceMetricsDimensions, String>(baseDimensionsMap) {
      {
        put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName);
        put(VeniceMetricsDimensions.REPUSH_STORE_TRIGGER_SOURCE, triggerSource.getDimensionValue());
        put(VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY, executionStatus.getDimensionValue());
      }
    });
  }

  public void recordStoreNominatedForScheduledCompaction(String storeName, int storeCurrentVersionNumber) {
    storeNominatedForScheduledCompactionMetric
        .record(1, new HashMap<VeniceMetricsDimensions, String>(baseDimensionsMap) {
          {
            put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName);
          }
        });
  }

  public void recordStoreRepushedForScheduledCompaction(String storeName, int storeCurrentVersionNumber) {
    storeNominatedForScheduledCompactionMetric
        .record(0, new HashMap<VeniceMetricsDimensions, String>(baseDimensionsMap) {
          {
            put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName);
          }
        });
  }

  enum ControllerTehutiMetricNameEnum implements TehutiMetricNameEnum {
    /** for {@link ControllerMetricEntity#REPUSH_STORE_ENDPOINT_CALL_COUNT} */
    REPUSH_STORE_ENDPOINT_CALL_COUNT, STORE_NOMINATED_FOR_SCHEDULED_COMPACTION;

    private final String metricName;

    ControllerTehutiMetricNameEnum() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}

package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ServerMetadataServiceStats extends AbstractVeniceStats {
  /**
   * Measure the number of time request based metadata endpoint was invoked
   */
  private final Sensor requestBasedMetadataInvokeCount;

  /**
   * Measure the number of time request based metadata endpoint failed to respond
   */
  private final Sensor requestBasedMetadataFailureCount;

  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final VeniceConcurrentHashMap<String, MetricEntityStateOneEnum<VeniceResponseStatusCategory>> perStoreMetrics =
      new VeniceConcurrentHashMap<>();

  public ServerMetadataServiceStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, "ServerMetadataStats");

    this.requestBasedMetadataInvokeCount = registerSensorIfAbsent("request_based_metadata_invoke_count", new Rate());
    this.requestBasedMetadataFailureCount = registerSensorIfAbsent("request_based_metadata_failure_count", new Rate());

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.emitOtelMetrics = otelData.emitOpenTelemetryMetrics();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  private MetricEntityStateOneEnum<VeniceResponseStatusCategory> getStoreMetrics(String storeName) {
    return perStoreMetrics.computeIfAbsent(storeName, k -> {
      Map<VeniceMetricsDimensions, String> dimensionsMap = new HashMap<>(baseDimensionsMap);
      dimensionsMap.put(VENICE_STORE_NAME, k);
      return MetricEntityStateOneEnum.create(
          ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricEntity(),
          otelRepository,
          dimensionsMap,
          VeniceResponseStatusCategory.class);
    });
  }

  public void recordRequestBasedMetadataInvokeCount() {
    requestBasedMetadataInvokeCount.record();
  }

  public void recordRequestBasedMetadataSuccessCount(String storeName) {
    if (emitOtelMetrics) {
      getStoreMetrics(storeName).record(1, VeniceResponseStatusCategory.SUCCESS);
    }
  }

  public void recordRequestBasedMetadataFailureCount(String storeName) {
    requestBasedMetadataFailureCount.record();
    if (emitOtelMetrics) {
      getStoreMetrics(storeName).record(1, VeniceResponseStatusCategory.FAIL);
    }
  }

  public enum ServerMetadataOtelMetricEntity implements ModuleMetricEntityInterface {
    METADATA_REQUEST_COUNT(
        "metadata.request_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Request-based metadata invocation count by outcome",
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;

    ServerMetadataOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}

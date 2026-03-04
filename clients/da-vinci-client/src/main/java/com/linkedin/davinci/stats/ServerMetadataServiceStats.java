package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.HashMap;
import java.util.Map;


/**
 * Stats for server metadata service endpoints.
 *
 * <p>Recording design:
 * <ul>
 *   <li>{@code invoke_count}: Tehuti-only (total requests). OTel invoke count is derivable as success + failure.</li>
 *   <li>{@code success_count}: OTel-only (per-store, per-cluster).</li>
 *   <li>{@code failure_count}: Tehuti + OTel (dual-recorded).</li>
 * </ul>
 */
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
  /**
   * Per-store OTel metrics. When the requested store does not exist (e.g., {@link VeniceNoStoreException}),
   * {@link OpenTelemetryMetricsSetup#UNKNOWN_STORE_NAME} is used as a sentinel so that all unknown-store failures
   * share one metric entry. Cardinality is bounded by stores deployed to this server + the sentinel.
   */
  private final Map<String, MetricEntityStateOneEnum<VeniceResponseStatusCategory>> perStoreMetrics =
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

  // Uses the 4-arg (OTel-only) create overload intentionally: the 7-arg overload would bind
  // Tehuti recording to every record() call, but we only want Tehuti on failure, not success.
  // Tehuti and OTel are therefore recorded in separate steps in recordRequestBasedMetadataFailureCount.
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

  /**
   * Records failure. Uses {@link OpenTelemetryMetricsSetup#UNKNOWN_STORE_NAME} when the exception
   * indicates the store doesn't exist, to bound per-store OTel cardinality from arbitrary
   * request-provided names.
   */
  public void recordRequestBasedMetadataFailureCount(String storeName, Exception e) {
    requestBasedMetadataFailureCount.record();
    if (emitOtelMetrics) {
      String metricStoreName =
          (e instanceof VeniceNoStoreException) ? OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME : storeName;
      getStoreMetrics(metricStoreName).record(1, VeniceResponseStatusCategory.FAIL);
    }
  }

}

package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


/**
 * Utility class to set up OpenTelemetry for different stats classes:
 * This takes in a {@link MetricsRepository} and optional base dimension values,
 * and determines if OpenTelemetry metrics should be emitted based on the repository type
 * and configuration. It also constructs base dimension maps and OpenTelemetry Attributes
 * that can be reused when recording metrics.
 */
public class OpenTelemetryMetricsSetup {
  /**
   * Result object containing the setup OpenTelemetry components
   */
  public static class OpenTelemetryMetricsSetupInfo {
    private final boolean emitOpenTelemetryMetrics;
    private final VeniceOpenTelemetryMetricsRepository otelRepository;
    private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
    private final Attributes baseAttributes;

    public OpenTelemetryMetricsSetupInfo(
        boolean emitOpenTelemetryMetrics,
        VeniceOpenTelemetryMetricsRepository otelRepository,
        Map<VeniceMetricsDimensions, String> baseDimensionsMap,
        Attributes baseAttributes) {
      this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
      this.otelRepository = otelRepository;
      this.baseDimensionsMap = baseDimensionsMap;
      this.baseAttributes = baseAttributes;
    }

    public boolean emitOpenTelemetryMetrics() {
      return emitOpenTelemetryMetrics;
    }

    public VeniceOpenTelemetryMetricsRepository getOtelRepository() {
      return otelRepository;
    }

    public Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
      return baseDimensionsMap;
    }

    public Attributes getBaseAttributes() {
      return baseAttributes;
    }
  }

  public static class Builder {
    /**
     * Sentinel value indicating that the Helix group ID is unassigned.
     * -1 is chosen because valid group IDs are non-negative integers.
     * Negative group IDs, including -1, do not have any special meaning in the system
     * other than representing an unassigned state.
     */
    private static final int UNASSIGNED_HELIX_GROUP_ID = -1;

    private final MetricsRepository metricsRepository;
    private String storeName;
    private RequestType requestType;
    private Boolean isTotalStats;
    private Boolean otelEnabledOverride;
    private String clusterName;
    private String routeName;
    private RequestRetryType requestRetryType;
    private int helixGroupId = UNASSIGNED_HELIX_GROUP_ID;

    public Builder(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
    }

    /**
     * Set the store name dimension.
     */
    public Builder setStoreName(String storeName) {
      this.storeName = storeName;
      return this;
    }

    /**
     * Set the request type dimension.
     */
    public Builder setRequestType(RequestType requestType) {
      this.requestType = requestType;
      return this;
    }

    /**
     * Set whether this is for total stats (affects whether OTel metrics are emitted).
     */
    public Builder isTotalStats(boolean isTotalStats) {
      this.isTotalStats = isTotalStats;
      return this;
    }

    /**
     * Per-category gate for OTel metrics, applied on top of the global OTel config.
     * OTel metrics are emitted only when <b>both</b> the global config and this override are
     * enabled. Setting this to {@code false} disables OTel for the stats class being built,
     * even if the global config is enabled. Setting it to {@code true} has no effect if the
     * global config is disabled — it cannot re-enable OTel.
     */
    public Builder setOtelEnabledOverride(boolean otelEnabledOverride) {
      this.otelEnabledOverride = otelEnabledOverride;
      return this;
    }

    /**
     * Set the cluster name dimension.
     */
    public Builder setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    /**
     * Set the route name dimension.
     */
    public Builder setRouteName(String routeName) {
      this.routeName = routeName;
      return this;
    }

    /**
     * Set the request retry type dimension.
     */
    public Builder setRequestRetryType(RequestRetryType requestRetryType) {
      this.requestRetryType = requestRetryType;
      return this;
    }

    /**
     * Set the Helix group ID dimension.
     */
    public Builder setHelixGroupId(int helixGroupId) {
      this.helixGroupId = helixGroupId;
      return this;
    }

    /**
     * Build: setup base dimensions and attributes, and determine if OTel metrics should be emitted.
     * @return OpenTelemetryMetricsSetupInfo containing this information
     */
    public OpenTelemetryMetricsSetupInfo build() {
      if (!(metricsRepository instanceof VeniceMetricsRepository)) {
        return buildOtelDisabled();
      }

      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();

      // Check if OTel metrics should be emitted
      boolean emitOtel = veniceMetricsConfig.emitOtelMetrics();
      if (isTotalStats != null && isTotalStats) {
        emitOtel = false; // Don't emit OTel metrics for total stats
      }
      if (otelEnabledOverride != null && !otelEnabledOverride) {
        emitOtel = false;
      }

      if (!emitOtel) {
        return buildOtelDisabled();
      }

      VeniceOpenTelemetryMetricsRepository otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
      Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
      AttributesBuilder baseAttributesBuilder = Attributes.builder();

      // Add store name if provided
      if (storeName != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_STORE_NAME), storeName);
      }

      // Add request type if provided
      if (requestType != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, requestType.getDimensionValue());
        baseAttributesBuilder.put(
            otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_METHOD),
            requestType.getDimensionValue());
      }

      // Add cluster name if provided
      if (clusterName != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, clusterName);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_CLUSTER_NAME), clusterName);
      }

      // Add route name if provided
      if (routeName != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_ROUTE_NAME, routeName);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_ROUTE_NAME), routeName);
      }

      // Add request retry type if provided
      if (requestRetryType != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE, requestRetryType.getDimensionValue());
        baseAttributesBuilder.put(
            otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE),
            requestRetryType.getDimensionValue());
      }

      // Add helix group ID if provided
      if (helixGroupId != UNASSIGNED_HELIX_GROUP_ID) {
        String helixGroupIdStr = Integer.toString(helixGroupId);
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID, helixGroupIdStr);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID), helixGroupIdStr);
      }

      Attributes baseAttributes = baseAttributesBuilder.build();

      return new OpenTelemetryMetricsSetupInfo(true, otelRepository, baseDimensionsMap, baseAttributes);
    }

    private OpenTelemetryMetricsSetupInfo buildOtelDisabled() {
      return new OpenTelemetryMetricsSetupInfo(false, null, null, null);
    }
  }

  public static Builder builder(MetricsRepository metricsRepository) {
    return new Builder(metricsRepository);
  }
}

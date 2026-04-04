package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
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
  public static final String UNKNOWN_STORE_NAME = "unknown_store";

  /**
   * Sanitizes a store name for use as an OTel dimension value.
   * Returns {@link #UNKNOWN_STORE_NAME} if the input is null, empty, or whitespace-only.
   */
  public static String sanitizeStoreName(String storeName) {
    String trimmed = (storeName == null) ? null : storeName.trim();
    return (trimmed == null || trimmed.isEmpty()) ? UNKNOWN_STORE_NAME : trimmed;
  }

  /**
   * Result object containing the setup OpenTelemetry components.
   *
   * <p>When {@link #emitOpenTelemetryMetrics()} returns {@code false}, {@link #getOtelRepository()} and
   * {@link #getBaseAttributes()} return {@code null}, while {@link #getBaseDimensionsMap()} returns an
   * empty map (not null) so callers that copy-and-augment the map don't need individual null guards.
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

    /** Returns {@code null} when {@link #emitOpenTelemetryMetrics()} is {@code false}. */
    public VeniceOpenTelemetryMetricsRepository getOtelRepository() {
      return otelRepository;
    }

    /** Returns an empty map (not null) when {@link #emitOpenTelemetryMetrics()} is {@code false}. */
    public Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
      return baseDimensionsMap;
    }

    /** Returns {@code null} when {@link #emitOpenTelemetryMetrics()} is {@code false}. */
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
    private final Map<VeniceMetricsDimensions, String> customDimensions = new HashMap<>();
    private String storeName;
    private RequestType requestType;
    private Boolean isTotalStats;
    private Boolean otelEnabledOverride;
    private String clusterName;
    private String regionName;
    private String routeName;
    private RequestRetryType requestRetryType;
    private String threadPoolName;
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
     * Set the region/datacenter name dimension.
     */
    public Builder setRegionName(String regionName) {
      this.regionName = regionName;
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
     * Set the thread pool name dimension.
     */
    public Builder setThreadPoolName(String threadPoolName) {
      this.threadPoolName = threadPoolName;
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
     * Add a custom dimension from a {@link VeniceDimensionInterface} enum value. Use this for
     * component-specific dimensions that are not standard builder parameters (e.g., buffer type)
     * that are used once or in a few places, to avoid bloating the builder with rarely-used parameters.
     * The dimension key and value are derived from the enum.
     *
     * @param dimensionValue the enum value providing both dimension key and value; must not be null
     * @throws NullPointerException if dimensionValue is null
     */
    public Builder addCustomDimension(VeniceDimensionInterface dimensionValue) {
      customDimensions.put(dimensionValue.getDimensionName(), dimensionValue.getDimensionValue());
      return this;
    }

    /**
     * Add a custom dimension with an explicit key and runtime string value. Use this for
     * dimensions whose value is a runtime string (e.g., region name) rather than an enum.
     *
     * @param key the dimension key; must not be null
     * @param value the dimension value; must not be null or empty
     * @throws IllegalArgumentException if key is null, or value is null or empty
     */
    public Builder addCustomDimension(VeniceMetricsDimensions key, String value) {
      if (key == null) {
        throw new IllegalArgumentException("Custom dimension key must not be null");
      }
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException("Custom dimension value must not be null or empty for key: " + key);
      }
      customDimensions.put(key, value);
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
        storeName = sanitizeStoreName(storeName);
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

      // Add region name if provided
      if (regionName != null) {
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_REGION_NAME, regionName);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_REGION_NAME), regionName);
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

      // Add thread pool name if provided
      if (threadPoolName != null) {
        String trimmed = threadPoolName.trim();
        String sanitizedThreadPoolName = trimmed.isEmpty() ? "unknown" : trimmed;
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME, sanitizedThreadPoolName);
        baseAttributesBuilder.put(
            otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME),
            sanitizedThreadPoolName);
      }

      // Add helix group ID if provided
      if (helixGroupId != UNASSIGNED_HELIX_GROUP_ID) {
        String helixGroupIdStr = Integer.toString(helixGroupId);
        baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID, helixGroupIdStr);
        baseAttributesBuilder
            .put(otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID), helixGroupIdStr);
      }

      // Add custom dimensions
      for (Map.Entry<VeniceMetricsDimensions, String> entry: customDimensions.entrySet()) {
        baseDimensionsMap.put(entry.getKey(), entry.getValue());
        baseAttributesBuilder.put(otelRepository.getDimensionName(entry.getKey()), entry.getValue());
      }

      Attributes baseAttributes = baseAttributesBuilder.build();

      return new OpenTelemetryMetricsSetupInfo(true, otelRepository, baseDimensionsMap, baseAttributes);
    }

    /**
     * Returns emptyMap (not null) for baseDimensionsMap so callers that copy-and-augment
     * the map (e.g., adding VENICE_STORE_NAME) don't need individual null guards.
     */
    private OpenTelemetryMetricsSetupInfo buildOtelDisabled() {
      return new OpenTelemetryMetricsSetupInfo(false, null, Collections.emptyMap(), null);
    }
  }

  public static Builder builder(MetricsRepository metricsRepository) {
    return new Builder(metricsRepository);
  }
}

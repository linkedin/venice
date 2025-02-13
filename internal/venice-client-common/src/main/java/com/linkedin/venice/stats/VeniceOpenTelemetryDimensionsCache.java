package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Cache for storing dimensions for OpenTelemetry metrics. The cache is used to avoid churning
 * same dimension objects every time a metric is emitted potentially affecting GC.
 *
 * This class holds
 * 1. some base dimensions to be reused
 * 2. a Thread-local map to pass in the dimension and its values to create {@link Attributes}
 *    and avoid creating temp maps/arrays
 * 3. the actual dimensions cache to hold the {@link Attributes} for every cache key
 *
 * Right now, this class is structured to be instantiated more than once for the same metric repository
 * based on having the base dimensions as part of the stats objects created for different request types,
 * to play along with the current design and pre create the base dimensions and its key. Eventually this
 * might come to a point where there are no base dimensions based on the restructure when removing
 * tehuti and this can be made a static class as part of {@link VeniceOpenTelemetryMetricsRepository}
 */
public class VeniceOpenTelemetryDimensionsCache {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryDimensionsCache.class);
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  /**
   * Store the base dimensions: created once per request-type, per store, etc. metrics
   * extending {@link AbstractVeniceHttpStats} where the common dimensions can be reused
   */
  private final Attributes baseMetricDimensions;
  /** part of cache key from the base dimensions: To reuse */
  private final Set<VeniceMetricsDimensions> baseMetricDimensionsSet;
  /** the keys in the base dimensions to ignore when creating the cache key from all other dimensions */
  private final String baseMetricDimensionsKey;

  /** used to pass in the dimension and its values to create {@link Attributes} and avoid creating temp maps/arrays */
  private static final ThreadLocal<Map<VeniceMetricsDimensions, String>> threadLocalReusableDimensionsMap =
      ThreadLocal.withInitial(HashMap::new);
  /** dimensions cache to hold the {@link Attributes} for every cache key */
  private static final VeniceConcurrentHashMap<String, Attributes> dimensionsCache = new VeniceConcurrentHashMap<>();

  public VeniceOpenTelemetryDimensionsCache(
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseMetricDimensionsMap,
      Map<String, String> customDimensionsMap) {
    this.otelRepository = otelRepository;
    this.baseMetricDimensionsSet = baseMetricDimensionsMap.keySet();

    // craft the base dimensions and its key
    AttributesBuilder attributesBuilder = Attributes.builder();
    StringBuilder baseMetricKeyBuilder = new StringBuilder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseMetricDimensionsMap.entrySet()) {
      VeniceMetricsDimensions dimension = entry.getKey();
      String dimensionValue = entry.getValue();
      if (dimensionValue == null || dimensionValue.isEmpty()) {
        throw new VeniceException("Dimension value cannot be null for " + dimension);
      }
      attributesBuilder.put(otelRepository.getDimensionName(dimension), dimensionValue);
      baseMetricKeyBuilder.append(dimension).append(dimensionValue);
    }
    // add custom dimensions passed in by the user: not needed for the key as it will be
    // the same for all metrics for this repository
    if (customDimensionsMap != null) {
      for (Map.Entry<String, String> entry: customDimensionsMap.entrySet()) {
        attributesBuilder.put(entry.getKey(), entry.getValue());
      }
    }
    this.baseMetricDimensions = attributesBuilder.build();
    this.baseMetricDimensionsKey = baseMetricKeyBuilder.toString();
  }

  /**
   * Check if the cache has the dimensions for the given metric entity and return the dimensions.
   * If not, create the dimensions and cache it.
   *
   * key to the cache is the String formed from dimension name and values in the order of the required dimensions
   * configured in the {@link MetricEntity} except the base and user provided dimensions as those will be same
   * for all metrics. Format of the key is "DIMENSION1NAMEdimension1valueDIMENSION2NAMEdimension2value..."
   */
  public Attributes checkCacheAndGetDimensions(MetricEntity metricEntity) {
    Set<VeniceMetricsDimensions> requiredDimensions = metricEntity.getDimensionsList();

    if (!otelRepository.emitOpenTelemetryMetrics() || baseMetricDimensionsSet.size() == requiredDimensions.size()) {
      return baseMetricDimensions;
    }

    // create cache key based on reusableDimensionsMap and requiredDimensions (which is sorted)
    // to keep the key order same
    Map<VeniceMetricsDimensions, String> reusableDimensionsMap = getThreadLocalReusableDimensionsMap();
    StringBuilder cacheKeyBuilder = new StringBuilder(baseMetricDimensionsKey);
    for (VeniceMetricsDimensions dimension: requiredDimensions) {
      if (!baseMetricDimensionsSet.contains(dimension)) {
        cacheKeyBuilder.append(dimension);
        String dimensionValue = reusableDimensionsMap.get(dimension);
        if (dimensionValue == null) {
          // TODO: this is not a comprehensive check as this thread local map is not cleared after use
          throw new VeniceException("Dimension value cannot be null for " + dimension);
        }
        cacheKeyBuilder.append(dimensionValue);
      }
    }
    String cacheKey = cacheKeyBuilder.toString();

    return dimensionsCache.computeIfAbsent(cacheKey, key -> {
      AttributesBuilder builder =
          (baseMetricDimensions != null) ? baseMetricDimensions.toBuilder() : Attributes.builder();

      for (VeniceMetricsDimensions dimension: requiredDimensions) {
        if (!baseMetricDimensionsSet.contains(dimension)) {
          builder.put(otelRepository.getDimensionName(dimension), reusableDimensionsMap.get(dimension));
        }
      }
      return builder.build();
    });
  }

  public static Map<VeniceMetricsDimensions, String> getThreadLocalReusableDimensionsMap() {
    return threadLocalReusableDimensionsMap.get();
  }

  public Attributes getBaseMetricDimensions() {
    return baseMetricDimensions;
  }

  // Visible for testing
  public Set<VeniceMetricsDimensions> getBaseMetricDimensionsSet() {
    return baseMetricDimensionsSet;
  }

  // Visible for testing
  public String getBaseMetricDimensionsKey() {
    return baseMetricDimensionsKey;
  }
}

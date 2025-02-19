package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.utils.RedundantLogFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class builds dimensions for OpenTelemetry metrics, optionally using a cache.  The cache prevents the
 * creation of identical dimension objects for every metric emission, which can negatively impact garbage collection.
 *
 * This class manages:
 * 1. Reusable base dimensions.
 * 2. A global dimension cache (configurable) that stores {@link Attributes} for each cache key.
 * 3. A thread-local map (configurable) for passing dimensions and their values, ultimately creating {@link Attributes}
 *    and avoiding the creation of temporary maps or arrays.
 *
 * Currently, this class is designed to be instantiated multiple times for the same metric repository. This is due
 * to the base dimensions being part of the stats objects created for different stores, clusters, and request types.
 * This approach aligns with the current design and allows for pre-creation of base dimensions and their keys.
 * Future refactoring, such as the removal of Tehuti, may eliminate the need for base dimensions.  At that point,
 * this class could potentially become a static class as part of {@link VeniceOpenTelemetryMetricsRepository}.
 */
public class VeniceOpenTelemetryDimensionsProvider {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryDimensionsProvider.class);
  private static final RedundantLogFilter REDUNDANT_LOG_FILTER = RedundantLogFilter.getRedundantLogFilter();
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

  /** Enable/disable caching of dimensions */
  private final boolean useCache;
  /** global dimensions cache to hold the {@link Attributes} for every cache key */
  private static final VeniceConcurrentHashMap<String, Attributes> dimensionsCache = new VeniceConcurrentHashMap<>();

  /**
   * Reuse some temporary objects instead of creating it for every record call.
   * This might help in reducing GC.
   */
  private final boolean reuseTemporaryObjects;
  /** used to pass in the dimension and its values to create {@link Attributes} and avoid creating temp maps/arrays */
  private static final ThreadLocal<Map<VeniceMetricsDimensions, String>> threadLocalReusableDimensionsMap =
      ThreadLocal.withInitial(HashMap::new);

  public VeniceOpenTelemetryDimensionsProvider(
      @Nonnull VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseMetricDimensionsMap,
      Map<String, String> customDimensionsMap) {
    this.otelRepository = otelRepository;
    this.useCache = otelRepository.getMetricsConfig().useDimensionsCache();
    this.reuseTemporaryObjects = otelRepository.getMetricsConfig().reuseTemporaryObjects();
    this.baseMetricDimensionsSet = baseMetricDimensionsMap == null ? null : baseMetricDimensionsMap.keySet();

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
    this.baseMetricDimensionsKey = useCache ? baseMetricKeyBuilder.toString() : null;
  }

  /**
   * create cache key based on inputDimensionsMap and requiredDimensions (which is
   * sorted to keep the key order same)
   */
  private String buildCacheKey(
      MetricEntity metricEntity,
      Set<VeniceMetricsDimensions> requiredDimensions,
      Map<VeniceMetricsDimensions, String> inputDimensionsMap) {
    StringBuilder cacheKeyBuilder = new StringBuilder(baseMetricDimensionsKey);
    for (VeniceMetricsDimensions dimension: requiredDimensions) {
      if (!baseMetricDimensionsSet.contains(dimension)) {
        cacheKeyBuilder.append(dimension);
        String dimensionValue = inputDimensionsMap.get(dimension);
        validateDimensionValuesAndThrow(metricEntity, dimension, dimensionValue);
        cacheKeyBuilder.append(dimensionValue);
      }
    }
    return cacheKeyBuilder.toString();
  }

  private Attributes buildNewDimensions(
      MetricEntity metricEntity,
      Set<VeniceMetricsDimensions> requiredDimensions,
      Map<VeniceMetricsDimensions, String> inputDimensionsMap) {
    AttributesBuilder builder =
        (baseMetricDimensions != null) ? baseMetricDimensions.toBuilder() : Attributes.builder();

    for (VeniceMetricsDimensions dimension: requiredDimensions) {
      if (!baseMetricDimensionsSet.contains(dimension)) {
        String dimensionValue = inputDimensionsMap.get(dimension);
        validateDimensionValuesAndThrow(metricEntity, dimension, dimensionValue);
        builder.put(otelRepository.getDimensionName(dimension), inputDimensionsMap.get(dimension));
      }
    }
    return builder.build();
  }

  /**
   * Check if the cache has the dimensions for the given metric entity and return the dimensions.
   * If not, create the dimensions and cache it.
   *
   * key to the cache is the String formed from dimension name and values in the order of the required dimensions
   * configured in the {@link MetricEntity} except the base and user provided dimensions as those will be same
   * for all metrics. Format of the key is "DIMENSION1NAMEdimension1valueDIMENSION2NAMEdimension2value..."
   */
  public Attributes getDimensions(MetricEntity metricEntity, Map<VeniceMetricsDimensions, String> inputDimensionsMap) {
    Set<VeniceMetricsDimensions> requiredDimensions = metricEntity.getDimensionsList();

    if (!otelRepository.emitOpenTelemetryMetrics() || baseMetricDimensionsSet.size() == requiredDimensions.size()) {
      return baseMetricDimensions;
    }

    if (useCache) {
      return dimensionsCache.computeIfAbsent(
          buildCacheKey(metricEntity, requiredDimensions, inputDimensionsMap),
          key -> buildNewDimensions(metricEntity, requiredDimensions, inputDimensionsMap));
    } else {
      return buildNewDimensions(metricEntity, requiredDimensions, inputDimensionsMap);
    }
  }

  public Map<VeniceMetricsDimensions, String> getInputDimensionsMap() {
    if (reuseTemporaryObjects) {
      return threadLocalReusableDimensionsMap.get();
    } else {
      return new HashMap<>();
    }
  }

  private void validateDimensionValuesAndThrow(
      MetricEntity metricEntity,
      VeniceMetricsDimensions dimension,
      String dimensionValue) {
    if (dimensionValue == null || dimensionValue.isEmpty()) {
      String errorLog = "Dimension value cannot be null or empty for key: " + dimension + " for metric: "
          + metricEntity.getMetricName();
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(errorLog)) {
        LOGGER.error(errorLog);
      }
      throw new VeniceException(errorLog);
    }
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

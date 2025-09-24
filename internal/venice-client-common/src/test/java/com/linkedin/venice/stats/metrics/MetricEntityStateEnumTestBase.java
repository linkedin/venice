package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.metrics.MetricType.COUNTER;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;


/**
 * Base class for MetricEntityState*Enums test classes to share common setup and utility methods.
 */
public abstract class MetricEntityStateEnumTestBase {
  protected VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  protected MetricEntity mockMetricEntity;
  protected Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /**
   * Common setup for all MetricEntityState enum tests.
   */
  protected void setUpCommonMocks() {
    mockOtelRepository = Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(true);
    when(mockOtelRepository.getMetricFormat()).thenReturn(getDefaultFormat());
    when(mockOtelRepository.getDimensionName(any())).thenCallRealMethod();
    when(mockOtelRepository.createAttributes(any(), any(), (VeniceDimensionInterface) any())).thenCallRealMethod();
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.getOtelCustomDimensionsMap()).thenReturn(new HashMap<>());
    when(mockOtelRepository.getMetricsConfig()).thenReturn(mockMetricsConfig);

    mockMetricEntity = Mockito.mock(MetricEntity.class);
    when(mockMetricEntity.getMetricType()).thenReturn(COUNTER);

    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
  }

  /**
   * Helper method to create a dimension set with the given dimensions.
   */
  protected Set<VeniceMetricsDimensions> createDimensionSet(VeniceMetricsDimensions... dimensions) {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD);
    for (VeniceMetricsDimensions dimension: dimensions) {
      dimensionsSet.add(dimension);
    }
    return dimensionsSet;
  }

  /**
   * Helper method to setup mock metric entity with dimensions.
   */
  protected void setupMockMetricEntity(Set<VeniceMetricsDimensions> dimensionsSet) {
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
  }

  /**
   * Helper method to build attributes for given enums.
   */
  protected Attributes buildAttributes(VeniceDimensionInterface... enums) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
      attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue());
    }
    for (VeniceDimensionInterface enumValue: enums) {
      attributesBuilder
          .put(mockOtelRepository.getDimensionName(enumValue.getDimensionName()), enumValue.getDimensionValue());
    }
    return attributesBuilder.build();
  }

  /**
   * Helper method to create attribute name string for given enum values.
   */
  protected String createAttributeName(String prefix, VeniceDimensionInterface... enums) {
    StringBuilder sb = new StringBuilder(prefix);
    for (int i = 0; i < enums.length; i++) {
      sb.append("Enum").append(i + 1).append(((Enum<?>) enums[i]).name());
    }
    return sb.toString();
  }
}

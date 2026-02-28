package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROUTE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class OpenTelemetryMetricsSetupTest {
  @Mock
  private VeniceMetricsRepository mockVeniceMetricsRepository;

  @Mock
  private VeniceMetricsConfig mockVeniceMetricsConfig;

  @Mock
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;

  @Mock
  private MetricsRepository mockNonVeniceMetricsRepository;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockOtelRepository.getDimensionName(VENICE_STORE_NAME))
        .thenReturn(VENICE_STORE_NAME.getDimensionNameInDefaultFormat());
    when(mockOtelRepository.getDimensionName(VENICE_REQUEST_METHOD))
        .thenReturn(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat());
    when(mockOtelRepository.getDimensionName(VENICE_CLUSTER_NAME))
        .thenReturn(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat());
    when(mockOtelRepository.getDimensionName(VENICE_ROUTE_NAME))
        .thenReturn(VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat());
  }

  @Test
  public void testBuilderWithNonVeniceMetricsRepository() {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockNonVeniceMetricsRepository).build();

    assertOtelDisabled(result);
  }

  @Test
  public void testBuilderWithVeniceMetricsRepositoryOtelDisabled() {
    setupGlobalOtel(false);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockVeniceMetricsRepository).build();

    assertOtelDisabled(result);
  }

  @Test
  public void testBuilderWithVeniceMetricsRepositoryOtelEnabledButTotalStats() {
    setupGlobalOtel(true);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockVeniceMetricsRepository).isTotalStats(true).build();

    assertOtelDisabled(result);
  }

  @DataProvider(name = "otelEnabledOverrideCombinations", parallel = true)
  public Object[][] otelEnabledOverrideCombinations() {
    // { globalOtelEnabled, otelEnabledOverride (null=not set), expectedEmitOtel }
    return new Object[][] {
        // Override=false disables OTel even when global is enabled
        { true, Boolean.FALSE, false },
        // Override=true cannot re-enable OTel when global is disabled
        { false, Boolean.TRUE, false },
        // Override=true with global enabled → OTel enabled (pass-through)
        { true, Boolean.TRUE, true },
        // Override not set → follows global config
        { true, null, true }, { false, null, false },
        // Both global and override disabled
        { false, Boolean.FALSE, false }, };
  }

  @Test(dataProvider = "otelEnabledOverrideCombinations")
  public void testOtelEnabledOverrideInteractionWithGlobalConfig(
      boolean globalOtelEnabled,
      Boolean otelEnabledOverride,
      boolean expectedEmitOtel) {
    // Use local mocks to be parallel-safe (instance mocks race under concurrent DataProvider)
    VeniceMetricsConfig localConfig = org.mockito.Mockito.mock(VeniceMetricsConfig.class);
    VeniceMetricsRepository localRepo = org.mockito.Mockito.mock(VeniceMetricsRepository.class);
    VeniceOpenTelemetryMetricsRepository localOtelRepo =
        org.mockito.Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    when(localRepo.getVeniceMetricsConfig()).thenReturn(localConfig);
    when(localConfig.emitOtelMetrics()).thenReturn(globalOtelEnabled);
    when(localRepo.getOpenTelemetryMetricsRepository()).thenReturn(localOtelRepo);

    OpenTelemetryMetricsSetup.Builder builder = OpenTelemetryMetricsSetup.builder(localRepo);
    if (otelEnabledOverride != null) {
      builder.setOtelEnabledOverride(otelEnabledOverride);
    }
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result = builder.build();

    assertEquals(result.emitOpenTelemetryMetrics(), expectedEmitOtel);
    if (expectedEmitOtel) {
      assertNotNull(result.getOtelRepository());
      assertNotNull(result.getBaseDimensionsMap());
      assertNotNull(result.getBaseAttributes());
    } else {
      assertOtelDisabled(result);
    }
  }

  private void setupGlobalOtel(boolean enabled) {
    when(mockVeniceMetricsRepository.getVeniceMetricsConfig()).thenReturn(mockVeniceMetricsConfig);
    when(mockVeniceMetricsConfig.emitOtelMetrics()).thenReturn(enabled);
    when(mockVeniceMetricsRepository.getOpenTelemetryMetricsRepository()).thenReturn(mockOtelRepository);
  }

  private void assertOtelDisabled(OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result) {
    assertNotNull(result);
    assertFalse(result.emitOpenTelemetryMetrics());
    assertNull(result.getOtelRepository());
    assertNull(result.getBaseDimensionsMap());
    assertNull(result.getBaseAttributes());
  }

  @Test
  public void testBuilderWithVeniceMetricsRepositoryOtelEnabled() {
    setupGlobalOtel(true);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockVeniceMetricsRepository).build();

    assertNotNull(result);
    assertTrue(result.emitOpenTelemetryMetrics());
    assertEquals(result.getOtelRepository(), mockOtelRepository);
    assertNotNull(result.getBaseDimensionsMap());
    assertTrue(result.getBaseDimensionsMap().isEmpty());
    assertNotNull(result.getBaseAttributes());
    assertTrue(result.getBaseAttributes().isEmpty());
  }

  @Test
  public void testBuilderWithAllDimensions() {
    setupGlobalOtel(true);

    String storeName = "test-store";
    RequestType requestType = RequestType.SINGLE_GET;
    String clusterName = "test-cluster";
    String routeName = "test-route";

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockVeniceMetricsRepository)
            .setStoreName(storeName)
            .setRequestType(requestType)
            .setClusterName(clusterName)
            .setRouteName(routeName)
            .isTotalStats(false)
            .build();

    assertNotNull(result);
    assertTrue(result.emitOpenTelemetryMetrics());
    assertEquals(result.getOtelRepository(), mockOtelRepository);

    Map<VeniceMetricsDimensions, String> baseDimensionsMap = result.getBaseDimensionsMap();
    assertNotNull(baseDimensionsMap);
    assertEquals(baseDimensionsMap.size(), 4);
    assertEquals(baseDimensionsMap.get(VENICE_STORE_NAME), storeName);
    assertEquals(baseDimensionsMap.get(VENICE_REQUEST_METHOD), requestType.getDimensionValue());
    assertEquals(baseDimensionsMap.get(VENICE_CLUSTER_NAME), clusterName);
    assertEquals(baseDimensionsMap.get(VENICE_ROUTE_NAME), routeName);

    Attributes baseAttributes = result.getBaseAttributes();
    assertNotNull(baseAttributes);
    assertEquals(baseAttributes.size(), 4);
    assertEquals(
        baseAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("venice.store.name")),
        storeName);
    assertEquals(
        baseAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("venice.request.method")),
        requestType.getDimensionValue());
    assertEquals(
        baseAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("venice.cluster.name")),
        clusterName);
    assertEquals(
        baseAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("venice.route.name")),
        routeName);
  }

  @Test
  public void testBuilderWithPartialDimensions() {
    setupGlobalOtel(true);

    String storeName = "test-store";
    RequestType requestType = RequestType.MULTI_GET;

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo result =
        OpenTelemetryMetricsSetup.builder(mockVeniceMetricsRepository)
            .setStoreName(storeName)
            .setRequestType(requestType)
            // Note: not setting cluster name or route name
            .build();

    assertNotNull(result);
    assertTrue(result.emitOpenTelemetryMetrics());

    Map<VeniceMetricsDimensions, String> baseDimensionsMap = result.getBaseDimensionsMap();
    assertNotNull(baseDimensionsMap);
    assertEquals(baseDimensionsMap.size(), 2);
    assertEquals(baseDimensionsMap.get(VENICE_STORE_NAME), storeName);
    assertEquals(baseDimensionsMap.get(VENICE_REQUEST_METHOD), requestType.getDimensionValue());
    assertNull(baseDimensionsMap.get(VENICE_CLUSTER_NAME));
    assertNull(baseDimensionsMap.get(VENICE_ROUTE_NAME));

    Attributes baseAttributes = result.getBaseAttributes();
    assertNotNull(baseAttributes);
    assertEquals(baseAttributes.size(), 2);
  }

  @Test
  public void testBuilderMethodChaining() {
    OpenTelemetryMetricsSetup.Builder builder = OpenTelemetryMetricsSetup.builder(mockNonVeniceMetricsRepository);

    OpenTelemetryMetricsSetup.Builder result1 = builder.setStoreName("test-store");
    OpenTelemetryMetricsSetup.Builder result2 = result1.setRequestType(RequestType.SINGLE_GET);
    OpenTelemetryMetricsSetup.Builder result3 = result2.setClusterName("test-cluster");
    OpenTelemetryMetricsSetup.Builder result4 = result3.setRouteName("test-route");
    OpenTelemetryMetricsSetup.Builder result5 = result4.isTotalStats(true);

    // All should return the same builder instance
    assertEquals(builder, result1);
    assertEquals(result1, result2);
    assertEquals(result2, result3);
    assertEquals(result3, result4);
    assertEquals(result4, result5);
  }
}

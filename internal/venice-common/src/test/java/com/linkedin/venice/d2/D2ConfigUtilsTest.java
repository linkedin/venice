package com.linkedin.venice.d2;

import static com.linkedin.d2.balancer.properties.PropertyKeys.HTTP_LB_QUARANTINE_MAX_PERCENT;
import static com.linkedin.d2.balancer.properties.PropertyKeys.HTTP_LB_QUARANTINE_METHOD;
import static com.linkedin.d2.balancer.properties.PropertyKeys.LB_STRATEGY_PROPERTIES;
import static com.linkedin.d2.balancer.properties.PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD;
import static com.linkedin.d2.balancer.properties.PropertyKeys.QUARANTINE_HEALTH_CHECK_PATH;
import static com.linkedin.d2.balancer.properties.PropertyKeys.QUARANTINE_MAX_PERCENT;
import static com.linkedin.d2.balancer.properties.PropertyKeys.QUARANTINE_PROPERTIES;
import static com.linkedin.d2.balancer.properties.PropertyKeys.RELATIVE_STRATEGY_PROPERTIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.D2QuarantineProperties;
import com.linkedin.d2.HttpMethod;
import com.linkedin.d2.balancer.config.LoadBalancerStrategyPropertiesConverter;
import com.linkedin.d2.balancer.config.RelativeStrategyPropertiesConverter;
import com.linkedin.d2.balancer.properties.PropertyKeys;
import com.linkedin.d2.balancer.properties.ServiceProperties;
import com.linkedin.d2.balancer.strategies.degrader.DegraderLoadBalancerStrategyConfig;
import com.linkedin.d2.balancer.strategies.degrader.DegraderLoadBalancerStrategyFactoryV3;
import com.linkedin.venice.d2.D2ConfigUtils.D2ServiceRole;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class D2ConfigUtilsTest {
  @DataProvider
  public Object[][] roles() {
    return new Object[][] { { D2ServiceRole.GENERIC }, { D2ServiceRole.CONTROLLER }, { D2ServiceRole.STORAGE_SERVER } };
  }

  @Test(dataProvider = "roles")
  public void testRoleDefaultsAreRecognizedByPinnedD2(D2ServiceRole role) {
    Map<String, Object> completed = D2ConfigUtils.withQuarantineDefaults(Collections.emptyMap(), role);
    Map<String, Object> legacy = getMap(completed, LB_STRATEGY_PROPERTIES);
    D2QuarantineProperties relative =
        RelativeStrategyPropertiesConverter.toProperties(getMap(completed, RELATIVE_STRATEGY_PROPERTIES))
            .getQuarantineProperties();
    DegraderLoadBalancerStrategyConfig legacyConfig = getLegacyConfig(legacy);

    assertEquals(
        LoadBalancerStrategyPropertiesConverter.toConfig(legacy).getQuarantineCfg().getQuarantineMaxPercent(),
        Double.valueOf(0.1));
    assertEquals(legacyConfig.getQuarantineMaxPercent(), 0.1);
    assertEquals(relative.getQuarantineMaxPercent(), Double.valueOf(0.1));
    if (role == D2ServiceRole.GENERIC) {
      assertFalse(legacy.containsKey(HTTP_LB_QUARANTINE_METHOD));
      assertEquals(legacyConfig.getHealthCheckMethod(), "OPTIONS");
      assertNull(legacyConfig.getHealthCheckPath());
      assertFalse(relative.hasHealthCheckMethod());
      assertFalse(relative.hasHealthCheckPath());
    } else {
      assertEquals(legacyConfig.getHealthCheckMethod(), "GET");
      assertEquals(legacyConfig.getHealthCheckPath(), "/health");
      assertEquals(relative.getHealthCheckMethod(), HttpMethod.GET);
      assertEquals(relative.getHealthCheckPath(), "/health");
    }
  }

  @DataProvider
  public Object[][] caps() {
    return new Object[][] { { 0.0, "0" }, { "0", 0.0 }, { "0.75", 0.25 }, { 0.75, "0.6" } };
  }

  @Test(dataProvider = "caps")
  public void testExplicitCapsArePreservedIndependently(Object legacyCap, Object relativeCap) {
    Map<String, Object> input = new HashMap<>(legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, legacyCap));
    input.putAll(relativeProperty(QUARANTINE_MAX_PERCENT, relativeCap));

    Map<String, Object> completed = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.STORAGE_SERVER);

    assertEquals(getMap(completed, LB_STRATEGY_PROPERTIES).get(HTTP_LB_QUARANTINE_MAX_PERCENT), legacyCap);
    assertEquals(
        getMap(getMap(completed, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES).get(QUARANTINE_MAX_PERCENT),
        relativeCap);
  }

  @Test
  public void testPinnedLegacyClampDoesNotRewriteExplicitOverride() {
    Map<String, Object> completed = D2ConfigUtils
        .withQuarantineDefaults(legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, "0.75"), D2ServiceRole.GENERIC);
    Map<String, Object> legacy = getMap(completed, LB_STRATEGY_PROPERTIES);

    DegraderLoadBalancerStrategyConfig legacyConfig = getLegacyConfig(legacy);

    assertEquals(legacyConfig.getQuarantineMaxPercent(), 0.5);
    assertEquals(legacy.get(HTTP_LB_QUARANTINE_MAX_PERCENT), "0.75");
    assertEquals(
        LoadBalancerStrategyPropertiesConverter.toConfig(legacy).getQuarantineCfg().getQuarantineMaxPercent(),
        Double.valueOf(0.75));
  }

  @DataProvider
  public Object[][] partialProbes() {
    return new Object[][] { { QUARANTINE_HEALTH_CHECK_METHOD, "OPTIONS", "OPTIONS", "/health" },
        { QUARANTINE_HEALTH_CHECK_PATH, "/custom-relative", "GET", "/custom-relative" } };
  }

  @Test(dataProvider = "partialProbes")
  public void testPartialMapsFillOnlyMissingLeaves(
      String suppliedKey,
      String suppliedValue,
      String expectedMethod,
      String expectedPath) {
    Map<String, Object> input = new HashMap<>(legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, "0.25"));
    input.putAll(relativeProperty(suppliedKey, suppliedValue));

    Map<String, Object> completed = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.CONTROLLER);

    Map<String, Object> legacy = getMap(completed, LB_STRATEGY_PROPERTIES);
    Map<String, Object> relative = getMap(getMap(completed, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES);
    assertEquals(legacy.get(HTTP_LB_QUARANTINE_MAX_PERCENT), "0.25");
    assertEquals(legacy.get(HTTP_LB_QUARANTINE_METHOD), "GET:/health");
    assertEquals(relative.get(QUARANTINE_MAX_PERCENT), "0.1");
    assertEquals(relative.get(QUARANTINE_HEALTH_CHECK_METHOD), expectedMethod);
    assertEquals(relative.get(QUARANTINE_HEALTH_CHECK_PATH), expectedPath);
  }

  @DataProvider
  public Object[][] customProbes() {
    return new Object[][] { { "OPTIONS:/custom-legacy", "options", "/custom-relative" }, { "", "GET", "" } };
  }

  @Test(dataProvider = "customProbes")
  public void testExplicitProbesIncludingEmptyPathArePreserved(String legacyMethod, String method, String path) {
    Map<String, Object> quarantine = new HashMap<>();
    quarantine.put(QUARANTINE_HEALTH_CHECK_METHOD, method);
    quarantine.put(QUARANTINE_HEALTH_CHECK_PATH, path);
    Map<String, Object> input = new HashMap<>(legacyProperty(HTTP_LB_QUARANTINE_METHOD, legacyMethod));
    input.put(RELATIVE_STRATEGY_PROPERTIES, Collections.singletonMap(QUARANTINE_PROPERTIES, quarantine));

    Map<String, Object> completed = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.CONTROLLER);

    assertEquals(getMap(completed, LB_STRATEGY_PROPERTIES).get(HTTP_LB_QUARANTINE_METHOD), legacyMethod);
    Map<String, Object> relative = getMap(getMap(completed, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES);
    assertEquals(relative.get(QUARANTINE_HEALTH_CHECK_METHOD), method);
    assertEquals(relative.get(QUARANTINE_HEALTH_CHECK_PATH), path);
    assertEquals(
        RelativeStrategyPropertiesConverter.toProperties(getMap(completed, RELATIVE_STRATEGY_PROPERTIES))
            .getQuarantineProperties()
            .getHealthCheckMethod(),
        HttpMethod.valueOf(method.toUpperCase(java.util.Locale.ROOT)));
  }

  @Test
  public void testImmutableInputsAndSeparateResultsDoNotShareQuarantineMaps() {
    Map<String, Object> input = new HashMap<>(legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, "0.2"));
    input.putAll(relativeProperty(QUARANTINE_HEALTH_CHECK_METHOD, "OPTIONS"));
    input = Collections.unmodifiableMap(input);

    Map<String, Object> first = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.CONTROLLER);
    Map<String, Object> second = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.STORAGE_SERVER);

    assertNotSame(first, input);
    assertNotSame(getMap(first, LB_STRATEGY_PROPERTIES), getMap(input, LB_STRATEGY_PROPERTIES));
    assertNotSame(getMap(first, RELATIVE_STRATEGY_PROPERTIES), getMap(input, RELATIVE_STRATEGY_PROPERTIES));
    Map<String, Object> firstQuarantine = getMap(getMap(first, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES);
    assertNotSame(firstQuarantine, getMap(getMap(input, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES));
    getMap(first, LB_STRATEGY_PROPERTIES).put(HTTP_LB_QUARANTINE_MAX_PERCENT, "0");
    firstQuarantine.put(QUARANTINE_MAX_PERCENT, "0");
    assertEquals(getMap(second, LB_STRATEGY_PROPERTIES).get(HTTP_LB_QUARANTINE_MAX_PERCENT), "0.2");
    assertEquals(
        getMap(getMap(second, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES).get(QUARANTINE_MAX_PERCENT),
        "0.1");
    assertEquals(
        getMap(input, LB_STRATEGY_PROPERTIES),
        Collections.singletonMap(HTTP_LB_QUARANTINE_MAX_PERCENT, "0.2"));
    assertEquals(
        getMap(getMap(input, RELATIVE_STRATEGY_PROPERTIES), QUARANTINE_PROPERTIES),
        Collections.singletonMap(QUARANTINE_HEALTH_CHECK_METHOD, "OPTIONS"));
  }

  @Test
  public void testNonQuarantinePropertiesAreUnchanged() {
    Map<String, Object> legacy = new HashMap<>();
    legacy.put(PropertyKeys.HTTP_LB_STRATEGY_PROPERTIES_UPDATE_INTERVAL_MS, "9000");
    legacy.put(PropertyKeys.HTTP_LB_INITIAL_RECOVERY_LEVEL, "0.02");
    Map<String, Object> transport = new HashMap<>();
    transport.put("http.requestTimeout", "12345");
    transport.put("http.maxResponseSize", "1048576");
    Map<String, Object> input = new HashMap<>();
    input.put(PropertyKeys.LB_STRATEGY_LIST, Arrays.asList("degraderV3", "degraderV2"));
    input.put(PropertyKeys.PRIORITIZED_SCHEMES, Arrays.asList("https", "http"));
    input.put(LB_STRATEGY_PROPERTIES, Collections.unmodifiableMap(legacy));
    input.put(PropertyKeys.TRANSPORT_CLIENT_PROPERTIES, Collections.unmodifiableMap(transport));
    input.put(PropertyKeys.DEGRADER_PROPERTIES, Collections.singletonMap("degrader.minCallCount", "10"));
    input.put(RELATIVE_STRATEGY_PROPERTIES, Collections.singletonMap(PropertyKeys.ENABLE_FAST_RECOVERY, "true"));
    input.put(PropertyKeys.BACKUP_REQUESTS, Collections.emptyList());

    Map<String, Object> completed = D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.STORAGE_SERVER);

    getMap(completed, LB_STRATEGY_PROPERTIES).remove(HTTP_LB_QUARANTINE_MAX_PERCENT);
    getMap(completed, LB_STRATEGY_PROPERTIES).remove(HTTP_LB_QUARANTINE_METHOD);
    getMap(completed, RELATIVE_STRATEGY_PROPERTIES).remove(QUARANTINE_PROPERTIES);
    assertEquals(completed, input);
  }

  @DataProvider
  public Object[][] malformedProperties() {
    String relativeQuarantinePath = RELATIVE_STRATEGY_PROPERTIES + "." + QUARANTINE_PROPERTIES;
    return new Object[][] { { Collections.singletonMap(LB_STRATEGY_PROPERTIES, "not-a-map"), LB_STRATEGY_PROPERTIES },
        { Collections.singletonMap(RELATIVE_STRATEGY_PROPERTIES, null), RELATIVE_STRATEGY_PROPERTIES },
        { Collections
            .singletonMap(RELATIVE_STRATEGY_PROPERTIES, Collections.singletonMap(QUARANTINE_PROPERTIES, "bad")),
            relativeQuarantinePath },
        { legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, null), HTTP_LB_QUARANTINE_MAX_PERCENT },
        { legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, 0), HTTP_LB_QUARANTINE_MAX_PERCENT },
        { legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, "bad"), HTTP_LB_QUARANTINE_MAX_PERCENT },
        { legacyProperty(HTTP_LB_QUARANTINE_MAX_PERCENT, Double.POSITIVE_INFINITY), HTTP_LB_QUARANTINE_MAX_PERCENT },
        { relativeProperty(QUARANTINE_MAX_PERCENT, 0), relativeQuarantinePath + "." + QUARANTINE_MAX_PERCENT },
        { relativeProperty(QUARANTINE_MAX_PERCENT, "NaN"), relativeQuarantinePath + "." + QUARANTINE_MAX_PERCENT },
        { legacyProperty(HTTP_LB_QUARANTINE_METHOD, false), HTTP_LB_QUARANTINE_METHOD },
        { relativeProperty(QUARANTINE_HEALTH_CHECK_METHOD, null),
            relativeQuarantinePath + "." + QUARANTINE_HEALTH_CHECK_METHOD },
        { relativeProperty(QUARANTINE_HEALTH_CHECK_METHOD, "HEAD"),
            relativeQuarantinePath + "." + QUARANTINE_HEALTH_CHECK_METHOD },
        { relativeProperty(QUARANTINE_HEALTH_CHECK_PATH, 1),
            relativeQuarantinePath + "." + QUARANTINE_HEALTH_CHECK_PATH } };
  }

  @Test(dataProvider = "malformedProperties")
  public void testMalformedPropertiesFailWithTheAffectedKey(Map<String, Object> input, String key) {
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> D2ConfigUtils.withQuarantineDefaults(input, D2ServiceRole.CONTROLLER));

    assertTrue(exception.getMessage().contains(key), exception.getMessage());
  }

  @Test
  public void testNullServicePropertiesAndRoleAreRejected() {
    expectThrows(VeniceException.class, () -> D2ConfigUtils.withQuarantineDefaults(null, D2ServiceRole.GENERIC));
    expectThrows(VeniceException.class, () -> D2ConfigUtils.withQuarantineDefaults(Collections.emptyMap(), null));
  }

  private static Map<String, Object> legacyProperty(String key, Object value) {
    return Collections.singletonMap(LB_STRATEGY_PROPERTIES, Collections.singletonMap(key, value));
  }

  private static Map<String, Object> relativeProperty(String key, Object value) {
    return Collections.singletonMap(
        RELATIVE_STRATEGY_PROPERTIES,
        Collections.singletonMap(QUARANTINE_PROPERTIES, Collections.singletonMap(key, value)));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> getMap(Map<String, Object> properties, String key) {
    return (Map<String, Object>) properties.get(key);
  }

  private static DegraderLoadBalancerStrategyConfig getLegacyConfig(Map<String, Object> properties) {
    ServiceProperties service = new ServiceProperties(
        "test-service",
        "test-cluster",
        "/",
        Arrays.asList("degraderV3", "degraderV2"),
        properties);
    return new DegraderLoadBalancerStrategyFactoryV3().newLoadBalancer(service).getConfig();
  }
}

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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.D2QuarantineProperties;
import com.linkedin.d2.HttpMethod;
import com.linkedin.d2.balancer.config.LoadBalancerStrategyPropertiesConverter;
import com.linkedin.d2.balancer.config.RelativeStrategyPropertiesConverter;
import com.linkedin.d2.balancer.properties.PropertyKeys;
import com.linkedin.d2.balancer.properties.ServiceProperties;
import com.linkedin.d2.balancer.properties.ServicePropertiesJsonSerializer;
import com.linkedin.d2.balancer.strategies.degrader.DegraderLoadBalancerStrategyConfig;
import com.linkedin.d2.balancer.strategies.degrader.DegraderLoadBalancerStrategyFactoryV3;
import com.linkedin.d2.balancer.zkfs.ZKFSUtil;
import com.linkedin.venice.d2.D2ConfigUtils.D2ServiceRole;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class D2ConfigUtilsIntegrationTest {
  private ZkServerWrapper zkServer;
  private ZooKeeper zooKeeper;

  @BeforeClass
  public void setUp() throws Exception {
    zkServer = ServiceFactory.getZkServer();
    zooKeeper = new ZooKeeper(zkServer.getAddress(), 5000, event -> {});
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    try {
      if (zooKeeper != null) {
        zooKeeper.close();
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(zkServer);
    }
  }

  @DataProvider
  public Object[][] publishers() {
    return new Object[][] { { false }, { true } };
  }

  @DataProvider
  public Object[][] defaultPolicies() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean testPublisher: new boolean[] { false, true }) {
      for (D2ServiceRole role: D2ServiceRole.values()) {
        for (boolean https: new boolean[] { false, true }) {
          cases.add(new Object[] { testPublisher, role, https });
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "defaultPolicies", timeOut = 60 * Time.MS_PER_SECOND)
  public void testFullDefaultPoliciesRoundTripThroughBothBuilders(
      boolean testPublisher,
      D2ServiceRole role,
      boolean https) {
    String cluster = Utils.getUniqueString("test-d2-cluster");
    String service = Utils.getUniqueString("test-d2-service");

    publish(testPublisher, https, cluster, service, role, Collections.emptyMap());

    ServiceProperties policy = D2TestUtils.getD2ServiceProperties(zkServer.getAddress(), service);
    assertEquals(policy.getClusterName(), cluster);
    assertEquals(policy.getServiceName(), service);
    assertEquals(policy.getPath(), "/");
    assertEquals(policy.getLoadBalancerStrategyList(), Arrays.asList("degraderV3", "degraderV2"));
    assertEquals(
        policy.getPrioritizedSchemes(),
        https ? Arrays.asList("https", "http") : Collections.singletonList("http"));
    Map<String, Object> legacy = policy.getLoadBalancerStrategyProperties();
    assertEquals(legacy.get(PropertyKeys.HTTP_LB_STRATEGY_PROPERTIES_UPDATE_INTERVAL_MS), "5000");
    assertEquals(legacy.get(PropertyKeys.HTTP_LB_STRATEGY_PROPERTIES_POINTS_PER_WEIGHT), "100");
    assertEquals(legacy.get(PropertyKeys.HTTP_LB_CONSISTENT_HASH_ALGORITHM), "pointBased");
    assertEquals(policy.getTransportClientProperties(), Collections.singletonMap("http.requestTimeout", "10000"));
    Map<String, String> expectedDegrader = new HashMap<>();
    expectedDegrader.put("degrader.minCallCount", "10");
    expectedDegrader.put("degrader.lowErrorRate", "0.01");
    expectedDegrader.put("degrader.highErrorRate", "0.1");
    assertEquals(policy.getDegraderProperties(), expectedDegrader);
    assertTrue(policy.getBackupRequests().isEmpty());
    assertTrue(policy.getServiceMetadataProperties().isEmpty());
    assertTrue(policy.getBanned().isEmpty());
    assertRecognizedCaps(policy, 0.1, 0.1);
    DegraderLoadBalancerStrategyConfig legacyConfig =
        new DegraderLoadBalancerStrategyFactoryV3().newLoadBalancer(policy).getConfig();
    D2QuarantineProperties relative = getRelativeQuarantine(policy);
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
  public Object[][] capOverrides() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean testPublisher: new boolean[] { false, true }) {
      cases.add(new Object[] { testPublisher, "0", 0.0 });
      cases.add(new Object[] { testPublisher, 0.0, "0" });
      cases.add(new Object[] { testPublisher, "0.75", 0.6 });
      cases.add(new Object[] { testPublisher, 0.2, "0.25" });
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "capOverrides", timeOut = 60 * Time.MS_PER_SECOND)
  public void testOverridesRoundTripWithoutChangingCallerMaps(
      boolean testPublisher,
      Object legacyCap,
      Object relativeCap) {
    Map<String, Object> legacy = new HashMap<>();
    legacy.put(HTTP_LB_QUARANTINE_MAX_PERCENT, legacyCap);
    legacy.put(HTTP_LB_QUARANTINE_METHOD, "OPTIONS:/custom-legacy");
    Map<String, Object> quarantine = new HashMap<>();
    quarantine.put(QUARANTINE_MAX_PERCENT, relativeCap);
    quarantine.put(QUARANTINE_HEALTH_CHECK_PATH, "/custom-relative");
    Map<String, Object> overrides = new HashMap<>();
    overrides.put(LB_STRATEGY_PROPERTIES, Collections.unmodifiableMap(legacy));
    overrides.put(
        RELATIVE_STRATEGY_PROPERTIES,
        Collections.singletonMap(QUARANTINE_PROPERTIES, Collections.unmodifiableMap(quarantine)));
    overrides.put(PropertyKeys.TRANSPORT_CLIENT_PROPERTIES, Collections.singletonMap("http.requestTimeout", "12345"));
    Map<String, Object> immutableOverrides = Collections.unmodifiableMap(overrides);
    String cluster = Utils.getUniqueString("test-override-cluster");
    String service = Utils.getUniqueString("test-override-service");

    publish(testPublisher, false, cluster, service, D2ServiceRole.STORAGE_SERVER, immutableOverrides);

    ServiceProperties policy = D2TestUtils.getD2ServiceProperties(zkServer.getAddress(), service);
    assertEquals(policy.getLoadBalancerStrategyProperties().get(HTTP_LB_QUARANTINE_MAX_PERCENT), legacyCap);
    assertEquals(policy.getLoadBalancerStrategyProperties().get(HTTP_LB_QUARANTINE_METHOD), "OPTIONS:/custom-legacy");
    D2QuarantineProperties relative = getRelativeQuarantine(policy);
    assertEquals(relative.getHealthCheckMethod(), HttpMethod.GET);
    assertEquals(relative.getHealthCheckPath(), "/custom-relative");
    assertRecognizedCaps(policy, Double.parseDouble(legacyCap.toString()), Double.parseDouble(relativeCap.toString()));
    assertEquals(policy.getTransportClientProperties().get("http.requestTimeout"), "12345");
    assertEquals(policy.getDegraderProperties().get("degrader.minCallCount"), "10");
    assertEquals(legacy.size(), 2);
    assertEquals(quarantine.size(), 2);
    assertFalse(quarantine.containsKey(QUARANTINE_HEALTH_CHECK_METHOD));
    assertEquals(immutableOverrides.get(LB_STRATEGY_PROPERTIES), legacy);
  }

  @Test(dataProvider = "publishers", timeOut = 60 * Time.MS_PER_SECOND)
  public void testOneStrategyOverrideKeepsOtherStrategyDefaults(boolean testPublisher) {
    Map<String, Object> overrides = Collections.singletonMap(
        RELATIVE_STRATEGY_PROPERTIES,
        Collections
            .singletonMap(QUARANTINE_PROPERTIES, Collections.singletonMap(QUARANTINE_HEALTH_CHECK_METHOD, "OPTIONS")));
    String service = Utils.getUniqueString("test-partial-service");

    publish(
        testPublisher,
        false,
        Utils.getUniqueString("test-partial-cluster"),
        service,
        D2ServiceRole.CONTROLLER,
        overrides);

    ServiceProperties policy = D2TestUtils.getD2ServiceProperties(zkServer.getAddress(), service);
    assertRecognizedCaps(policy, 0.1, 0.1);
    assertEquals(policy.getLoadBalancerStrategyProperties().get(HTTP_LB_QUARANTINE_METHOD), "GET:/health");
    assertEquals(getRelativeQuarantine(policy).getHealthCheckMethod(), HttpMethod.OPTIONS);
    assertEquals(getRelativeQuarantine(policy).getHealthCheckPath(), "/health");
  }

  @DataProvider
  public Object[][] invalidPublicationArguments() {
    return new Object[][] { { false, false, D2ServiceRole.CONTROLLER }, { true, false, D2ServiceRole.CONTROLLER },
        { true, true, D2ServiceRole.CONTROLLER }, { true, false, null }, { true, true, null } };
  }

  @Test(dataProvider = "invalidPublicationArguments", timeOut = 60 * Time.MS_PER_SECOND)
  public void testInvalidArgumentsFailBeforePublishingAndAllowClusterRecovery(
      boolean testPublisher,
      boolean autoCluster,
      D2ServiceRole role) throws Exception {
    Map<String, Object> overrides = role == null
        ? Collections.emptyMap()
        : Collections.singletonMap(RELATIVE_STRATEGY_PROPERTIES, Collections.singletonMap(QUARANTINE_PROPERTIES, null));
    String service = Utils.getUniqueString("test-invalid-service");
    String rejectedCluster = Utils.getUniqueString("test-invalid-cluster");

    VeniceException exception = expectThrows(VeniceException.class, () -> {
      if (autoCluster) {
        D2TestUtils.setupD2Config(zkServer.getAddress(), false, service, role, overrides);
      } else {
        publish(testPublisher, false, rejectedCluster, service, role, overrides);
      }
    });

    String invalidKey = role == null ? "role" : RELATIVE_STRATEGY_PROPERTIES + "." + QUARANTINE_PROPERTIES;
    assertTrue(exception.getMessage().contains(invalidKey), exception.getMessage());
    assertNull(zooKeeper.exists(servicePath(service), false));

    String correctedCluster = rejectedCluster + "-corrected";
    publish(testPublisher, false, correctedCluster, service, D2ServiceRole.CONTROLLER, Collections.emptyMap());
    assertEquals(D2TestUtils.getD2ServiceProperties(zkServer.getAddress(), service).getClusterName(), correctedCluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testTestPublisherReusesClusterAndRejectsConflictingCluster() throws Exception {
    String service = Utils.getUniqueString("test-cached-service");
    String cluster = D2TestUtils.setupD2Config(zkServer.getAddress(), false, service);
    assertEquals(
        D2TestUtils
            .setupD2Config(zkServer.getAddress(), false, service, D2ServiceRole.CONTROLLER, Collections.emptyMap()),
        cluster);
    D2TestUtils.setupD2Config(zkServer.getAddress(), false, cluster, service);
    byte[] before = zooKeeper.getData(servicePath(service), false, null);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> D2TestUtils.setupD2Config(zkServer.getAddress(), false, cluster + "-different", service));

    assertTrue(exception.getMessage().contains("Same D2 service attempted to register to multiple D2 clusters."));
    assertEquals(zooKeeper.getData(servicePath(service), false, null), before);
    assertEquals(D2TestUtils.getD2ServiceProperties(zkServer.getAddress(), service).getClusterName(), cluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testClientAndAnnouncementDoNotRewriteSeededServicePolicy() throws Exception {
    String cluster = Utils.getUniqueString("test-owned-cluster");
    String service = Utils.getUniqueString("test-owned-service");
    D2ConfigUtils.setupD2Config(zkServer.getAddress(), false, cluster, service);
    Map<String, Object> seeded = new HashMap<>(D2TestUtils.getD2ServiceDefaults());
    seeded.put(PropertyKeys.SERVICE_NAME, service);
    seeded.put(PropertyKeys.CLUSTER_NAME, cluster);
    seeded.put(PropertyKeys.PATH, "/owner-path");
    seeded.put(LB_STRATEGY_PROPERTIES, Collections.singletonMap(HTTP_LB_QUARANTINE_MAX_PERCENT, "0"));
    seeded.put(
        RELATIVE_STRATEGY_PROPERTIES,
        Collections.singletonMap(QUARANTINE_PROPERTIES, Collections.singletonMap(QUARANTINE_MAX_PERCENT, "0")));
    ServicePropertiesJsonSerializer serializer = new ServicePropertiesJsonSerializer();
    byte[] seededBytes = serializer.toBytes(serializer.fromMap(seeded));
    Stat seededStat = zooKeeper.setData(servicePath(service), seededBytes, -1);
    D2ClientFactory.getD2Client(zkServer.getAddress(), Optional.empty());
    D2Server announcer =
        D2ConfigUtils.createD2Server(zkServer.getAddress(), "http://localhost:" + TestUtils.getFreePort(), cluster);
    try {
      announcer.forceStart();

      Stat after = new Stat();
      assertEquals(zooKeeper.getData(servicePath(service), false, after), seededBytes);
      assertEquals(after.getVersion(), seededStat.getVersion());
    } finally {
      try {
        announcer.notifyShutdown();
      } finally {
        D2ClientFactory.release(zkServer.getAddress());
      }
    }
  }

  private void publish(
      boolean testPublisher,
      boolean https,
      String cluster,
      String service,
      D2ServiceRole role,
      Map<String, Object> overrides) {
    if (testPublisher) {
      if (role == D2ServiceRole.GENERIC && overrides.isEmpty()) {
        D2TestUtils.setupD2Config(zkServer.getAddress(), https, cluster, service);
      } else {
        D2TestUtils.setupD2Config(zkServer.getAddress(), https, cluster, service, role, overrides);
      }
    } else if (role == D2ServiceRole.GENERIC && overrides.isEmpty()) {
      D2ConfigUtils.setupD2Config(zkServer.getAddress(), https, cluster, service);
    } else {
      D2ConfigUtils.setupD2Config(zkServer.getAddress(), https, cluster, service, role, overrides);
    }
  }

  private static void assertRecognizedCaps(ServiceProperties policy, double legacyCap, double relativeCap) {
    assertEquals(
        LoadBalancerStrategyPropertiesConverter.toConfig(policy.getLoadBalancerStrategyProperties())
            .getQuarantineCfg()
            .getQuarantineMaxPercent(),
        Double.valueOf(legacyCap));
    assertEquals(getRelativeQuarantine(policy).getQuarantineMaxPercent(), Double.valueOf(relativeCap));
    assertEquals(
        new DegraderLoadBalancerStrategyFactoryV3().newLoadBalancer(policy).getConfig().getQuarantineMaxPercent(),
        Math.min(legacyCap, 0.5));
  }

  private static D2QuarantineProperties getRelativeQuarantine(ServiceProperties policy) {
    return RelativeStrategyPropertiesConverter.toProperties(policy.getRelativeStrategyProperties())
        .getQuarantineProperties();
  }

  private static String servicePath(String service) {
    return ZKFSUtil.servicePath("/d2") + "/" + service;
  }
}

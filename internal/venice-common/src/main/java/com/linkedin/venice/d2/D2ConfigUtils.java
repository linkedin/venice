package com.linkedin.venice.d2;

import com.linkedin.d2.HttpMethod;
import com.linkedin.d2.balancer.config.ConfigWriter;
import com.linkedin.d2.balancer.properties.PropertyKeys;
import com.linkedin.d2.balancer.servers.ZKUriStoreFactory;
import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import com.linkedin.d2.balancer.servers.ZooKeeperServer;
import com.linkedin.d2.discovery.util.D2Config;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Utility class for setting up D2 service discovery configuration and creating D2 server announcers.
 * This extracts production-ready D2 setup logic for use outside of test contexts.
 */
public final class D2ConfigUtils {
  private static final String DEFAULT_QUARANTINE_MAX_PERCENT = "0.1";

  public enum D2ServiceRole {
    GENERIC, CONTROLLER, STORAGE_SERVER
  }

  private D2ConfigUtils() {
  }

  public static void setupD2Config(String zkHosts, boolean https, String d2ClusterName, String d2ServiceName) {
    setupD2Config(zkHosts, https, d2ClusterName, d2ServiceName, D2ServiceRole.GENERIC, Collections.emptyMap());
  }

  /**
   * Publishes a Venice-owned policy, with service overrides taking precedence over defaults.
   * This does not read or merge an existing externally managed policy.
   */
  public static void setupD2Config(
      String zkHosts,
      boolean https,
      String d2ClusterName,
      String d2ServiceName,
      D2ServiceRole role,
      Map<String, Object> serviceOverrides) {
    int sessionTimeout = 5000;
    String basePath = "/d2";
    int retryLimit = 10;

    try {
      Map<String, Object> clusterDefaults = Collections.emptyMap();
      Map<String, Object> serviceDefaults = getD2ServiceDefaults();
      Map<String, Object> clusterServiceConfigurations =
          getD2ServiceConfig(d2ClusterName, d2ServiceName, https, role, serviceOverrides);
      Map<String, Object> extraClusterServiceConfigurations = Collections.emptyMap();
      Map<String, Object> serviceVariants = Collections.emptyMap();

      D2Config d2Config = new D2Config(
          zkHosts,
          sessionTimeout,
          basePath,
          sessionTimeout,
          retryLimit,
          clusterDefaults,
          serviceDefaults,
          clusterServiceConfigurations,
          extraClusterServiceConfigurations,
          serviceVariants);

      d2Config.configure();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  /**
   * Completes only missing quarantine leaves, without modifying or sharing the caller's quarantine maps.
   * D2 accepts caps as {@link Double} or numeric {@link String} values. Generic services retain D2's
   * OPTIONS/service-path probe defaults; controllers and storage servers default to GET /health.
   */
  public static Map<String, Object> withQuarantineDefaults(Map<String, Object> serviceProperties, D2ServiceRole role) {
    if (serviceProperties == null) {
      throw new VeniceException("D2 service properties must not be null");
    }
    if (role == null) {
      throw new VeniceException("D2 service role must not be null");
    }

    Map<String, Object> legacy =
        getPropertyMap(serviceProperties, PropertyKeys.LB_STRATEGY_PROPERTIES, PropertyKeys.LB_STRATEGY_PROPERTIES);
    String relativePath = PropertyKeys.RELATIVE_STRATEGY_PROPERTIES;
    Map<String, Object> relative = getPropertyMap(serviceProperties, relativePath, relativePath);
    String quarantinePath = relativePath + "." + PropertyKeys.QUARANTINE_PROPERTIES;
    Map<String, Object> quarantine = getPropertyMap(relative, PropertyKeys.QUARANTINE_PROPERTIES, quarantinePath);
    validateCap(legacy, PropertyKeys.HTTP_LB_QUARANTINE_MAX_PERCENT, PropertyKeys.LB_STRATEGY_PROPERTIES);
    validateString(legacy, PropertyKeys.HTTP_LB_QUARANTINE_METHOD, PropertyKeys.LB_STRATEGY_PROPERTIES);
    validateCap(quarantine, PropertyKeys.QUARANTINE_MAX_PERCENT, quarantinePath);
    validateString(quarantine, PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD, quarantinePath);
    validateString(quarantine, PropertyKeys.QUARANTINE_HEALTH_CHECK_PATH, quarantinePath);
    if (quarantine.containsKey(PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD)) {
      String method = (String) quarantine.get(PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD);
      if (!HttpMethod.GET.name().equalsIgnoreCase(method) && !HttpMethod.OPTIONS.name().equalsIgnoreCase(method)) {
        throw new VeniceException(
            "Invalid D2 property " + quarantinePath + "." + PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD
                + ": expected GET or OPTIONS");
      }
    }

    Map<String, Object> legacyDefaults = new HashMap<>();
    legacyDefaults.put(PropertyKeys.HTTP_LB_QUARANTINE_MAX_PERCENT, DEFAULT_QUARANTINE_MAX_PERCENT);
    Map<String, Object> quarantineDefaults = new HashMap<>();
    quarantineDefaults.put(PropertyKeys.QUARANTINE_MAX_PERCENT, DEFAULT_QUARANTINE_MAX_PERCENT);
    if (role != D2ServiceRole.GENERIC) {
      legacyDefaults.put(PropertyKeys.HTTP_LB_QUARANTINE_METHOD, "GET:/health");
      quarantineDefaults.put(PropertyKeys.QUARANTINE_HEALTH_CHECK_METHOD, HttpMethod.GET.name());
      quarantineDefaults.put(PropertyKeys.QUARANTINE_HEALTH_CHECK_PATH, "/health");
    }
    Map<String, Object> defaults = new HashMap<>();
    defaults.put(PropertyKeys.LB_STRATEGY_PROPERTIES, legacyDefaults);
    defaults.put(
        PropertyKeys.RELATIVE_STRATEGY_PROPERTIES,
        Collections.singletonMap(PropertyKeys.QUARANTINE_PROPERTIES, quarantineDefaults));
    return ConfigWriter.merge(serviceProperties, defaults);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> getPropertyMap(Map<String, Object> properties, String key, String path) {
    if (!properties.containsKey(key)) {
      return Collections.emptyMap();
    }
    Object value = properties.get(key);
    if (!(value instanceof Map)) {
      throw new VeniceException("Invalid D2 property " + path + ": expected a map");
    }
    return (Map<String, Object>) value;
  }

  private static void validateCap(Map<String, Object> properties, String key, String path) {
    if (!properties.containsKey(key)) {
      return;
    }
    Object value = properties.get(key);
    double cap;
    if (value instanceof Double) {
      cap = (Double) value;
    } else if (value instanceof String) {
      try {
        cap = Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        throw new VeniceException("Invalid D2 property " + path + "." + key + ": expected a numeric String", e);
      }
    } else {
      throw new VeniceException("Invalid D2 property " + path + "." + key + ": expected a Double or numeric String");
    }
    if (!Double.isFinite(cap)) {
      throw new VeniceException("Invalid D2 property " + path + "." + key + ": expected a finite cap");
    }
  }

  private static void validateString(Map<String, Object> properties, String key, String path) {
    if (properties.containsKey(key) && !(properties.get(key) instanceof String)) {
      throw new VeniceException("Invalid D2 property " + path + "." + key + ": expected a String");
    }
  }

  public static D2Server createD2Server(String zkHosts, String localUri, String clusterName) {
    int sessionTimeout = 5000;
    String basePath = "/d2";

    ZKUriStoreFactory storeFactory = new ZKUriStoreFactory();
    ZooKeeperAnnouncer announcer = new ZooKeeperAnnouncer(new ZooKeeperServer());
    announcer.setCluster(clusterName);
    announcer.setUri(localUri);
    announcer.setWeight(1);
    ZooKeeperConnectionManager zkManager =
        new ZooKeeperConnectionManager(zkHosts, sessionTimeout, basePath, storeFactory, announcer);
    long startupTimeoutMillis = 5000;
    boolean continueIfStartupFails = false;
    long shutdownTimeoutMillis = 5000;
    boolean continueIfShutdownFails = true;
    boolean doNotStart = false;
    boolean delayStart = true;
    boolean initMarkUp = true;
    boolean healthCheckEnabled = false;
    long healthCheckInterval = 1000;
    int healthCheckRetries = 3;
    String healthCheckUrl = "";
    int d2HealthCheckerTimeoutMs = 500;

    D2ServerManager d2Manager = new D2ServerManager(
        zkManager,
        startupTimeoutMillis,
        continueIfStartupFails,
        shutdownTimeoutMillis,
        continueIfShutdownFails,
        doNotStart,
        delayStart,
        initMarkUp,
        healthCheckEnabled,
        healthCheckInterval,
        healthCheckRetries,
        healthCheckUrl,
        null,
        d2HealthCheckerTimeoutMs);

    return new D2Server(d2Manager);
  }

  public static List<ServiceDiscoveryAnnouncer> getD2Servers(
      String zkHosts,
      String d2ClusterName,
      String... localUris) {
    List<ServiceDiscoveryAnnouncer> d2List = new ArrayList<>();
    for (String localUri: localUris) {
      D2Server d2 = createD2Server(zkHosts, localUri, d2ClusterName);
      d2List.add(d2);
    }
    return d2List;
  }

  private static Map<String, Object> getD2ServiceDefaults() {
    List<String> loadBalancerStrategyList = new ArrayList<>();
    loadBalancerStrategyList.add("degraderV3");
    loadBalancerStrategyList.add("degraderV2");

    List<String> prioritizedSchemes = new ArrayList<>();
    prioritizedSchemes.add("http");
    prioritizedSchemes.add("https");

    Map<String, Object> loadBalancerStrategyProperties = new HashMap<>();
    loadBalancerStrategyProperties.put("http.loadBalancer.updateIntervalMs", "5000");
    loadBalancerStrategyProperties.put("http.loadBalancer.pointsPerWeight", "100");
    loadBalancerStrategyProperties.put("http.loadBalancer.consistentHashAlgorithm", "pointBased");

    Map<String, String> transportClientProperties = new HashMap<>();
    transportClientProperties.put("http.requestTimeout", "10000");

    Map<String, String> degraderProperties = new HashMap<>();
    degraderProperties.put("degrader.minCallCount", "10");
    degraderProperties.put("degrader.lowErrorRate", "0.01");
    degraderProperties.put("degrader.highErrorRate", "0.1");

    Map<String, Object> serviceDefaults = new HashMap<>();
    serviceDefaults.put("loadBalancerStrategyList", loadBalancerStrategyList);
    serviceDefaults.put("prioritizedSchemes", prioritizedSchemes);
    serviceDefaults.put("loadBalancerStrategyProperties", loadBalancerStrategyProperties);
    serviceDefaults.put("transportClientProperties", transportClientProperties);
    serviceDefaults.put("degraderProperties", degraderProperties);

    return withQuarantineDefaults(serviceDefaults, D2ServiceRole.GENERIC);
  }

  private static Map<String, Object> getD2ServiceConfig(
      String cluster,
      String service,
      boolean https,
      D2ServiceRole role,
      Map<String, Object> serviceOverrides) {
    Map<String, Object> serviceMap = new HashMap<>();
    serviceMap.put("path", "/");
    if (https) {
      serviceMap.put("prioritizedSchemes", Arrays.asList("https", "http"));
    } else {
      serviceMap.put("prioritizedSchemes", Arrays.asList("http"));
    }

    Map<String, Object> servicesMap = new HashMap<>();
    servicesMap.put(service, ConfigWriter.merge(withQuarantineDefaults(serviceOverrides, role), serviceMap));

    Map<String, Object> clusterMap = new HashMap<>();
    clusterMap.put("services", servicesMap);

    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put(cluster, clusterMap);

    return serviceConfig;
  }
}

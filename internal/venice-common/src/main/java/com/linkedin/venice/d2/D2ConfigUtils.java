package com.linkedin.venice.d2;

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
  private D2ConfigUtils() {
  }

  public static void setupD2Config(String zkHosts, boolean https, String d2ClusterName, String d2ServiceName) {
    int sessionTimeout = 5000;
    String basePath = "/d2";
    int retryLimit = 10;

    try {
      Map<String, Object> clusterDefaults = Collections.emptyMap();
      Map<String, Object> serviceDefaults = getD2ServiceDefaults();
      Map<String, Object> clusterServiceConfigurations = getD2ServiceConfig(d2ClusterName, d2ServiceName, https);
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

    return serviceDefaults;
  }

  private static Map<String, Object> getD2ServiceConfig(String cluster, String service, boolean https) {
    Map<String, Object> serviceMap = new HashMap<>();
    serviceMap.put("path", "/");
    if (https) {
      serviceMap.put("prioritizedSchemes", Arrays.asList("https", "http"));
    } else {
      serviceMap.put("prioritizedSchemes", Arrays.asList("http"));
    }

    Map<String, Object> servicesMap = new HashMap<>();
    servicesMap.put(service, serviceMap);

    Map<String, Object> clusterMap = new HashMap<>();
    clusterMap.put("services", servicesMap);

    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put(cluster, clusterMap);

    return serviceConfig;
  }
}

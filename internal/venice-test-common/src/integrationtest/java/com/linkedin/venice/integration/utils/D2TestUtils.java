package com.linkedin.venice.integration.utils;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.d2.balancer.servers.ZKUriStoreFactory;
import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import com.linkedin.d2.balancer.servers.ZooKeeperServer;
import com.linkedin.d2.discovery.util.D2Config;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.d2.D2Server;
import com.linkedin.venice.d2.D2ServerManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class D2TestUtils {
  private static final Map<String, String> D2_SERVICE_TO_CLUSTER = new HashMap<>();

  /**
   * In our setup, Routers from different Venice clusters announce to different D2 services. Hence, we need to use
   * different D2 clusters for each service name so as not to route requests to Routers in other clusters. This function
   * creates a cluster name for each service and returns the created name.
   */
  public static String setupD2Config(String zkHosts, boolean https, String d2ServiceName) {
    // d2ClusterName must be different for routers with different d2ServiceName
    // We've consciously not set it to be deterministically derivable from service name to make sure we don't rely on
    // hardcoded values.
    String d2ClusterName =
        D2_SERVICE_TO_CLUSTER.computeIfAbsent(d2ServiceName, s -> Utils.getUniqueString(d2ServiceName + "_cluster"));
    setupD2Config(zkHosts, https, d2ClusterName, d2ServiceName);
    return d2ClusterName;
  }

  public static void setupD2Config(String zkHosts, boolean https, String d2ClusterName, String d2ServiceName) {
    String d2ClusterNameFromCache = D2_SERVICE_TO_CLUSTER.get(d2ServiceName);
    if (d2ClusterNameFromCache == null) {
      D2_SERVICE_TO_CLUSTER.put(d2ServiceName, d2ClusterName);
    } else if (!d2ClusterNameFromCache.equals(d2ClusterName)) {
      throw new VeniceException(
          new StringBuilder("Same D2 service attempted to register to multiple D2 clusters.")
              .append(" Already registered cluster: ")
              .append(d2ClusterNameFromCache)
              .append(". Newly attempted cluster: ")
              .append(d2ClusterName)
              .toString());
    }

    int sessionTimeout = 5000;
    String basePath = "/d2";
    int retryLimit = 10;

    D2Config d2Config;
    try {
      Map<String, Object> clusterDefaults = Collections.EMPTY_MAP;
      Map<String, Object> serviceDefaults = getD2ServiceDefaults();
      Map<String, Object> clusterServiceConfigurations = getD2ServiceConfig(d2ClusterName, d2ServiceName, https);
      Map<String, Object> extraClusterServiceConfigurations = Collections.EMPTY_MAP;
      Map<String, Object> serviceVariants = Collections.EMPTY_MAP;

      d2Config = new D2Config(
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

      // populate zookeeper
      d2Config.configure();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public static String getRandomD2ServiceName(Map<String, String> clusterToD2, String clusterName) {
    String defaultD2Service = Utils.getUniqueString(clusterName + "_d2");
    if (clusterToD2 == null) {
      return defaultD2Service;
    }
    return clusterToD2.getOrDefault(clusterName, defaultD2Service);
  }

  public static D2Server createD2Server(String zkHosts, String localUri, String clusterName) {
    int sessionTimeout = 5000;
    String basePath = "/d2";

    // Set up D2 server/announcer
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

    D2Server d2 = new D2Server(d2Manager);
    return d2;
  }

  /**
   * @param zkHosts
   * @param localUris varags if we want to announce on multiple uris (for example on an http port and https port)
   * @return
   */
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

  public static D2Client getAndStartD2Client(String zkHosts) {
    return getAndStartD2Client(zkHosts, false);
  }

  public static D2Client getAndStartHttpsD2Client(String zkHosts) {
    return getAndStartD2Client(zkHosts, true);
  }

  public static D2Client getD2Client(String zkHosts, boolean https) {
    return getD2Client(zkHosts, https, HttpProtocolVersion.HTTP_1_1);
  }

  public static D2Client getD2Client(String zkHosts, boolean https, HttpProtocolVersion httpProtocolVersion) {
    int sessionTimeout = 5000;
    String basePath = "/d2";

    if (httpProtocolVersion.equals(HttpProtocolVersion.HTTP_2) && !https) {
      throw new VeniceException("Param 'https' needs to be 'true' when enabling http/2");
    }
    Map<String, TransportClientFactory> transportClients = new HashMap<>();
    TransportClientFactory httpTransport =
        new HttpClientFactory.Builder().setUsePipelineV2(true).setDefaultHttpVersion(httpProtocolVersion).build();
    transportClients.put("http", httpTransport);
    transportClients.put("https", httpTransport);

    D2ClientBuilder builder = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setZkStartupTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setLbWaitTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setBasePath(basePath)
        .setClientFactories(transportClients);

    if (https) {
      SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
      builder.setSSLContext(sslFactory.getSSLContext())
          .setSSLParameters(sslFactory.getSSLParameters())
          .setIsSSLEnabled(true);
    }

    return builder.build();
  }

  public static void startD2Client(D2Client d2Client) {
    CountDownLatch latch = new CountDownLatch(1);
    d2Client.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        throw new RuntimeException("d2client throws error on startup", e);
      }

      @Override
      public void onSuccess(None result) {
        latch.countDown();
      }
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new VeniceException(e);
    }
  }

  public static D2Client getAndStartD2Client(String zkHosts, boolean https) {
    D2Client d2Client = getD2Client(zkHosts, https);
    startD2Client(d2Client);
    return d2Client;
  }

  /**
   * @see <a href="https://github.com/linkedin/rest.li/blob/master/examples/d2-quickstart/config/src/main/d2Config/d2Config.json">D2 Quickstart</a>
   */
  public static Map<String, Object> getD2ServiceDefaults() {

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

  /**
   * Generates a nested map like the following (if cluster=="VeniceStorageService", service=="venice-service", https==true)
   *
   * {
   *   "VeniceStorageService" : {
   *     "services" : {
   *       "venice-service" : {
   *         "path" : "/",
   *         "prioritizedSchemes" : "https"
   *       }
   *     }
   *   }
   * }
   *
   * @param cluster D2 name for the cluster of servers that provide a common set of services (this function only
   *                supports one service)
   * @param service Name of the service provided by this cluster
   * @param https   true if this service should only be queried with https
   * @return a map of cluster name to config object (which is itself a map containing more maps)
   *
   * @see <a href="https://github.com/linkedin/rest.li/blob/master/examples/d2-quickstart/config/src/main/d2Config/d2Config.json">D2 Quickstart</a>
   */
  public static Map<String, Object> getD2ServiceConfig(String cluster, String service, boolean https) {
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

package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.client.store.ClientConfig.*;

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
import com.linkedin.venice.utils.SslUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;


public class D2TestUtils {
  public static final String DEFAULT_TEST_CLUSTER_NAME = "VeniceStorageService";
  public static final String DEFAULT_TEST_SERVICE_NAME = DEFAULT_D2_SERVICE_NAME;

  public static final String CONTROLLER_CLUSTER_NAME = "VeniceController";
  public static final String CONTROLLER_SERVICE_NAME = "VeniceController";

  public static void setupD2Config(String zkHosts, boolean https) {
    setupD2Config(zkHosts, https, DEFAULT_TEST_CLUSTER_NAME, DEFAULT_TEST_SERVICE_NAME, false);
  }

  public static void setupD2Config(
      String zkHosts,
      boolean https,
      String clusterName,
      String serviceName,
      boolean stickyRoutingForSingleGet) {
    int sessionTimeout = 5000;
    String basePath = "/d2";
    int retryLimit = 10;

    D2Config d2Config;
    try {
      Map<String, Object> clusterDefaults = Collections.EMPTY_MAP;
      Map<String, Object> serviceDefaults = getD2ServiceDefaults();
      Map<String, Object> clusterServiceConfigurations =
          getD2ServiceConfig(clusterName, serviceName, https, stickyRoutingForSingleGet);
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

      d2Config.configure();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public static String getD2ServiceName(String clusterToD2, String clusterName) {
    String d2 = null;
    if (!StringUtils.isEmpty(clusterToD2)) {
      d2 = clusterToD2.substring(clusterToD2.indexOf(clusterName) + clusterName.length() + 1);
      int end = d2.indexOf(",");
      if (end > 0) {
        d2 = d2.substring(0, end);
      }
    }
    return d2 == null ? DEFAULT_TEST_SERVICE_NAME : d2;
  }

  private static D2Server getD2Server(String zkHosts, String localUri) {
    return getD2Server(zkHosts, localUri, DEFAULT_TEST_CLUSTER_NAME);
  }

  public static D2Server getD2Server(String zkHosts, String localUri, String clusterName) {
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
  public static List<D2Server> getD2Servers(String zkHosts, String... localUris) {
    List<D2Server> d2List = new ArrayList<>();
    for (String localUri: localUris) {
      D2Server d2 = getD2Server(zkHosts, localUri);
      d2List.add(d2);
    }
    return d2List;
  }

  public static List<D2Server> getD2Servers(String zkHosts, String[] localUris, String clusterName) {
    List<D2Server> d2List = new ArrayList<>();
    for (String localUri: localUris) {
      D2Server d2 = getD2Server(zkHosts, localUri, clusterName);
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
   * @param cluster D2 name for the cluster of servers that provide a common set of services (this function only supports one service)
   * @param service Name of the service provided by this cluster
   * @param https true if this service should only be queried with https
   * @param stickyRouting if the service enables the sticky routing for single-get
   * @return
   *
   * @see <a href="https://github.com/linkedin/rest.li/blob/master/examples/d2-quickstart/config/src/main/d2Config/d2Config.json">D2 Quickstart</a>
   */
  public static Map<String, Object> getD2ServiceConfig(
      String cluster,
      String service,
      boolean https,
      boolean stickyRouting) {
    Map<String, Object> serviceMap = new HashMap<>();
    serviceMap.put("path", "/");
    if (https) {
      serviceMap.put("prioritizedSchemes", Arrays.asList("https", "http"));
    } else {
      serviceMap.put("prioritizedSchemes", Arrays.asList("http"));
    }

    if (stickyRouting) {
      /**
       * Check this doc:
       * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/D2+User+Manual#D2UserManual-Stickiness(howtoconfigureanduse)
       */
      Map<String, Object> loadBalancerStrategyProperties = new HashMap<>();
      loadBalancerStrategyProperties.put("http.loadBalancer.hashMethod", "uriRegex");
      Map<String, Object> hashConfig = new HashMap<>();
      List<String> regexes = new ArrayList<>();
      regexes.add("/storage/(.*)\\?f=b64");
      hashConfig.put("regexes", regexes);
      hashConfig.put("warnOnNoMatch", "false");
      loadBalancerStrategyProperties.put("http.loadBalancer.hashConfig", hashConfig);
      serviceMap.put("loadBalancerStrategyProperties", loadBalancerStrategyProperties);
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

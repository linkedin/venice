package com.linkedin.venice.integration.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.d2.balancer.servers.ZKUriStoreFactory;
import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import com.linkedin.d2.balancer.servers.ZooKeeperServer;
import com.linkedin.d2.discovery.util.D2Config;
import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.d2.spring.D2ServerManager;
import com.linkedin.venice.exceptions.VeniceException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class D2TestUtils {
  public static final String D2_SERVICE_NAME = "venice-service";
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void assignLocalUriToD2Servers(List<D2Server> d2ServerList, int port){
    String localUri = "http://localhost:" + port;
    for (D2Server server : d2ServerList){
      ZooKeeperAnnouncer[] announcers = server.getZkAnnouncers();
      for (int i=0; i<announcers.length; i++){
        announcers[i].setUri(localUri);
      }
    }
  }

  public static void setupD2Config(String zkHosts){

    int sessionTimeout = 5000;
    String basePath = "/d2";
    int retryLimit = 10;

    D2Config d2Config;
    try {
      Map<String, Object> clusterDefaults = Collections.EMPTY_MAP;
      // see: https://github.com/linkedin/rest.li/blob/master/examples/d2-quickstart/config/src/main/d2Config/d2Config.json
      String serviceDefaultsJson =
          "{\"loadBalancerStrategyList\":[\"degraderV3\",\"degraderV2\"],\"prioritizedSchemes\":[\"http\"],\"loadBalancerStrategyProperties\":{\"http.loadBalancer.updateIntervalMs\":\"5000\",\"http.loadBalancer.pointsPerWeight\":\"100\"},\"transportClientProperties\":{\"http.requestTimeout\":\"10000\"},\"degraderProperties\":{\"degrader.minCallCount\":\"10\",\"degrader.lowErrorRate\":\"0.01\",\"degrader.highErrorRate\":\"0.1\"}}";
      Map<String, Object> serviceDefaults = mapper.readValue(serviceDefaultsJson, Map.class);
      /*
       * The serviceConfig sets up a d2 cluster called "VeniceStorageService"
       * It has one service called "venice-service"
       * The "path" field of "/" maps requests of the form d2://venice-service/bar to http://host:port/bar
       *   if the "path" field was "/foo" then it would map d2://venice-server/bar to http://host:port/foo/bar
       */
      String serviceConfigJson = "{\"VeniceStorageService\":{\"services\":{\"" + D2_SERVICE_NAME + "\":{\"path\":\"/\"}}}}";
      Map<String, Object> clusterServiceConfigurations = mapper.readValue(serviceConfigJson, Map.class);
      Map<String, Object> extraClusterServiceConfigurations = Collections.EMPTY_MAP;
      Map<String, Object> serviceVariants = Collections.EMPTY_MAP;

      d2Config =
          new D2Config(zkHosts, sessionTimeout, basePath, sessionTimeout, retryLimit, clusterDefaults, serviceDefaults,
              clusterServiceConfigurations, extraClusterServiceConfigurations, serviceVariants);

      d2Config.configure();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public static List<D2Server> getD2Servers(String zkHosts){

    int sessionTimeout = 5000;
    String basePath = "/d2";

    // Set up D2 server/announcer
    ZKUriStoreFactory storeFactory = new ZKUriStoreFactory();
    ZooKeeperAnnouncer announcer = new ZooKeeperAnnouncer(new ZooKeeperServer());
    announcer.setCluster("VeniceStorageService");
    announcer.setUri("bogus-uri-that-gets-replaced");
    announcer.setWeight(1);
    ZooKeeperConnectionManager zkManager = new ZooKeeperConnectionManager(
        zkHosts,
        sessionTimeout,
        basePath,
        storeFactory,
        announcer);
    long startupTimeoutMillis = 5000;
    boolean continueIfStartupFails = false;
    long shutdownTimeoutMillis = 5000;
    boolean continueIfShutdownFails = true;
    boolean doNotStart = false;
    boolean delayStart = true;
    boolean healthCheckEnabled = false;
    long healthCheckInterval = 1000;
    int healthCheckRetries = 3;
    String healthCheckUrl = "";
    ScheduledExecutorService scheduledExecutorService = null;
    int d2HealthCheckerTimeoutMs = 500;

    D2ServerManager d2Manager = new D2ServerManager(
        zkManager,
        startupTimeoutMillis,
        continueIfStartupFails,
        shutdownTimeoutMillis,
        continueIfShutdownFails,
        doNotStart,
        delayStart,
        healthCheckEnabled,
        healthCheckInterval,
        healthCheckRetries,
        healthCheckUrl,
        scheduledExecutorService,
        d2HealthCheckerTimeoutMs);

    D2Server d2 = new D2Server(d2Manager);
    List<D2Server> d2List = new ArrayList<>();
    d2List.add(d2);
    return d2List;
  }

  public static D2Client getAndStartD2Client(String zkHosts) {
    int sessionTimeout = 5000;
    String basePath = "/d2";
    D2Client d2Client =
        new D2ClientBuilder().setZkHosts(zkHosts).setZkSessionTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
            .setZkStartupTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
            .setLbWaitTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
            .setBasePath(basePath)
            .build();

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
    return d2Client;
  }
}

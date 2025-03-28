package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.MAX_READ_CAPACITY;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_SSL_HANDSHAKE_THREADS;
import static com.linkedin.venice.ConfigKeys.ROUTER_CONNECTION_LIMIT;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK;
import static com.linkedin.venice.ConfigKeys.ROUTER_HTTP_CLIENT_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION;
import static com.linkedin.venice.ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE;
import static com.linkedin.venice.ConfigKeys.ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.ROUTER_RESOLVE_THREADS;
import static com.linkedin.venice.ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE;
import static com.linkedin.venice.ConfigKeys.SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER;
import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_BUCKETS;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_SCALE;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE;
import static com.linkedin.venice.stats.VeniceMetricsConfig.OTEL_VENICE_METRICS_ENABLED;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.tehuti.MetricsAware;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper for the {@link RouterServer}.
 */
public class VeniceRouterWrapper extends ProcessWrapper implements MetricsAware {
  public static final String SERVICE_NAME = "VeniceRouter";
  public static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME =
      ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "_test";
  private static final String ROUTER_SERVICE_NAME = "venice-router";
  private static final String ROUTER_SERVICE_METRIC_PREFIX = "router";
  private final VeniceProperties properties;
  private final String zkAddress;
  private RouterServer routerServer;
  private final String d2ClusterName;
  private final String clusterDiscoveryD2ClusterName;
  private final String regionName;

  private static final Logger LOGGER = LogManager.getLogger(VeniceRouterWrapper.class);

  VeniceRouterWrapper(
      String regionName,
      String serviceName,
      File dataDirectory,
      RouterServer routerServer,
      VeniceProperties properties,
      String zkAddress,
      String d2ClusterName,
      String clusterDiscoveryD2ClusterName) {
    super(serviceName, dataDirectory);
    this.routerServer = routerServer;
    this.properties = properties;
    this.zkAddress = zkAddress;
    this.d2ClusterName = d2ClusterName;
    this.clusterDiscoveryD2ClusterName = clusterDiscoveryD2ClusterName;
    this.regionName = Objects.requireNonNull(regionName, "Region name cannot be null for VeniceRouterWrapper");
  }

  static StatefulServiceProvider<VeniceRouterWrapper> generateService(
      String regionName,
      String clusterName,
      ZkServerWrapper zkServerWrapper,
      String veniceZkBasePath,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      boolean sslToStorageNodes,
      Map<String, String> clusterToD2,
      Map<String, String> clusterToServerD2,
      Properties properties) {
    String zkAddress = zkServerWrapper.getAddress();

    Map<String, String> finalClusterToD2;
    if (clusterToD2 == null || clusterToD2.isEmpty()) {
      finalClusterToD2 = Collections.singletonMap(clusterName, Utils.getUniqueString("router_d2_service"));
    } else if (clusterToD2.containsKey(clusterName)) {
      finalClusterToD2 = clusterToD2;
    } else {
      throw new IllegalArgumentException(
          String.format("clusterToD2 [%s] doesn't contain clusterName [%s]", clusterToD2, clusterName));
    }

    Map<String, String> finalClusterToServerD2;
    if (clusterToServerD2 == null || clusterToServerD2.isEmpty()) {
      finalClusterToServerD2 = Collections.singletonMap(clusterName, Utils.getUniqueString("server_d2_service"));
    } else if (clusterToServerD2.containsKey(clusterName)) {
      finalClusterToServerD2 = clusterToServerD2;
    } else {
      throw new IllegalArgumentException(
          String.format("clusterToServerD2 [%s] doesn't contain clusterName [%s]", clusterToServerD2, clusterName));
    }

    return (serviceName, dataDirectory) -> {
      int port;
      // If a port is specified using ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER, this port will be used,
      // as the requester will be connecting to the router via this port.
      // This method will not honor the port specified with LISTENER_PORT as it might be used by
      // VeniceServerWrapper, since it is passed via the shared properties.
      if (properties.containsKey(ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER)) {
        port = Integer.parseInt(properties.getProperty(ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER));
      } else {
        port = TestUtils.getFreePort();
      }
      int sslPort = TestUtils.getFreePort();
      PropertyBuilder builder = new PropertyBuilder().put(CLUSTER_NAME, clusterName)
          .put(LISTENER_PORT, port)
          .put(LISTENER_SSL_PORT, sslPort)
          .put(ZOOKEEPER_ADDRESS, zkAddress + veniceZkBasePath)
          .put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress())
          .put(SSL_TO_STORAGE_NODES, sslToStorageNodes)
          .put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(finalClusterToD2))
          .put(CLUSTER_TO_SERVER_D2, TestUtils.getClusterToD2String(finalClusterToServerD2))
          .put(ROUTER_CLIENT_SSL_HANDSHAKE_THREADS, 5)
          .put(ROUTER_RESOLVE_THREADS, 5)
          // Below configs are to attempt to minimize resource utilization in tests
          .put(ROUTER_CONNECTION_LIMIT, 200)
          .put(ROUTER_HTTP_CLIENT_POOL_SIZE, 2)
          .put(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 2)
          .put(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 1)
          .put(ROUTER_MAX_OUTGOING_CONNECTION, 10)
          // To speed up test
          .put(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 1)
          .put(MAX_READ_CAPACITY, DEFAULT_PER_ROUTER_READ_QUOTA)
          .put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterName)
          .put(ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT.name())
          // OpenTelemetry configs
          .put(OTEL_VENICE_METRICS_ENABLED, Boolean.TRUE.toString())
          .put(OTEL_EXPORTER_OTLP_METRICS_PROTOCOL, "http/protobuf")
          .put(OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE, "delta")
          .put(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION, "base2_exponential_bucket_histogram")
          .put(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_SCALE, 3)
          .put(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_BUCKETS, 250)
          .put(properties);

      // setup d2 config first
      String d2ServiceName = D2TestUtils.getRandomD2ServiceName(finalClusterToD2, clusterName);

      VeniceProperties routerProperties = builder.build();
      boolean https = routerProperties.getBoolean(ROUTER_HTTP2_INBOUND_ENABLED, false);
      String httpURI = "http://localhost:" + port;
      String httpsURI = "https://localhost:" + sslPort;
      List<ServiceDiscoveryAnnouncer> d2Servers = new ArrayList<>();
      String d2ClusterName = D2TestUtils.setupD2Config(zkAddress, https, d2ServiceName);
      d2Servers.addAll(D2TestUtils.getD2Servers(zkAddress, d2ClusterName, httpURI, httpsURI));

      // Also announce to the default service name
      String clusterDiscoveryD2ClusterName =
          D2TestUtils.setupD2Config(zkAddress, https, CLUSTER_DISCOVERY_D2_SERVICE_NAME);
      d2Servers.addAll(D2TestUtils.getD2Servers(zkAddress, clusterDiscoveryD2ClusterName, httpURI, httpsURI));

      RouterServer router = new RouterServer(
          routerProperties,
          d2Servers,
          Optional.empty(),
          Optional.of(SslUtils.getVeniceLocalSslFactory()),
          VeniceMetricsRepository.getVeniceMetricsRepository(
              ROUTER_SERVICE_NAME,
              ROUTER_SERVICE_METRIC_PREFIX,
              ROUTER_SERVICE_METRIC_ENTITIES,
              routerProperties.getAsMap()),
          D2TestUtils.getAndStartD2Client(zkAddress),
          CLUSTER_DISCOVERY_D2_SERVICE_NAME);
      return new VeniceRouterWrapper(
          regionName,
          serviceName,
          dataDirectory,
          router,
          routerProperties,
          zkAddress,
          d2ClusterName,
          clusterDiscoveryD2ClusterName);
    };
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return properties.getInt(LISTENER_PORT);
  }

  public int getSslPort() {
    return properties.getInt(LISTENER_SSL_PORT);
  }

  public String getD2ServiceNameForCluster(String clusterName) {
    return routerServer.getConfig().getClusterToD2Map().get(clusterName);
  }

  @Override
  protected void internalStart() throws Exception {
    routerServer.start();
    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> routerServer.isRunning());
    LOGGER.info("Started VeniceRouterWrapper: {}", this);
  }

  @Override
  protected void internalStop() throws Exception {
    routerServer.stop();
  }

  @Override
  protected void newProcess() {
    String httpURI = "http://" + getHost() + ":" + getPort();
    String httpsURI = "https://" + getHost() + ":" + getSslPort();

    List<ServiceDiscoveryAnnouncer> d2Servers = D2TestUtils.getD2Servers(zkAddress, d2ClusterName, httpURI, httpsURI);

    d2Servers.addAll(D2TestUtils.getD2Servers(zkAddress, clusterDiscoveryD2ClusterName, httpURI, httpsURI));

    routerServer = new RouterServer(
        properties,
        d2Servers,
        Optional.empty(),
        Optional.of(SslUtils.getVeniceLocalSslFactory()),
        VeniceMetricsRepository.getVeniceMetricsRepository(
            ROUTER_SERVICE_NAME,
            ROUTER_SERVICE_METRIC_PREFIX,
            ROUTER_SERVICE_METRIC_ENTITIES,
            properties.getAsMap()),
        D2TestUtils.getAndStartD2Client(zkAddress),
        CLUSTER_DISCOVERY_D2_SERVICE_NAME);
    LOGGER.info("Started VeniceRouterWrapper: {}", this);
  }

  @Override
  public String getComponentTagForLogging() {
    return new StringBuilder(getComponentTagPrefix(regionName)).append(super.getComponentTagForLogging()).toString();
  }

  public HelixBaseRoutingRepository getRoutingDataRepository() {
    return routerServer.getRoutingDataRepository();
  }

  public ReadOnlyStoreRepository getMetaDataRepository() {
    return routerServer.getMetadataRepository();
  }

  public ReadOnlySchemaRepository getSchemaRepository() {
    return routerServer.getSchemaRepository();
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routerServer.getRoutersClusterManager();
  }

  @Override
  public MetricsRepository getMetricsRepository() {
    return routerServer.getMetricsRepository();
  }

  public RouterServer getRouter() {
    return routerServer;
  }

  public void refresh() {
    routerServer.refresh();
  }

  @Override
  public String toString() {
    return "VeniceRouterWrapper{" + "address=http://" + getAddress() + ", sslAddress=https://" + getHost() + ":"
        + getSslPort() + ", regionName=" + regionName + ", d2ClusterName=" + d2ClusterName
        + ", clusterDiscoveryD2ClusterName=" + clusterDiscoveryD2ClusterName + "}";
  }
}

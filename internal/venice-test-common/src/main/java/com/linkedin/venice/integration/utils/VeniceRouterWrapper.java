package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.tehuti.MetricsAware;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;


/**
 * A wrapper for the {@link VeniceRouterWrapper}.
 */
public class VeniceRouterWrapper extends ProcessWrapper implements MetricsAware {
  public static final String SERVICE_NAME = "VeniceRouter";
  private final VeniceProperties properties;
  private final String zkAddress;
  private RouterServer service;

  VeniceRouterWrapper(
      String serviceName,
      File dataDirectory,
      RouterServer service,
      VeniceProperties properties,
      String zkAddress) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.properties = properties;
    this.zkAddress = zkAddress;
  }

  static StatefulServiceProvider<VeniceRouterWrapper> generateService(
      String clusterName,
      KafkaBrokerWrapper kafkaBrokerWrapper,
      boolean sslToStorageNodes,
      String clusterToD2,
      Properties properties) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, dataDirectory) -> {
      int port = Utils.getFreePort();
      int sslPort = Utils.getFreePort();
      PropertyBuilder builder = new PropertyBuilder().put(CLUSTER_NAME, clusterName)
          .put(LISTENER_PORT, port)
          .put(LISTENER_SSL_PORT, sslPort)
          .put(ZOOKEEPER_ADDRESS, zkAddress)
          .put(KAFKA_ZK_ADDRESS, kafkaBrokerWrapper.getZkAddress())
          .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
          .put(SSL_TO_STORAGE_NODES, sslToStorageNodes)
          .put(
              CLUSTER_TO_D2,
              StringUtils.isEmpty(clusterToD2) ? TestUtils.getClusterToDefaultD2String(clusterName) : clusterToD2)
          .put(ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES, true)
          // Below configs are to attempt to minimize resource utilization in tests
          .put(ROUTER_CONNECTION_LIMIT, 20)
          .put(ROUTER_HTTP_CLIENT_POOL_SIZE, 2)
          .put(ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 2)
          .put(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 1)
          .put(ROUTER_MAX_OUTGOING_CONNECTION, 10)
          // To speed up test
          .put(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 0)
          .put(MAX_READ_CAPACITY, DEFAULT_PER_ROUTER_READ_QUOTA)
          .put(SYSTEM_SCHEMA_CLUSTER_NAME, clusterName)
          .put(properties);

      // setup d2 config first
      String d2 = D2TestUtils.getD2ServiceName(clusterToD2, clusterName);

      VeniceProperties routerProperties = builder.build();
      boolean https = routerProperties.getBoolean(ROUTER_HTTP2_INBOUND_ENABLED, false);
      List<D2Server> d2Servers;
      if (!D2TestUtils.DEFAULT_TEST_SERVICE_NAME.equals(d2)) {
        D2TestUtils.setupD2Config(zkAddress, https, d2, d2, false);
        d2Servers = D2TestUtils
            .getD2Servers(zkAddress, new String[] { "http://localhost:" + port, "https://localhost:" + sslPort }, d2);
        // Also announce to the default service name (venice-discovery)
        D2TestUtils.setupD2Config(zkAddress, https);
        d2Servers
            .addAll(D2TestUtils.getD2Servers(zkAddress, "http://localhost:" + port, "https://localhost:" + sslPort));
      } else {
        D2TestUtils.setupD2Config(zkAddress, https);
        // Announce to d2 by default
        d2Servers = D2TestUtils.getD2Servers(zkAddress, "http://localhost:" + port, "https://localhost:" + sslPort);
      }

      RouterServer router = new RouterServer(
          routerProperties,
          d2Servers,
          Optional.empty(),
          Optional.of(H2SSLUtils.getLocalHttp2SslFactory()));
      return new VeniceRouterWrapper(serviceName, dataDirectory, router, routerProperties, zkAddress);
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

  public String getD2Service() {
    return D2TestUtils.DEFAULT_TEST_SERVICE_NAME;
  }

  @Override
  protected void internalStart() throws Exception {
    service.start();

    TestUtils.waitForNonDeterministicCompletion(
        IntegrationTestUtils.MAX_ASYNC_START_WAIT_TIME_MS,
        TimeUnit.MILLISECONDS,
        () -> service.isRunning());
  }

  @Override
  protected void internalStop() throws Exception {
    service.stop();
  }

  @Override
  protected void newProcess() {
    List<D2Server> d2Servers =
        D2TestUtils.getD2Servers(zkAddress, "http://localhost:" + getPort(), "https://localhost:" + getSslPort());
    service = new RouterServer(properties, d2Servers, Optional.empty(), Optional.of(SslUtils.getLocalSslFactory()));
  }

  public HelixBaseRoutingRepository getRoutingDataRepository() {
    return service.getRoutingDataRepository();
  }

  public ReadOnlyStoreRepository getMetaDataRepository() {
    return service.getMetadataRepository();
  }

  public ReadOnlySchemaRepository getSchemaRepository() {
    return service.getSchemaRepository();
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return service.getRoutersClusterManager();
  }

  @Override
  public MetricsRepository getMetricsRepository() {
    return service.getMetricsRepository();
  }

  public void refresh() {
    service.refresh();
  }
}

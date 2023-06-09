package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS;
import static com.linkedin.venice.ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES;
import static com.linkedin.venice.ConfigKeys.SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


/**
 * A wrapper for the {@link VeniceRouterWrapper}.
 * This class provides mock implementations of the routingdatarepo and such so it is light-weight and standalone.
 */
public class MockVeniceRouterWrapper extends ProcessWrapper {
  static final String SERVICE_NAME = "MockVeniceRouter";
  public static final String CONTROLLER = "http://localhost:1234";
  private RouterServer service;
  private final String clusterName;
  private final int port;

  MockVeniceRouterWrapper(
      String serviceName,
      File dataDirectory,
      RouterServer service,
      String clusterName,
      int port,
      boolean sslToStorageNodes) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.clusterName = clusterName;
  }

  static StatefulServiceProvider<MockVeniceRouterWrapper> generateService(
      String zkAddress,
      boolean sslToStorageNodes,
      Properties extraConfigs) {

    Store mockStore = Mockito.mock(Store.class);
    doReturn(true).when(mockStore).isEnableReads();
    doReturn(1).when(mockStore).getCurrentVersion();
    doReturn(CompressionStrategy.NO_OP).when(mockStore).getCompressionStrategy();
    HelixReadOnlyStoreRepository mockMetadataRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

    Version mockVersion = Mockito.mock(Version.class);
    doReturn(Optional.of(mockVersion)).when(mockStore).getVersion(Mockito.anyInt());

    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    doReturn(partitionerConfig).when(mockVersion).getPartitionerConfig();

    HelixReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(HelixReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, "\"string\"")).when(mockSchemaRepository).getKeySchema(Mockito.anyString());

    HelixCustomizedViewOfflinePushRepository mockRepo = Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(1).when(mockRepo).getNumberOfPartitions(ArgumentMatchers.anyString());

    Optional<HelixHybridStoreQuotaRepository> mockHybridStoreQuotaRepository =
        Optional.of(Mockito.mock(HelixHybridStoreQuotaRepository.class));

    Instance mockControllerInstance = Mockito.mock(Instance.class);
    doReturn(CONTROLLER).when(mockControllerInstance).getUrl();
    doReturn(mockControllerInstance).when(mockRepo).getLeaderController();

    HelixReadOnlyStoreConfigRepository mockStoreConfigRepository =
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    HelixLiveInstanceMonitor mockLiveInstanceMonitor = Mockito.mock(HelixLiveInstanceMonitor.class);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(any());
    final String kafkaBootstrapServers = "localhost:1234";

    return (serviceName, dataDirectory) -> {
      int port = TestUtils.getFreePort();
      String httpURI = "http://localhost:" + port;
      String httpsURI = "https://localhost:" + sslPortFromPort(port);
      List<ServiceDiscoveryAnnouncer> d2ServerList = new ArrayList<>();
      String d2ServiceName = Utils.getUniqueString("D2_SERVICE_NAME");
      String serverD2ServiceName = Utils.getUniqueString("SERVER_D2_SERVICE_NAME");
      if (!StringUtils.isEmpty(zkAddress)) {
        // Set up d2 config before announcing
        String d2ClusterName = D2TestUtils.setupD2Config(zkAddress, false, d2ServiceName);
        d2ServerList.addAll(D2TestUtils.getD2Servers(zkAddress, d2ClusterName, httpURI, httpsURI));
        String clusterDiscoveryD2ClusterName =
            D2TestUtils.setupD2Config(zkAddress, false, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME);
        d2ServerList.addAll(D2TestUtils.getD2Servers(zkAddress, clusterDiscoveryD2ClusterName, httpURI, httpsURI));
      }
      String clusterName = Utils.getUniqueString("mock-venice-router-cluster");
      PropertyBuilder builder = new PropertyBuilder().put(CLUSTER_NAME, clusterName)
          .put(LISTENER_PORT, port)
          .put(LISTENER_SSL_PORT, sslPortFromPort(port))
          .put(ZOOKEEPER_ADDRESS, zkAddress)
          .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
          .put(SSL_TO_STORAGE_NODES, sslToStorageNodes)
          .put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, d2ServiceName)))
          .put(
              CLUSTER_TO_SERVER_D2,
              TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, serverD2ServiceName)))
          .put(ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 0)
          .put(ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES, true)
          .put(ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT.name())
          .put(extraConfigs);
      StoreConfig storeConfig = new StoreConfig("test");
      storeConfig.setCluster(clusterName);
      doReturn(Optional.of(storeConfig)).when(mockStoreConfigRepository).getStoreConfig(Mockito.anyString());

      RouterServer router = new RouterServer(
          builder.build(),
          mockRepo,
          mockHybridStoreQuotaRepository,
          mockMetadataRepository,
          mockSchemaRepository,
          mockStoreConfigRepository,
          d2ServerList,
          Optional.of(SslUtils.getVeniceLocalSslFactory()),
          mockLiveInstanceMonitor);
      return new MockVeniceRouterWrapper(serviceName, dataDirectory, router, clusterName, port, sslToStorageNodes);
    };
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return port;
  }

  public int getSslPort() {
    return sslPortFromPort(port);
  }

  public String getClusterName() {
    return clusterName;
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
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Mock venice router does not support restart.");
  }

  public String getRouterD2Service() {
    return VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME;
  }

  public String getD2ServiceNameForCluster(String clusterName) {
    return service.getConfig().getClusterToD2Map().get(clusterName);
  }

  private static int sslPortFromPort(int port) {
    return port + 1;
  }

  public MetricsRepository getMetricsRepository() {
    return service.getMetricsRepository();
  }
}

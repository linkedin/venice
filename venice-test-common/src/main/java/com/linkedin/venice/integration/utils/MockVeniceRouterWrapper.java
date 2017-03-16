package com.linkedin.venice.integration.utils;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.linkedin.venice.schema.SchemaEntry;

import org.mockito.Matchers;
import org.mockito.Mockito;
import static org.mockito.Mockito.doReturn;


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
  private final boolean sslToStorageNodes;

  MockVeniceRouterWrapper(String serviceName, File dataDirectory, RouterServer service, String clusterName, int port, boolean sslToStorageNodes) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.clusterName = clusterName;
    this.sslToStorageNodes = sslToStorageNodes;
  }

  static StatefulServiceProvider<MockVeniceRouterWrapper> generateService(String zkAddress, boolean sslToStorageNodes) {

    Store mockStore = Mockito.mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    HelixReadOnlyStoreRepository mockMetadataRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

    HelixReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(HelixReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, "\"string\"")).when(mockSchemaRepository).getKeySchema(Mockito.anyString());

    HelixRoutingDataRepository mockRepo = Mockito.mock(HelixRoutingDataRepository.class);
    doReturn(1).when(mockRepo).getNumberOfPartitions(Matchers.anyString());

    Instance mockControllerInstance = Mockito.mock(Instance.class);
    doReturn(CONTROLLER).when(mockControllerInstance).getUrl();
    doReturn(mockControllerInstance).when(mockRepo).getMasterController();

    return (serviceName, port, dataDirectory) -> {
      List<D2Server> d2ServerList;
      if (Utils.isNullOrEmpty(zkAddress)) {
        d2ServerList = new ArrayList<>();
      } else {
        d2ServerList = D2TestUtils.getD2Servers(zkAddress, "http://localhost:" + port, "https://localhost:" + sslPortFromPort(port));
      }
      String clusterName = TestUtils.getUniqueString("mock-venice-router-cluster");
      RouterServer router = new RouterServer(port, sslPortFromPort(port), clusterName, mockRepo,
          mockMetadataRepository, mockSchemaRepository, d2ServerList, Optional.of(SslUtils.getLocalSslFactory()), sslToStorageNodes);
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
        () -> service.isStarted());
  }

  @Override
  protected void internalStop() throws Exception {
    service.stop();
  }

  @Override
  protected void newProcess()
      throws Exception {
    throw new UnsupportedOperationException("Mock venice router does not support restart.");
  }

  private static int sslPortFromPort(int port) {
    return port + 1;
  }
}

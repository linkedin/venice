package com.linkedin.venice.integration.utils;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.helix.HelixReadonlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.RouterServer;
import java.io.File;
import java.util.List;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;


/**
 * A wrapper for the {@link VeniceRouterWrapper}.
 * This class provides mock implementations of the routingdatarepo and such so it is light-weight and standalone.
 */
public class MockVeniceRouterWrapper extends ProcessWrapper {

  public static final String SERVICE_NAME = "MockVeniceRouter";
  public static final String CONTROLLER = "http://localhost:1234";

  private final RouterServer service;
  private final String clusterName;
  private final int port;

  MockVeniceRouterWrapper(String serviceName, File dataDirectory, RouterServer service, String clusterName, int port) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
    this.clusterName = clusterName;
  }

  static StatefulServiceProvider<MockVeniceRouterWrapper> generateService(List<D2Server> d2ServerList) {


    Store mockStore = Mockito.mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    HelixReadonlyStoreRepository mockMetadataRepository = Mockito.mock(HelixReadonlyStoreRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

    HelixRoutingDataRepository mockRepo = Mockito.mock(HelixRoutingDataRepository.class);

    Instance mockControllerInstance = Mockito.mock(Instance.class);
    doReturn(CONTROLLER).when(mockControllerInstance).getUrl();
    doReturn(mockControllerInstance).when(mockRepo).getMasterController();

    return (serviceName, port, dataDirectory) -> {
      String clusterName = TestUtils.getUniqueString("mock-venice-router-cluster");
      RouterServer router = new RouterServer(port, clusterName, mockRepo, mockMetadataRepository, d2ServerList);
      return new MockVeniceRouterWrapper(serviceName, dataDirectory, router, clusterName, port);
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

  public String getClusterName() {
    return clusterName;
  }

  @Override
  protected void start() throws Exception {
    service.start();
  }

  @Override
  protected void stop() throws Exception {
    service.stop();
  }

}

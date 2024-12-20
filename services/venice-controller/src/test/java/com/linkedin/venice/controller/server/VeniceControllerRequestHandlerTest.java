package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.CreateNewStoreRequest;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.Pair;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceControllerRequestHandlerTest {
  private VeniceControllerRequestHandler requestHandler;
  private Admin admin;
  private ControllerRequestHandlerDependencies dependencies;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    dependencies = mock(ControllerRequestHandlerDependencies.class);
    when(dependencies.getAdmin()).thenReturn(admin);
    when(dependencies.isSslEnabled()).thenReturn(true);
    requestHandler = new VeniceControllerRequestHandler(dependencies);
  }

  @Test
  public void testGetLeaderController() {
    ControllerRequest request = mock(ControllerRequest.class);
    LeaderControllerResponse response = new LeaderControllerResponse();
    Instance leaderInstance = mock(Instance.class);

    when(request.getClusterName()).thenReturn("testCluster");
    when(admin.getLeaderController("testCluster")).thenReturn(leaderInstance);
    when(leaderInstance.getUrl(true)).thenReturn("https://leader-url:443");
    when(leaderInstance.getUrl(false)).thenReturn("http://leader-url:80");
    when(leaderInstance.getGrpcUrl()).thenReturn("leader-grpc-url:50051");
    when(leaderInstance.getGrpcSslUrl()).thenReturn("leader-grpc-url:50052");
    when(leaderInstance.getPort()).thenReturn(80);
    when(leaderInstance.getSslPort()).thenReturn(443); // SSL enabled
    when(leaderInstance.getGrpcPort()).thenReturn(50051);
    when(leaderInstance.getGrpcSslPort()).thenReturn(50052);

    requestHandler.getLeaderController(request, response);

    assertEquals(response.getCluster(), "testCluster");
    assertEquals(response.getUrl(), "https://leader-url:443"); // SSL enabled
    assertEquals(response.getSecureUrl(), "https://leader-url:443");
    assertEquals(response.getGrpcUrl(), "leader-grpc-url:50051");
    assertEquals(response.getSecureGrpcUrl(), "leader-grpc-url:50052");

    // SSL not enabled
    when(dependencies.isSslEnabled()).thenReturn(false);
    requestHandler = new VeniceControllerRequestHandler(dependencies);
    LeaderControllerResponse response1 = new LeaderControllerResponse();
    requestHandler.getLeaderController(request, response1);
    assertEquals(response1.getUrl(), "http://leader-url:80");
    assertEquals(response1.getSecureUrl(), "https://leader-url:443");
    assertEquals(response1.getGrpcUrl(), "leader-grpc-url:50051");
    assertEquals(response1.getSecureGrpcUrl(), "leader-grpc-url:50052");
  }

  @Test
  public void testDiscoverCluster() {
    ClusterDiscoveryRequest request = mock(ClusterDiscoveryRequest.class);
    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    Pair<String, String> clusterToD2Pair = Pair.create("testCluster", "testD2Service");

    when(request.getStoreName()).thenReturn("testStore");
    when(admin.discoverCluster("testStore")).thenReturn(clusterToD2Pair);
    when(admin.getServerD2Service("testCluster")).thenReturn("testServerD2Service");

    requestHandler.discoverCluster(request, response);

    assertEquals(response.getName(), "testStore");
    assertEquals(response.getCluster(), "testCluster");
    assertEquals(response.getD2Service(), "testD2Service");
    assertEquals(response.getServerD2Service(), "testServerD2Service");
  }

  @Test
  public void testCreateStore() {
    CreateNewStoreRequest request = mock(CreateNewStoreRequest.class);
    NewStoreResponse response = new NewStoreResponse();

    when(request.getClusterName()).thenReturn("testCluster");
    when(request.getStoreName()).thenReturn("testStore");
    when(request.getKeySchema()).thenReturn("testKeySchema");
    when(request.getValueSchema()).thenReturn("testValueSchema");
    when(request.getOwner()).thenReturn("testOwner");
    when(request.getAccessPermissions()).thenReturn("testAccessPermissions");
    when(request.isSystemStore()).thenReturn(false);

    requestHandler.createStore(request, response);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        false,
        Optional.of("testAccessPermissions"));
    assertEquals(response.getCluster(), "testCluster");
    assertEquals(response.getName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testCreateStoreWithNullAccessPermissions() {
    CreateNewStoreRequest request = mock(CreateNewStoreRequest.class);
    NewStoreResponse response = new NewStoreResponse();

    when(request.getClusterName()).thenReturn("testCluster");
    when(request.getStoreName()).thenReturn("testStore");
    when(request.getKeySchema()).thenReturn("testKeySchema");
    when(request.getValueSchema()).thenReturn("testValueSchema");
    when(request.getOwner()).thenReturn("testOwner");
    when(request.getAccessPermissions()).thenReturn(null);
    when(request.isSystemStore()).thenReturn(true);

    requestHandler.createStore(request, response);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        true,
        Optional.empty());
    assertEquals(response.getCluster(), "testCluster");
    assertEquals(response.getName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testIsSslEnabled() {
    boolean sslEnabled = requestHandler.isSslEnabled();
    assertTrue(sslEnabled);
  }
}

package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
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
  public void testGetLeaderControllerDetails() {
    String clusterName = "testCluster";
    LeaderControllerGrpcRequest request = LeaderControllerGrpcRequest.newBuilder().setClusterName(clusterName).build();
    Instance leaderInstance = mock(Instance.class);
    when(admin.getLeaderController(clusterName)).thenReturn(leaderInstance);
    when(leaderInstance.getUrl(true)).thenReturn("https://leader-url:443");
    when(leaderInstance.getUrl(false)).thenReturn("http://leader-url:80");
    when(leaderInstance.getGrpcUrl()).thenReturn("leader-grpc-url:50051");
    when(leaderInstance.getGrpcSslUrl()).thenReturn("leader-grpc-url:50052");
    when(leaderInstance.getPort()).thenReturn(80);
    when(leaderInstance.getSslPort()).thenReturn(443); // SSL enabled
    when(leaderInstance.getGrpcPort()).thenReturn(50051);
    when(leaderInstance.getGrpcSslPort()).thenReturn(50052);

    LeaderControllerGrpcResponse response = requestHandler.getLeaderControllerDetails(request);

    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getHttpUrl(), "https://leader-url:443"); // SSL enabled
    assertEquals(response.getHttpsUrl(), "https://leader-url:443");
    assertEquals(response.getGrpcUrl(), "leader-grpc-url:50051");
    assertEquals(response.getSecureGrpcUrl(), "leader-grpc-url:50052");

    // SSL not enabled
    when(dependencies.isSslEnabled()).thenReturn(false);
    requestHandler = new VeniceControllerRequestHandler(dependencies);
    LeaderControllerGrpcResponse response1 = requestHandler.getLeaderControllerDetails(request);
    assertEquals(response1.getHttpUrl(), "http://leader-url:80");
    assertEquals(response1.getHttpsUrl(), "https://leader-url:443");
    assertEquals(response1.getGrpcUrl(), "leader-grpc-url:50051");
    assertEquals(response1.getSecureGrpcUrl(), "leader-grpc-url:50052");
  }

  @Test
  public void testDiscoverCluster() {
    String storeName = "testStore";
    DiscoverClusterGrpcRequest request = DiscoverClusterGrpcRequest.newBuilder().setStoreName(storeName).build();
    Pair<String, String> clusterToD2Pair = Pair.create("testCluster", "testD2Service");
    when(admin.discoverCluster(storeName)).thenReturn(clusterToD2Pair);
    when(admin.getServerD2Service("testCluster")).thenReturn("testServerD2Service");

    DiscoverClusterGrpcResponse response = requestHandler.discoverCluster(request);

    assertEquals(response.getStoreName(), storeName);
    assertEquals(response.getClusterName(), "testCluster");
    assertEquals(response.getD2Service(), "testD2Service");
    assertEquals(response.getServerD2Service(), "testServerD2Service");
  }

  @Test
  public void testCreateStore() {
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setKeySchema("testKeySchema")
        .setValueSchema("testValueSchema")
        .setOwner("testOwner")
        .setAccessPermission("testAccessPermissions")
        .setIsSystemStore(false)
        .build();

    CreateStoreGrpcResponse response = requestHandler.createStore(request);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        false,
        Optional.of("testAccessPermissions"));
    assertEquals(response.getClusterStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getClusterStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testCreateStoreWithNullAccessPermissions() {
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setKeySchema("testKeySchema")
        .setValueSchema("testValueSchema")
        .setOwner("testOwner")
        .setIsSystemStore(true)
        .build();

    CreateStoreGrpcResponse response = requestHandler.createStore(request);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        true,
        Optional.empty());
    assertEquals(response.getClusterStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getClusterStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testIsSslEnabled() {
    boolean sslEnabled = requestHandler.isSslEnabled();
    assertTrue(sslEnabled);
  }
}

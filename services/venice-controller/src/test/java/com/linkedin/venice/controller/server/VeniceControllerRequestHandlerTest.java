package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcRequest;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcResponse;
import java.util.HashMap;
import java.util.Map;
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
    when(admin.discoverCluster(storeName)).thenReturn("testCluster");
    when(admin.getRouterD2Service("testCluster")).thenReturn("testD2Service");
    when(admin.getServerD2Service("testCluster")).thenReturn("testServerD2Service");

    DiscoverClusterGrpcResponse response = requestHandler.discoverCluster(request);

    assertEquals(response.getStoreName(), storeName);
    assertEquals(response.getClusterName(), "testCluster");
    assertEquals(response.getD2Service(), "testD2Service");
    assertEquals(response.getServerD2Service(), "testServerD2Service");
  }

  @Test
  public void testIsSslEnabled() {
    boolean sslEnabled = requestHandler.isSslEnabled();
    assertTrue(sslEnabled);
  }

  @Test
  public void testListChildClustersForParentController() {
    String clusterName = "testCluster";
    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(clusterName).build();

    Map<String, String> childUrlMap = new HashMap<>();
    childUrlMap.put("dc1", "http://dc1-controller:8080");
    childUrlMap.put("dc2", "http://dc2-controller:8080");

    Map<String, String> childD2Map = new HashMap<>();
    childD2Map.put("dc1", "d2://dc1-controller");
    childD2Map.put("dc2", "d2://dc2-controller");

    when(admin.isParent()).thenReturn(true);
    when(admin.getChildDataCenterControllerUrlMap(clusterName)).thenReturn(childUrlMap);
    when(admin.getChildDataCenterControllerD2Map(clusterName)).thenReturn(childD2Map);
    when(admin.getChildControllerD2ServiceName(clusterName)).thenReturn("VeniceController");

    ListChildClustersGrpcResponse response = requestHandler.listChildClusters(request);

    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getChildDataCenterControllerUrlMapCount(), 2);
    assertEquals(response.getChildDataCenterControllerUrlMapMap().get("dc1"), "http://dc1-controller:8080");
    assertEquals(response.getChildDataCenterControllerUrlMapMap().get("dc2"), "http://dc2-controller:8080");
    assertEquals(response.getChildDataCenterControllerD2MapCount(), 2);
    assertEquals(response.getChildDataCenterControllerD2MapMap().get("dc1"), "d2://dc1-controller");
    assertEquals(response.getChildDataCenterControllerD2MapMap().get("dc2"), "d2://dc2-controller");
    assertTrue(response.hasD2ServiceName());
    assertEquals(response.getD2ServiceName(), "VeniceController");
  }

  @Test
  public void testListChildClustersForChildController() {
    String clusterName = "testCluster";
    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(clusterName).build();

    when(admin.isParent()).thenReturn(false);

    ListChildClustersGrpcResponse response = requestHandler.listChildClusters(request);

    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getChildDataCenterControllerUrlMapCount(), 0);
    assertEquals(response.getChildDataCenterControllerD2MapCount(), 0);
    assertFalse(response.hasD2ServiceName());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is required")
  public void testListChildClustersMissingClusterName() {
    ListChildClustersGrpcRequest request = ListChildClustersGrpcRequest.newBuilder().build();
    requestHandler.listChildClusters(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is required")
  public void testListChildClustersEmptyClusterName() {
    ListChildClustersGrpcRequest request = ListChildClustersGrpcRequest.newBuilder().setClusterName("").build();
    requestHandler.listChildClusters(request);
  }

  @Test
  public void testListChildClustersWithNullMaps() {
    String clusterName = "testCluster";
    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(clusterName).build();

    when(admin.isParent()).thenReturn(true);
    when(admin.getChildDataCenterControllerUrlMap(clusterName)).thenReturn(null);
    when(admin.getChildDataCenterControllerD2Map(clusterName)).thenReturn(null);
    when(admin.getChildControllerD2ServiceName(clusterName)).thenReturn(null);

    ListChildClustersGrpcResponse response = requestHandler.listChildClusters(request);

    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getChildDataCenterControllerUrlMapCount(), 0);
    assertEquals(response.getChildDataCenterControllerD2MapCount(), 0);
    assertFalse(response.hasD2ServiceName());
  }
}

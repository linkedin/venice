package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.GetBackupVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.GetBackupVersionGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.utils.Pair;
import java.util.Collections;
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
  public void testIsSslEnabled() {
    boolean sslEnabled = requestHandler.isSslEnabled();
    assertTrue(sslEnabled);
  }

  @Test
  public void testGetBackupVersionForParentController() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName).build();
    GetBackupVersionGrpcRequest request = GetBackupVersionGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    Store mockStore = mock(Store.class);
    when(admin.getStore(clusterName, storeName)).thenReturn(mockStore);

    Map<String, String> backupVersionMap = new HashMap<>();
    backupVersionMap.put("dc-0", "1");
    backupVersionMap.put("dc-1", "2");
    when(admin.getBackupVersionsForMultiColos(clusterName, storeName)).thenReturn(backupVersionMap);

    GetBackupVersionGrpcResponse response = requestHandler.getBackupVersion(request);

    assertEquals(response.getStoreInfo().getClusterName(), clusterName);
    assertEquals(response.getStoreInfo().getStoreName(), storeName);
    assertEquals(response.getStoreVersionMapMap().size(), 2);
    assertEquals(response.getStoreVersionMapMap().get("dc-0"), "1");
    assertEquals(response.getStoreVersionMapMap().get("dc-1"), "2");
  }

  @Test
  public void testGetBackupVersionForChildController() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName).build();
    GetBackupVersionGrpcRequest request = GetBackupVersionGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    Store mockStore = mock(Store.class);
    when(admin.getStore(clusterName, storeName)).thenReturn(mockStore);

    // Child controller returns empty map
    when(admin.getBackupVersionsForMultiColos(clusterName, storeName)).thenReturn(Collections.emptyMap());
    when(admin.getBackupVersion(clusterName, storeName)).thenReturn(3);

    GetBackupVersionGrpcResponse response = requestHandler.getBackupVersion(request);

    assertEquals(response.getStoreInfo().getClusterName(), clusterName);
    assertEquals(response.getStoreInfo().getStoreName(), storeName);
    assertEquals(response.getStoreVersionMapMap().size(), 1);
    assertEquals(response.getStoreVersionMapMap().get(storeName), "3");
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetBackupVersionStoreNotFound() {
    String clusterName = "testCluster";
    String storeName = "nonExistentStore";
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName).build();
    GetBackupVersionGrpcRequest request = GetBackupVersionGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    when(admin.getStore(clusterName, storeName)).thenReturn(null);

    requestHandler.getBackupVersion(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is mandatory parameter")
  public void testGetBackupVersionMissingClusterName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setStoreName("testStore").build();
    GetBackupVersionGrpcRequest request = GetBackupVersionGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    requestHandler.getBackupVersion(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store name is mandatory parameter")
  public void testGetBackupVersionMissingStoreName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").build();
    GetBackupVersionGrpcRequest request = GetBackupVersionGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    requestHandler.getBackupVersion(request);
  }
}

package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreRequestHandlerTest {
  private StoreRequestHandler storeRequestHandler;
  private Admin admin;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    when(dependencies.getAdmin()).thenReturn(admin);
    storeRequestHandler = new StoreRequestHandler(dependencies);
  }

  @Test
  public void testCreateStore() {
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setKeySchema("testKeySchema")
        .setValueSchema("testValueSchema")
        .setOwner("testOwner")
        .setAccessPermission("testAccessPermissions")
        .setIsSystemStore(false)
        .build();

    CreateStoreGrpcResponse response = storeRequestHandler.createStore(request);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        false,
        Optional.of("testAccessPermissions"));
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testCreateStoreWithNullAccessPermissions() {
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setKeySchema("testKeySchema")
        .setValueSchema("testValueSchema")
        .setOwner("testOwner")
        .setIsSystemStore(true)
        .build();

    CreateStoreGrpcResponse response = storeRequestHandler.createStore(request);

    verify(admin, times(1)).createStore(
        "testCluster",
        "testStore",
        "testOwner",
        "testKeySchema",
        "testValueSchema",
        true,
        Optional.empty());
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getOwner(), "testOwner");
  }

  @Test
  public void testUpdateAclForStoreSuccess() {
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setAccessPermissions("read,write")
        .build();

    UpdateAclForStoreGrpcResponse response = storeRequestHandler.updateAclForStore(request);

    verify(admin, times(1)).updateAclForStore("testCluster", "testStore", "read,write");
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Access permissions is required for updating ACL")
  public void testUpdateAclForStoreMissingAccessPermissions() {
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    storeRequestHandler.updateAclForStore(request);
  }

  @Test
  public void testGetAclForStoreWithPermissions() {
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    when(admin.getAclForStore("testCluster", "testStore")).thenReturn("read,write");

    GetAclForStoreGrpcResponse response = storeRequestHandler.getAclForStore(request);

    verify(admin, times(1)).getAclForStore("testCluster", "testStore");
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getAccessPermissions(), "read,write");
  }

  @Test
  public void testGetAclForStoreWithoutPermissions() {
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    when(admin.getAclForStore("testCluster", "testStore")).thenReturn(null);

    GetAclForStoreGrpcResponse response = storeRequestHandler.getAclForStore(request);

    verify(admin, times(1)).getAclForStore("testCluster", "testStore");
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertTrue(response.getAccessPermissions().isEmpty());
  }

  @Test
  public void testDeleteAclForStore() {
    DeleteAclForStoreGrpcRequest request = DeleteAclForStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    DeleteAclForStoreGrpcResponse response = storeRequestHandler.deleteAclForStore(request);

    verify(admin, times(1)).deleteAclForStore("testCluster", "testStore");
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
  }

  @Test
  public void testCheckResourceCleanupForStoreCreation() {
    // No lingering resources
    ClusterStoreGrpcInfo request =
        ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build();
    storeRequestHandler.checkResourceCleanupForStoreCreation(request);
    verify(admin, times(1)).checkResourceCleanupBeforeStoreCreation("testCluster", "testStore");

    // Lingering resources hence throws exception
    doThrow(new RuntimeException("Lingering resources found")).when(admin)
        .checkResourceCleanupBeforeStoreCreation("testCluster", "testStore");
    Exception e =
        expectThrows(RuntimeException.class, () -> storeRequestHandler.checkResourceCleanupForStoreCreation(request));
    assertEquals(e.getMessage(), "Lingering resources found");
  }
}

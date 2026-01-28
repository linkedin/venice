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
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.controller.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

  @Test
  public void testListStoresSuccess() {
    ListStoresGrpcRequest request =
        ListStoresGrpcRequest.newBuilder().setClusterName("testCluster").setIncludeSystemStores(true).build();

    Store store1 = mock(Store.class);
    when(store1.getName()).thenReturn("store1");
    when(store1.isSystemStore()).thenReturn(false);

    Store store2 = mock(Store.class);
    when(store2.getName()).thenReturn("store2");
    when(store2.isSystemStore()).thenReturn(false);

    List<Store> stores = Arrays.asList(store1, store2);
    when(admin.getAllStores("testCluster")).thenReturn(stores);

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    verify(admin, times(1)).getAllStores("testCluster");
    assertEquals(response.getClusterName(), "testCluster");
    assertEquals(response.getStoreNamesCount(), 2);
    assertTrue(response.getStoreNamesList().contains("store1"));
    assertTrue(response.getStoreNamesList().contains("store2"));
  }

  @Test
  public void testListStoresWithoutSystemStores() {
    ListStoresGrpcRequest request =
        ListStoresGrpcRequest.newBuilder().setClusterName("testCluster").setIncludeSystemStores(false).build();

    Store regularStore = mock(Store.class);
    when(regularStore.getName()).thenReturn("regularStore");
    when(regularStore.isSystemStore()).thenReturn(false);

    Store systemStore = mock(Store.class);
    when(systemStore.getName()).thenReturn("systemStore");
    when(systemStore.isSystemStore()).thenReturn(true);

    List<Store> stores = Arrays.asList(regularStore, systemStore);
    when(admin.getAllStores("testCluster")).thenReturn(stores);

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    assertEquals(response.getStoreNamesCount(), 1);
    assertTrue(response.getStoreNamesList().contains("regularStore"));
    assertTrue(!response.getStoreNamesList().contains("systemStore"));
  }

  @Test
  public void testListStoresEmptyList() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder().setClusterName("testCluster").build();

    when(admin.getAllStores("testCluster")).thenReturn(Collections.emptyList());

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    assertEquals(response.getStoreNamesCount(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is required")
  public void testListStoresMissingClusterName() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder().build();
    storeRequestHandler.listStores(request);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Missing parameter: store_config_value_filter")
  public void testListStoresWithOnlyConfigNameFilter() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder()
        .setClusterName("testCluster")
        .setStoreConfigNameFilter("hybridStoreConfig")
        .build();

    when(admin.getAllStores("testCluster")).thenReturn(Collections.emptyList());

    storeRequestHandler.listStores(request);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Missing parameter: store_config_name_filter")
  public void testListStoresWithOnlyConfigValueFilter() {
    ListStoresGrpcRequest request =
        ListStoresGrpcRequest.newBuilder().setClusterName("testCluster").setStoreConfigValueFilter("someValue").build();

    when(admin.getAllStores("testCluster")).thenReturn(Collections.emptyList());

    storeRequestHandler.listStores(request);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*is not a valid store config.*")
  public void testListStoresWithInvalidConfigNameFilter() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder()
        .setClusterName("testCluster")
        .setStoreConfigNameFilter("invalidConfigName")
        .setStoreConfigValueFilter("someValue")
        .build();

    when(admin.getAllStores("testCluster")).thenReturn(Collections.emptyList());

    storeRequestHandler.listStores(request);
  }

  @Test
  public void testListStoresWithDataReplicationPolicyFilter() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder()
        .setClusterName("testCluster")
        .setStoreConfigNameFilter("dataReplicationPolicy")
        .setStoreConfigValueFilter("ACTIVE_ACTIVE")
        .build();

    Store hybridStore = mock(Store.class);
    when(hybridStore.getName()).thenReturn("hybridStore");
    when(hybridStore.isSystemStore()).thenReturn(false);
    when(hybridStore.isHybrid()).thenReturn(true);
    HybridStoreConfig hybridConfig = mock(HybridStoreConfig.class);
    when(hybridConfig.getDataReplicationPolicy()).thenReturn(DataReplicationPolicy.ACTIVE_ACTIVE);
    when(hybridStore.getHybridStoreConfig()).thenReturn(hybridConfig);

    Store nonHybridStore = mock(Store.class);
    when(nonHybridStore.getName()).thenReturn("nonHybridStore");
    when(nonHybridStore.isSystemStore()).thenReturn(false);
    when(nonHybridStore.isHybrid()).thenReturn(false);

    List<Store> stores = Arrays.asList(hybridStore, nonHybridStore);
    when(admin.getAllStores("testCluster")).thenReturn(stores);

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    assertEquals(response.getStoreNamesCount(), 1);
    assertTrue(response.getStoreNamesList().contains("hybridStore"));
  }

  @Test
  public void testListStoresWithDataReplicationPolicyFilterNoMatch() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder()
        .setClusterName("testCluster")
        .setStoreConfigNameFilter("dataReplicationPolicy")
        .setStoreConfigValueFilter("NON_AGGREGATE")
        .build();

    Store hybridStore = mock(Store.class);
    when(hybridStore.getName()).thenReturn("hybridStore");
    when(hybridStore.isSystemStore()).thenReturn(false);
    when(hybridStore.isHybrid()).thenReturn(true);
    HybridStoreConfig hybridConfig = mock(HybridStoreConfig.class);
    when(hybridConfig.getDataReplicationPolicy()).thenReturn(DataReplicationPolicy.ACTIVE_ACTIVE);
    when(hybridStore.getHybridStoreConfig()).thenReturn(hybridConfig);

    List<Store> stores = Arrays.asList(hybridStore);
    when(admin.getAllStores("testCluster")).thenReturn(stores);

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    assertEquals(response.getStoreNamesCount(), 0);
  }

  @Test
  public void testListStoresWithDataReplicationPolicyFilterNullPolicy() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder()
        .setClusterName("testCluster")
        .setStoreConfigNameFilter("dataReplicationPolicy")
        .setStoreConfigValueFilter("ACTIVE_ACTIVE")
        .build();

    Store hybridStore = mock(Store.class);
    when(hybridStore.getName()).thenReturn("hybridStore");
    when(hybridStore.isSystemStore()).thenReturn(false);
    when(hybridStore.isHybrid()).thenReturn(true);
    HybridStoreConfig hybridConfig = mock(HybridStoreConfig.class);
    when(hybridConfig.getDataReplicationPolicy()).thenReturn(null);
    when(hybridStore.getHybridStoreConfig()).thenReturn(hybridConfig);

    List<Store> stores = Arrays.asList(hybridStore);
    when(admin.getAllStores("testCluster")).thenReturn(stores);

    ListStoresGrpcResponse response = storeRequestHandler.listStores(request);

    assertEquals(response.getStoreNamesCount(), 0);
  }
}

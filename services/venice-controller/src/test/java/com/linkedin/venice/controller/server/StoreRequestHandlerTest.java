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
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "storeStatusMaps")
  public Object[][] storeStatusMaps() {
    Map<String, String> populatedMap = new HashMap<>();
    populatedMap.put("store1", "ONLINE");
    populatedMap.put("store2", "DEGRADED");
    populatedMap.put("store3", "UNAVAILABLE");
    return new Object[][] { { populatedMap }, { Collections.emptyMap() } };
  }

  @Test(dataProvider = "storeStatusMaps")
  public void testGetStoreStatuses(Map<String, String> expectedStatusMap) {
    when(admin.getAllStoreStatuses("testCluster")).thenReturn(expectedStatusMap);

    Map<String, String> response = storeRequestHandler.getStoreStatuses("testCluster");

    verify(admin, times(1)).getAllStoreStatuses("testCluster");
    assertEquals(response, expectedStatusMap);
  }

  @DataProvider(name = "blankClusterNames")
  public Object[][] blankClusterNames() {
    return new Object[][] { { null }, { "" } };
  }

  @Test(dataProvider = "blankClusterNames", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is required")
  public void testGetStoreStatusesWithBlankClusterName(String clusterName) {
    storeRequestHandler.getStoreStatuses(clusterName);
  }

  @Test
  public void testGetRepushInfoSuccess() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    Optional<String> fabric = Optional.of("testFabric");

    Version mockVersion = mock(Version.class);
    when(mockVersion.getNumber()).thenReturn(1);
    when(mockVersion.getCreatedTime()).thenReturn(123456789L);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.ONLINE);
    when(mockVersion.getPushJobId()).thenReturn("test-push-job-123");
    when(mockVersion.getPartitionCount()).thenReturn(10);
    when(mockVersion.getReplicationFactor()).thenReturn(3);

    RepushInfo mockRepushInfo =
        RepushInfo.createRepushInfo(mockVersion, "kafka.broker.url:9092", "testD2Service", "testZkHost");

    when(admin.getRepushInfo(clusterName, storeName, fabric)).thenReturn(mockRepushInfo);

    RepushInfo response = storeRequestHandler.getRepushInfo(clusterName, storeName, fabric);

    verify(admin, times(1)).getRepushInfo(clusterName, storeName, fabric);
    assertEquals(response.getKafkaBrokerUrl(), "kafka.broker.url:9092");
    assertEquals(response.getVersion().getNumber(), 1);
    assertEquals(response.getVersion().getCreatedTime(), 123456789L);
    assertEquals(response.getVersion().getStatus(), VersionStatus.ONLINE);
    assertEquals(response.getVersion().getPushJobId(), "test-push-job-123");
    assertEquals(response.getVersion().getPartitionCount(), 10);
    assertEquals(response.getVersion().getReplicationFactor(), 3);
    assertEquals(response.getSystemSchemaClusterD2ServiceName(), "testD2Service");
    assertEquals(response.getSystemSchemaClusterD2ZkHost(), "testZkHost");
  }

  @Test
  public void testGetRepushInfoWithoutFabric() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    Optional<String> fabric = Optional.empty();

    Version mockVersion = mock(Version.class);
    when(mockVersion.getNumber()).thenReturn(2);
    when(mockVersion.getCreatedTime()).thenReturn(987654321L);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.PUSHED);
    when(mockVersion.getPushJobId()).thenReturn("test-push-job-456");
    when(mockVersion.getPartitionCount()).thenReturn(5);
    when(mockVersion.getReplicationFactor()).thenReturn(2);

    RepushInfo mockRepushInfo =
        RepushInfo.createRepushInfo(mockVersion, "another.kafka.broker:9092", "anotherD2Service", "anotherZkHost");

    when(admin.getRepushInfo(clusterName, storeName, fabric)).thenReturn(mockRepushInfo);

    RepushInfo response = storeRequestHandler.getRepushInfo(clusterName, storeName, fabric);

    verify(admin, times(1)).getRepushInfo(clusterName, storeName, fabric);
    assertEquals(response.getKafkaBrokerUrl(), "another.kafka.broker:9092");
    assertEquals(response.getVersion().getNumber(), 2);
  }

  @Test
  public void testGetRepushInfoWithNullVersion() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    Optional<String> fabric = Optional.empty();

    RepushInfo mockRepushInfo = RepushInfo.createRepushInfo(null, "kafka.broker:9092", null, null);

    when(admin.getRepushInfo(clusterName, storeName, fabric)).thenReturn(mockRepushInfo);

    RepushInfo response = storeRequestHandler.getRepushInfo(clusterName, storeName, fabric);

    assertEquals(response.getKafkaBrokerUrl(), "kafka.broker:9092");
    assertTrue(response.getVersion() == null);
    assertTrue(response.getSystemSchemaClusterD2ServiceName() == null);
    assertTrue(response.getSystemSchemaClusterD2ZkHost() == null);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Repush info not available.*")
  public void testGetRepushInfoReturnsNullThrowsException() {
    when(admin.getRepushInfo("testCluster", "testStore", Optional.empty())).thenReturn(null);
    storeRequestHandler.getRepushInfo("testCluster", "testStore", Optional.empty());
  }

  @Test
  public void testGetStoreSuccess() {
    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn("testStore");
    when(mockStore.getOwner()).thenReturn("testOwner");
    when(mockStore.getCurrentVersion()).thenReturn(1);
    when(mockStore.getVersions()).thenReturn(Collections.emptyList());
    when(mockStore.getBackupVersionRetentionMs()).thenReturn(1000L);
    when(mockStore.getMaxRecordSizeBytes()).thenReturn(1024);
    when(admin.getStore("testCluster", "testStore")).thenReturn(mockStore);
    when(admin.getBackupVersionDefaultRetentionMs()).thenReturn(2000L);
    when(admin.getDefaultMaxRecordSizeBytes()).thenReturn(2048);
    when(admin.getCurrentVersionsForMultiColos("testCluster", "testStore")).thenReturn(Collections.emptyMap());
    when(admin.isSSLEnabledForPush("testCluster", "testStore")).thenReturn(false);
    when(admin.getKafkaBootstrapServers(false)).thenReturn("localhost:9092");

    StoreInfo response = storeRequestHandler.getStore("testCluster", "testStore");

    verify(admin, times(1)).getStore("testCluster", "testStore");
    assertEquals(response.getName(), "testStore");
  }

  @Test
  public void testGetStoreNotFound() {
    when(admin.getStore("testCluster", "nonExistent")).thenReturn(null);

    VeniceNoStoreException e =
        expectThrows(VeniceNoStoreException.class, () -> storeRequestHandler.getStore("testCluster", "nonExistent"));
    assertTrue(e.getMessage().contains("nonExistent"));
  }

  @Test
  public void testGetStoreWithDefaultRetentionMs() {
    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn("testStore");
    when(mockStore.getOwner()).thenReturn("testOwner");
    when(mockStore.getCurrentVersion()).thenReturn(1);
    when(mockStore.getVersions()).thenReturn(Collections.emptyList());
    when(mockStore.getBackupVersionRetentionMs()).thenReturn(-1L);
    when(mockStore.getMaxRecordSizeBytes()).thenReturn(-1);
    when(admin.getStore("testCluster", "testStore")).thenReturn(mockStore);
    when(admin.getBackupVersionDefaultRetentionMs()).thenReturn(86400000L);
    when(admin.getDefaultMaxRecordSizeBytes()).thenReturn(1048576);
    when(admin.getCurrentVersionsForMultiColos("testCluster", "testStore")).thenReturn(Collections.emptyMap());
    when(admin.isSSLEnabledForPush("testCluster", "testStore")).thenReturn(true);
    when(admin.getKafkaBootstrapServers(true)).thenReturn("localhost:9093");

    StoreInfo response = storeRequestHandler.getStore("testCluster", "testStore");

    verify(admin, times(1)).getBackupVersionDefaultRetentionMs();
    verify(admin, times(1)).getDefaultMaxRecordSizeBytes();
    assertEquals(response.getBackupVersionRetentionMs(), 86400000L);
    assertEquals(response.getMaxRecordSizeBytes(), 1048576);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is required")
  public void testGetStoreMissingClusterName() {
    storeRequestHandler.getStore("", "testStore");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store name is required")
  public void testGetStoreMissingStoreName() {
    storeRequestHandler.getStore("testCluster", "");
  }
}

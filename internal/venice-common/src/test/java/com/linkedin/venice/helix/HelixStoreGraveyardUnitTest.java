package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStoreAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixStoreGraveyardUnitTest {
  private HelixStoreGraveyard graveyard;

  @BeforeMethod
  void setUp() {
    graveyard =
        spy(new HelixStoreGraveyard(mock(ZkClient.class), new HelixAdapterSerializer(), Arrays.asList("clusterName")));
    doNothing().when(graveyard).updateZNode(anyString(), any());
  }

  @Test
  void testNoStoreFoundReturnsDefault() {
    String storeName = "non_existent_store";
    when(graveyard.getStoreFromAllClusters(storeName)).thenReturn(Collections.emptyList());

    int result = graveyard.getLargestUsedRTVersionNumber(storeName);
    assertEquals(result, Store.NON_EXISTING_VERSION);
  }

  @Test
  void testFindsLargestUsedRTVersionNumber() {
    String storeName = "test_store";

    Store store1 = mock(Store.class);
    Store store2 = mock(Store.class);
    when(store1.getLargestUsedRTVersionNumber()).thenReturn(2);
    when(store2.getLargestUsedRTVersionNumber()).thenReturn(5);

    List<Store> stores = Arrays.asList(store1, store2);
    when(graveyard.getStoreFromAllClusters(storeName)).thenReturn(stores);

    int result = graveyard.getLargestUsedRTVersionNumber(storeName);
    assertEquals(result, 5);
  }

  @Test
  void testPutStoreIntoGraveyardMigratingStoreUpdatesVersions() {
    Store store = mock(Store.class);
    when(store.isMigrating()).thenReturn(true);
    when(store.getName()).thenReturn("migrating_store");
    when(store.getLargestUsedVersionNumber()).thenReturn(2);
    when(store.getLargestUsedRTVersionNumber()).thenReturn(3);

    when(graveyard.getLargestUsedVersionNumber("migrating_store")).thenReturn(4);
    when(graveyard.getLargestUsedRTVersionNumber("migrating_store")).thenReturn(5);

    graveyard.putStoreIntoGraveyard("cluster1", store);

    verify(store).setLargestUsedVersionNumber(4);
    verify(store).setLargestUsedRTVersionNumber(5);
  }

  @Test
  void testNoDeletedStores() {
    String userStoreName = "userStore";
    when(graveyard.getStoreFromAllClusters(userStoreName)).thenReturn(Collections.emptyList());

    int result = graveyard.getPerUserStoreSystemStoreLargestUsedVersionNumber(
        userStoreName,
        VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE,
        true);

    assertEquals(result, Store.NON_EXISTING_VERSION);
  }

  @Test
  void testStoreWithValidSystemStoreAttributes() {
    String userStoreName = "userStore";
    Store deletedStore = mock(Store.class);
    SystemStoreAttributes attributes = mock(SystemStoreAttributes.class);
    when(attributes.getLargestUsedRTVersionNumber()).thenReturn(5);

    Map<String, SystemStoreAttributes> systemStoreNamesToAttributes = new HashMap<>();
    systemStoreNamesToAttributes.put(VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(), attributes);
    when(deletedStore.getSystemStores()).thenReturn(systemStoreNamesToAttributes);

    List<Store> deletedStores = Collections.singletonList(deletedStore);
    when(graveyard.getStoreFromAllClusters(userStoreName)).thenReturn(deletedStores);

    int result = graveyard.getPerUserStoreSystemStoreLargestUsedVersionNumber(
        userStoreName,
        VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE,
        true);

    assertEquals(result, 5);
  }

  @Test
  void testStoreWithDVCStoreAttributes() {
    String userStoreName = "userStore";
    Store deletedStore = mock(Store.class);
    SystemStoreAttributes attributes = mock(SystemStoreAttributes.class);
    when(attributes.getLargestUsedRTVersionNumber()).thenReturn(5);

    Map<String, SystemStoreAttributes> systemStoreNamesToAttributes = new HashMap<>();
    systemStoreNamesToAttributes.put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getPrefix(), attributes);
    when(deletedStore.getSystemStores()).thenReturn(systemStoreNamesToAttributes);

    List<Store> deletedStores = Collections.singletonList(deletedStore);
    when(graveyard.getStoreFromAllClusters(userStoreName)).thenReturn(deletedStores);

    int result = graveyard.getPerUserStoreSystemStoreLargestUsedVersionNumber(
        userStoreName,
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
        true);

    assertEquals(result, 5);
  }

  @Test
  void testGetStoreDeletedTime() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    String path = graveyard.getStoreGraveyardPath(clusterName, storeName);

    // Mock the dataAccessor to return a Stat with creation time
    ZkBaseDataAccessor<Store> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    graveyard.dataAccessor = mockDataAccessor;

    // Test case 1: Store exists in graveyard - should return creation time
    Stat mockStat = mock(Stat.class);
    long expectedCreationTime = 1234567890L;
    when(mockStat.getMtime()).thenReturn(expectedCreationTime);
    when(mockDataAccessor.getStat(eq(path), eq(AccessOption.PERSISTENT))).thenReturn(mockStat);

    long actualCreationTime = graveyard.getStoreDeletedTime(clusterName, storeName);
    assertEquals(actualCreationTime, expectedCreationTime);

    // Test case 2: Store does not exist in graveyard - should return STORE_NOT_IN_GRAVEYARD
    when(mockDataAccessor.getStat(eq(path), eq(AccessOption.PERSISTENT))).thenReturn(null);

    long nonExistentStoreTime = graveyard.getStoreDeletedTime(clusterName, storeName);
    assertEquals(nonExistentStoreTime, HelixStoreGraveyard.STORE_NOT_IN_GRAVEYARD);
  }
}

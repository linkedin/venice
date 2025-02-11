package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.StoreConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyStoreConfigRepository {
  private ZkStoreConfigAccessor mockAccessor;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;

  private static final String DEFAULT_STORE_NAME = "testGetStoreConfigStore";
  private static final String DEFAULT_META_SYSTEM_STORE_NAME = "venice_system_store_meta_store_testGetStoreConfigStore";
  private static final String DEFAULT_CLUSTER_NAME = "testGetStoreConfigCluster";
  private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(DEFAULT_STORE_NAME);
  static {
    DEFAULT_STORE_CONFIG.setCluster(DEFAULT_CLUSTER_NAME);
  }

  @BeforeMethod
  public void setUp() {
    mockAccessor = mock(ZkStoreConfigAccessor.class);
    storeConfigRepository = new HelixReadOnlyStoreConfigRepository(mock(ZkClient.class), mockAccessor);
  }

  @Test
  public void testGetStoreConfig() {
    List<String> list = new ArrayList<>();
    list.add(DEFAULT_STORE_NAME);
    doReturn(list).when(mockAccessor).getAllStores();
    // 1.) obtain a config doesn't exist in available store set
    storeConfigRepository.refresh();
    Assert.assertFalse(storeConfigRepository.getStoreConfig(DEFAULT_STORE_NAME + "test").isPresent());

    // 2.) obtain a config exists in available store set but not in cache

    // a.) ZK has no store config
    doReturn(null).when(mockAccessor).getStoreConfig(DEFAULT_STORE_NAME);
    Assert.assertFalse(storeConfigRepository.getStoreConfig(DEFAULT_STORE_NAME).isPresent());

    // b. ZK has the store config
    doReturn(DEFAULT_STORE_CONFIG).when(mockAccessor).getStoreConfig(DEFAULT_STORE_NAME);
    Assert.assertEquals(storeConfigRepository.getStoreConfig(DEFAULT_STORE_NAME).get(), DEFAULT_STORE_CONFIG);
    verify(mockAccessor, times(1)).subscribeStoreConfigDataChangedListener(eq(DEFAULT_STORE_NAME), any());

    // 3.) Obtain a config exists in available store set and in cache
    Assert.assertEquals(storeConfigRepository.getStoreConfig(DEFAULT_STORE_NAME).get(), DEFAULT_STORE_CONFIG);
    // the invocation count should not increase and remain at 1
    verify(mockAccessor, times(1)).subscribeStoreConfigDataChangedListener(eq(DEFAULT_STORE_NAME), any());

    // 4.) Obtain the config for the system store. It should use the same config for the store
    Assert
        .assertEquals(storeConfigRepository.getStoreConfig(DEFAULT_META_SYSTEM_STORE_NAME).get(), DEFAULT_STORE_CONFIG);
    verify(mockAccessor, times(1)).subscribeStoreConfigDataChangedListener(eq(DEFAULT_STORE_NAME), any());
  }

  @Test
  // when the store config changes among fetches, it should be updated in the cache
  public void testGetStoreConfigRace() {
    List<String> list = new ArrayList<>();
    list.add(DEFAULT_STORE_NAME);
    doReturn(list).when(mockAccessor).getAllStores();
    storeConfigRepository.refresh();

    StoreConfig copiedConfig = DEFAULT_STORE_CONFIG.cloneStoreConfig();
    copiedConfig.setCluster("testGetStoreConfigCluster2");
    doReturn(DEFAULT_STORE_CONFIG).doReturn(copiedConfig).when(mockAccessor).getStoreConfig(DEFAULT_STORE_NAME);
    Assert.assertEquals(storeConfigRepository.getStoreConfig(DEFAULT_STORE_NAME).get(), copiedConfig);
  }

  @Test
  public void testRefreshAndClear() {
    int storeCount = 10;
    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < storeCount; i++) {
      String name = "testRefreshAndClearStore" + i;
      storeNames.add(name);
    }
    doReturn(storeNames).when(mockAccessor).getAllStores();

    storeConfigRepository.refresh();
    for (int i = 0; i < storeCount; i++) {
      Assert.assertTrue(storeConfigRepository.getAvailableStoreSet().contains("testRefreshAndClearStore" + i));
    }
    Assert.assertEquals(storeConfigRepository.getLoadedStoreConfigMap().size(), 0);

    storeConfigRepository.clear();
    for (int i = 0; i < storeCount; i++) {
      Assert.assertFalse(storeConfigRepository.getAvailableStoreSet().contains("testRefreshAndClearStore" + i));
    }
    Assert.assertEquals(storeConfigRepository.getLoadedStoreConfigMap().size(), 0);
  }

  @Test
  public void testGetStoreConfigChildrenChangedNotification() throws Exception {
    String STORE_PREFIX = "testRefreshAndClearStore";
    String CLUSTER_PREFIX = "testRefreshAndClearCluster";
    HelixReadOnlyStoreConfigRepository.StoreConfigAddedOrDeletedChangedListener listener =
        storeConfigRepository.getStoreConfigAddedOrDeletedListener();
    int storeCount = 10;
    List<StoreConfig> list = new ArrayList<>();
    List<String> storeList = new ArrayList<>();
    for (int i = 0; i < storeCount; i++) {
      StoreConfig config = new StoreConfig(STORE_PREFIX + i);
      storeList.add(STORE_PREFIX + i);
      config.setCluster(CLUSTER_PREFIX + i);
      list.add(config);
    }
    doReturn(storeList).when(mockAccessor).getAllStores();
    storeConfigRepository.refresh();
    for (int i = 0; i < storeCount; i++) {
      doReturn(list.get(i)).when(mockAccessor).getStoreConfig(STORE_PREFIX + i);
      storeConfigRepository.getStoreConfig(STORE_PREFIX + i);
    }

    List<String> storeNames = list.stream().map(config -> config.getStoreName()).collect(Collectors.toList());
    storeNames.remove(0);
    String newStoreName = "testRefreshAndClearStoreNew";
    storeNames.add(newStoreName);

    StoreConfig newStoreConfig = new StoreConfig(newStoreName);
    newStoreConfig.setCluster("testRefreshAndClearClusterNew");

    listener.handleChildChange("", storeNames);

    Assert.assertFalse(storeConfigRepository.getStoreConfig(STORE_PREFIX + 0).isPresent());
    Assert.assertTrue(storeConfigRepository.getAvailableStoreSet().contains(newStoreName));
  }

  @Test
  public void testGetUpdateStoreConfigNotification() throws Exception {
    String storeName = "testGetUpdateStoreConfigNotification";
    StoreConfig config = new StoreConfig(storeName);
    List<String> list = new ArrayList<>();
    list.add(storeName);
    doReturn(list).when(mockAccessor).getAllStores();
    config.setCluster("testCluster");
    doReturn(config).when(mockAccessor).getStoreConfig(storeName);
    storeConfigRepository.refresh();
    storeConfigRepository.getStoreConfigOrThrow(storeName);

    HelixReadOnlyStoreConfigRepository.StoreConfigChangedListener listener =
        storeConfigRepository.getStoreConfigChangedListener();
    StoreConfig newConfig = new StoreConfig(storeName);
    newConfig.setCluster("newCluster");
    listener.handleDataChange("", newConfig);

    Assert.assertEquals(storeConfigRepository.getStoreConfig(storeName).get().getCluster(), newConfig.getCluster());
  }
}

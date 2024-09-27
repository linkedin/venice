package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.StoreConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyStoreConfigRepository {
  private ZkStoreConfigAccessor mockAccessor;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;

  @BeforeMethod
  public void setUp() {
    mockAccessor = mock(ZkStoreConfigAccessor.class);
    storeConfigRepository = new HelixReadOnlyStoreConfigRepository(mock(ZkClient.class), mockAccessor, 1, 1000);
  }

  @Test
  public void testGetStoreConfig() {
    String clusterName = "testGetStoreConfigCluster";
    String storeName = "testGetStoreConfigStore";
    StoreConfig config = new StoreConfig(storeName);
    config.setCluster(clusterName);
    List<StoreConfig> list = new ArrayList<>();
    list.add(config);
    doReturn(list).when(mockAccessor).getAllStoreConfigs(1, 1000);
    storeConfigRepository.refresh();
    Assert.assertEquals(
        storeConfigRepository.getStoreConfig(storeName).get().getCluster(),
        clusterName,
        "Should get the cluster from config correctly.");
    Assert.assertFalse(
        storeConfigRepository.getStoreConfig("non-existing-store").isPresent(),
        "Store config should not exist.");
  }

  @Test
  public void testRefreshAndClear() {
    int storeCount = 10;
    List<StoreConfig> list = new ArrayList<>();
    for (int i = 0; i < storeCount; i++) {
      StoreConfig config = new StoreConfig("testRefreshAndClearStore" + i);
      config.setCluster("testRefreshAndClearCluster" + i);
      list.add(config);
    }
    doReturn(list).when(mockAccessor).getAllStoreConfigs(1, 1000);

    storeConfigRepository.refresh();
    for (int i = 0; i < storeCount; i++) {
      Assert.assertEquals(
          storeConfigRepository.getStoreConfig("testRefreshAndClearStore" + i).get().getCluster(),
          "testRefreshAndClearCluster" + i,
          "Should already load all configs correctly.");
    }
    storeConfigRepository.clear();
    for (int i = 0; i < storeCount; i++) {
      Assert.assertFalse(
          storeConfigRepository.getStoreConfig("testRefreshAndClearStore" + i).isPresent(),
          "Should already clear all configs correctly.");
    }
  }

  @Test
  public void testGetStoreConfigChildrenChangedNotification() throws Exception {
    HelixReadOnlyStoreConfigRepository.StoreConfigAddedOrDeletedChangedListener listener =
        storeConfigRepository.getStoreConfigAddedOrDeletedListener();
    int storeCount = 10;
    List<StoreConfig> list = new ArrayList<>();
    for (int i = 0; i < storeCount; i++) {
      StoreConfig config = new StoreConfig("testRefreshAndClearStore" + i);
      config.setCluster("testRefreshAndClearCluster" + i);
      list.add(config);
    }
    doReturn(list).when(mockAccessor).getAllStoreConfigs(1, 1000);
    storeConfigRepository.refresh();

    List<String> storeNames = list.stream().map(config -> config.getStoreName()).collect(Collectors.toList());
    storeNames.remove(0);
    String newStoreName = "testRefreshAndClearStoreNew";
    storeNames.add(newStoreName);

    List<String> newStoreNames = new ArrayList<>();
    newStoreNames.add(newStoreName);
    List<StoreConfig> newStoreConfigList = new ArrayList<>();
    StoreConfig newStoreConfig = new StoreConfig(newStoreName);
    newStoreConfig.setCluster("testRefreshAndClearClusterNew");
    newStoreConfigList.add(newStoreConfig);
    doReturn(newStoreConfigList).when(mockAccessor).getStoreConfigs(eq(newStoreNames));

    listener.handleChildChange("", storeNames);

    Assert.assertFalse(storeConfigRepository.getStoreConfig("testRefreshAndClearStore" + 0).isPresent());
    Assert.assertEquals(
        storeConfigRepository.getStoreConfig(newStoreName).get().getCluster(),
        newStoreConfig.getCluster());
  }

  @Test
  public void testGetUpdateStoreConfigNotification() throws Exception {
    String storeNAme = "testGetUpdateStoreConfigNotification";
    List<StoreConfig> list = new ArrayList<>();
    StoreConfig config = new StoreConfig(storeNAme);
    config.setCluster("testCluster");
    list.add(config);
    doReturn(list).when(mockAccessor).getAllStoreConfigs(1, 1000);
    storeConfigRepository.refresh();

    HelixReadOnlyStoreConfigRepository.StoreConfigChangedListener listener =
        storeConfigRepository.getStoreConfigChangedListener();
    StoreConfig newConfig = new StoreConfig(storeNAme);
    newConfig.setCluster("newCluster");
    listener.handleDataChange("", newConfig);

    Assert.assertEquals(storeConfigRepository.getStoreConfig(storeNAme).get().getCluster(), newConfig.getCluster());
  }

  @Test
  public void testStoreConfigLazyFetch() {
    String storeName = "testLazyFetchStore";
    Optional<StoreConfig> storeConfigOptional = storeConfigRepository.getStoreConfig(storeName);
    // config is empty
    Assert.assertFalse(storeConfigOptional.isPresent());

    // config is fetched
    StoreConfig config = mock(StoreConfig.class);
    doReturn(config).when(config).cloneStoreConfig();
    doReturn(config).when(mockAccessor).getStoreConfig(storeName);
    storeConfigOptional = storeConfigRepository.getStoreConfig(storeName);
    Assert.assertEquals(storeConfigOptional.get(), config);
    verify(mockAccessor).subscribeStoreConfigDataChangedListener(eq(storeName), any());
  }
}

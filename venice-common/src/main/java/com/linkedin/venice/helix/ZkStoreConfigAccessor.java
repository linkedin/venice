package com.linkedin.venice.helix;

import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class ZkStoreConfigAccessor {
  public static final Logger logger = Logger.getLogger(ZkStoreConfigAccessor.class);
  private static final String ROOT_PATH = "/storeConfigs";

  private final ZkClient zkClient;
  private final ZkBaseDataAccessor<StoreConfig> dataAccessor;

  public ZkStoreConfigAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkClient = zkClient;
    adapterSerializer.registerSerializer(ROOT_PATH + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY,
        new StoreConfigJsonSerializer());
    this.zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(this.zkClient);
  }

  public List<String> getAllStores() {
    return dataAccessor.getChildNames(ROOT_PATH, AccessOption.PERSISTENT);
  }

  public synchronized boolean containsConfig(String store) {
    return dataAccessor.exists(getStoreConfigPath(store), AccessOption.PERSISTENT);
  }

  public synchronized StoreConfig getConfig(String store) {
    return dataAccessor.get(getStoreConfigPath(store), null, AccessOption.PERSISTENT);
  }

  public synchronized void createConfig(String store, String cluster) {
    StoreConfig config = new StoreConfig(store);
    config.setCluster(cluster);
    HelixUtils.create(dataAccessor, getStoreConfigPath(store), config);
  }

  public synchronized void updateConfig(StoreConfig config) {
    HelixUtils.compareAndUpdate(dataAccessor, getStoreConfigPath(config.getStoreName()), currentData -> config);
  }

  public synchronized void deleteConfig(String store) {
    HelixUtils.remove(dataAccessor, getStoreConfigPath(store));
  }

  private static String getStoreConfigPath(String storeName) {
    return ROOT_PATH + "/" + storeName;
  }
}

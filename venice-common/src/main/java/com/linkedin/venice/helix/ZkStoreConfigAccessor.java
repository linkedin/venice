package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.log4j.Logger;


/**
 * The class is used to access the store configs in Zookeeper.
 * <p/>
 * This class is non-cluster specified.
 */
public class ZkStoreConfigAccessor {
  public static final Logger logger = Logger.getLogger(ZkStoreConfigAccessor.class);
  private static final String ROOT_PATH = "/storeConfigs";

  private final ZkClient zkClient;
  private final ZkBaseDataAccessor<StoreConfig> dataAccessor;

  public ZkStoreConfigAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkClient = zkClient;
    adapterSerializer
        .registerSerializer(ROOT_PATH + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY, new StoreConfigJsonSerializer());
    adapterSerializer.registerSerializer(ROOT_PATH, new VeniceJsonSerializer<>(Integer.TYPE));
    this.zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(this.zkClient);
  }

  public List<String> getAllStores() {
    return dataAccessor.getChildNames(ROOT_PATH, AccessOption.PERSISTENT);
  }

  public List<StoreConfig> getAllStoreConfigs(int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    // Only return not null configs.
    List<StoreConfig> configs = HelixUtils.getChildren(dataAccessor, ROOT_PATH, refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs)
        .stream()
        .filter(storeConfig -> storeConfig != null)
        .collect(Collectors.toList());

    logger.info("Read " + configs.size() + " store configs from path:" + ROOT_PATH);
    return configs;
  }

  public synchronized boolean containsConfig(String store) {
    return dataAccessor.exists(getStoreConfigPath(store), AccessOption.PERSISTENT);
  }

  public synchronized StoreConfig getStoreConfig(String store) {
    return dataAccessor.get(getStoreConfigPath(store), null, AccessOption.PERSISTENT);
  }

  public synchronized List<StoreConfig> getStoreConfigs(List<String> stores) {
    List<String> pathes = stores.stream().map(store -> getStoreConfigPath(store)).collect(Collectors.toList());
    return dataAccessor.get(pathes, null, AccessOption.PERSISTENT);
  }

  public synchronized void createConfig(String store, String cluster) {
    StoreConfig config = new StoreConfig(store);
    config.setCluster(cluster);
    HelixUtils.create(dataAccessor, getStoreConfigPath(store), config);
  }

  public synchronized void updateConfig(StoreConfig config) {
    HelixUtils.compareAndUpdate(dataAccessor, getStoreConfigPath(config.getStoreName()), currentData -> config);
  }

  public void subscribeStoreConfigDataChangedListener(String storeName, IZkDataListener listener) {
    dataAccessor.subscribeDataChanges(getStoreConfigPath(storeName), listener);
  }

  public void unsubscribeStoreConfigDataChangedListener(String storeName, IZkDataListener listener) {
    dataAccessor.unsubscribeDataChanges(getStoreConfigPath(storeName), listener);
  }

  public void subscribeStoreConfigAddedOrDeletedListener(IZkChildListener listener) {
    dataAccessor.subscribeChildChanges(ROOT_PATH, listener);
  }

  public void unsubscribeStoreConfigAddedOrDeletedListener(IZkChildListener listener) {
    dataAccessor.unsubscribeChildChanges(ROOT_PATH, listener);
  }

  public synchronized void deleteConfig(String store) {
    HelixUtils.remove(dataAccessor, getStoreConfigPath(store));
  }

  private static String getStoreConfigPath(String storeName) {
    return ROOT_PATH + "/" + storeName;
  }
}

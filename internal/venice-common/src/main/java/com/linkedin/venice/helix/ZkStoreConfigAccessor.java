package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The class is used to access the store configs in Zookeeper.
 * <p/>
 * This class is non-cluster specified.
 */
public class ZkStoreConfigAccessor {
  private static final Logger LOGGER = LogManager.getLogger(ZkStoreConfigAccessor.class);
  private static final String ROOT_PATH = "/" + STORE_CONFIGS;

  private final ZkClient zkClient;
  private final ZkBaseDataAccessor<StoreConfig> dataAccessor;
  private final Optional<MetaStoreWriter> metaStoreWriter;

  public ZkStoreConfigAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      Optional<MetaStoreWriter> metaStoreWriter) {
    this.zkClient = zkClient;
    adapterSerializer
        .registerSerializer(ROOT_PATH + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY, new StoreConfigJsonSerializer());
    adapterSerializer.registerSerializer(ROOT_PATH, new VeniceJsonSerializer<>(Integer.TYPE));
    this.zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(this.zkClient);
    this.metaStoreWriter = metaStoreWriter;
  }

  public List<String> getAllStores() {
    return HelixUtils.listPathContents(dataAccessor, ROOT_PATH);
  }

  public synchronized boolean containsConfig(String store) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(store);
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
      /**
       * For meta system store, Controller will use the same {@link StoreConfig} as the regular Venice store
       * during migration.
       */
      store = systemStoreType.extractRegularStoreName(store);
    }
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
      store = systemStoreType.extractRegularStoreName(store);
    }
    return dataAccessor.exists(getStoreConfigPath(store), AccessOption.PERSISTENT);
  }

  public synchronized StoreConfig getStoreConfig(String store) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(store);
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
      /**
       * For meta system store, Controller will use the same {@link StoreConfig} as the regular Venice store
       * during migration.
       */
      store = systemStoreType.extractRegularStoreName(store);
    }
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
      store = systemStoreType.extractRegularStoreName(store);
    }
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

  public synchronized void updateConfig(StoreConfig config, boolean isStoreMetaSystemStoreEnabled) {
    HelixUtils.compareAndUpdate(dataAccessor, getStoreConfigPath(config.getStoreName()), currentData -> config);
    if (isStoreMetaSystemStoreEnabled && metaStoreWriter.isPresent()) {
      metaStoreWriter.get().writeStoreClusterConfig(config);
    }
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

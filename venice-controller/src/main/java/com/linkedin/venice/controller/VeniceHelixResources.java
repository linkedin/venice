package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.helix.HelixOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;

/**
 * Aggregate all of essentials resources which is required by controller in one place.
 * <p>
 * All resources in this class is dedicated for one Venice cluster.
 */
public class VeniceHelixResources implements VeniceResource {
  private final HelixManager controller;
  private final HelixReadWriteStoreRepository metadataRepository;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixReadWriteSchemaRepository schemaRepository;
  private final HelixStatusMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;
  private final OfflinePushMonitor OfflinePushMonitor;
  private final HelixStoreGraveyard storeGraveyard;
  private final ZkRoutersClusterManager routersClusterManager;
  private final AggPartitionHealthStats aggPartitionHealthStats;
  private final ZkStoreConfigAccessor storeConfigAccessor;

  public VeniceHelixResources(String clusterName,
                              ZkClient zkClient,
                              HelixAdapterSerializer adapterSerializer,
                              HelixManager helixManager,
                              VeniceControllerClusterConfig config,
                              StoreCleaner storeCleaner,
                              Map<String, MetricsRepository> metricsRepositories) {
    this.config = config;
    this.controller = helixManager;
    this.metadataRepository = new HelixReadWriteStoreRepository(zkClient, adapterSerializer, clusterName);
    this.schemaRepository = new HelixReadWriteSchemaRepository(this.metadataRepository,
        zkClient, adapterSerializer, clusterName);
    this.routingDataRepository = new HelixRoutingDataRepository(helixManager);
    this.messageChannel = new HelixStatusMessageChannel(helixManager, HelixStatusMessageChannel.DEFAULT_BROAD_CAST_MESSAGES_TIME_OUT);
    this.OfflinePushMonitor = new OfflinePushMonitor(clusterName, routingDataRepository,
        new HelixOfflinePushMonitorAccessor(clusterName, zkClient, adapterSerializer), storeCleaner, metadataRepository);
    storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, clusterName);
    routersClusterManager = new ZkRoutersClusterManager(zkClient, adapterSerializer, clusterName);
    aggPartitionHealthStats =
        new AggPartitionHealthStats(metricsRepositories.get(clusterName), routingDataRepository, metadataRepository,
            config.getReplicaFactor());
    this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer);
  }

  @Override
  public void refresh() {
    clear();
    metadataRepository.refresh();
    schemaRepository.refresh();
    routingDataRepository.refresh();
    OfflinePushMonitor.loadAllPushes();
    routersClusterManager.refresh();
    repairStoreConfigs();
  }

  @Override
  public void clear() {
    metadataRepository.clear();
    schemaRepository.clear();
    routingDataRepository.clear();
    routersClusterManager.clear();
  }

  public HelixReadWriteStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  public HelixReadWriteSchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  public HelixRoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  public HelixStatusMessageChannel getMessageChannel() {
    return messageChannel;
  }

  public HelixManager getController() {
    return controller;
  }

  public VeniceControllerClusterConfig getConfig() {
    return config;
  }

  public OfflinePushMonitor getOfflinePushMonitor() {
    return OfflinePushMonitor;
  }
  public HelixStoreGraveyard getStoreGraveyard() {
    return storeGraveyard;
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public AggPartitionHealthStats getAggPartitionHealthStats() {
    return aggPartitionHealthStats;
  }

  /**
   * As the old store has not been added into store config map. So we repair the mapping here.
   * // TODO this code should be removed after we do a successful deployment and repair all stores.
   */
  private void repairStoreConfigs() {
    metadataRepository.lock();
    try {
      metadataRepository.getAllStores()
          .stream()
          .filter(store -> !storeConfigAccessor.containsConfig(store.getName()))
          .forEach(store -> {
            storeConfigAccessor.createConfig(store.getName(), config.getClusterName());
          });
    } finally {
      metadataRepository.unLock();
    }
  }
}

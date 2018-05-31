package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.pushmonitor.AggPushHealthStats;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import io.tehuti.metrics.MetricsRepository;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZkClient;

/**
 * Aggregate all of essentials resources which is required by controller in one place.
 * <p>
 * All resources in this class is dedicated for one Venice cluster.
 */
public class VeniceHelixResources implements VeniceResource {
  private final SafeHelixManager controller;
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
                              SafeHelixManager helixManager,
                              VeniceControllerClusterConfig config,
                              StoreCleaner storeCleaner, MetricsRepository metricsRepository) {
    this.config = config;
    this.controller = helixManager;
    this.metadataRepository = new HelixReadWriteStoreRepository(zkClient, adapterSerializer, clusterName,
        config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.schemaRepository =
        new HelixReadWriteSchemaRepository(this.metadataRepository, zkClient, adapterSerializer, clusterName);
    // Use the separate helix manger for listening on the external view to prevent it from blocking state transition and messages.
    SafeHelixManager spectatorManager = getSpectatorManager(clusterName, zkClient.getServers());
    this.routingDataRepository = new HelixRoutingDataRepository(spectatorManager);
    this.messageChannel = new HelixStatusMessageChannel(helixManager, config.getHelixSendMessageTimeoutMs());
    this.OfflinePushMonitor = new OfflinePushMonitor(clusterName, routingDataRepository,
        new HelixOfflinePushMonitorAccessor(clusterName, zkClient, adapterSerializer,
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs()), storeCleaner,
        metadataRepository, new AggPushHealthStats(clusterName, metricsRepository));
    storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, clusterName);
    // On controller side, router cluster manager is used as an accessor without maintaining any cache, so do not need to refresh once zk reconnected.
    routersClusterManager =
        new ZkRoutersClusterManager(zkClient, adapterSerializer, clusterName, config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs());
    aggPartitionHealthStats =
        new AggPartitionHealthStats(clusterName, metricsRepository, routingDataRepository, metadataRepository,
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

  public SafeHelixManager getController() {
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

  private SafeHelixManager getSpectatorManager(String clusterName, String zkAddress) {
    SafeHelixManager manager =
        new SafeHelixManager(HelixManagerFactory.getZKHelixManager(clusterName, "", InstanceType.SPECTATOR, zkAddress));
    try {
      manager.connect();
      return manager;
    } catch (Exception e) {
      throw new VeniceException("Spectator manager could not connect to cluster: " + clusterName, e);
    }
  }
}

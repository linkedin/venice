package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepositoryAdapter;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.AggPushHealthStats;
import com.linkedin.venice.pushmonitor.AggPushStatusCleanUpStats;
import com.linkedin.venice.pushmonitor.LeakedPushStatusCleanUpService;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Aggregate all essentials resources required by controller to manage a Venice cluster.
 * <p>
 * All resources in this class is dedicated for one Venice cluster.
 */
public class HelixVeniceClusterResources implements VeniceResource {
  private static final Logger LOGGER = LogManager.getLogger(HelixVeniceClusterResources.class);

  private final String clusterName;
  private final SafeHelixManager helixManager;
  private final ClusterLockManager clusterLockManager;
  private final ReadWriteStoreRepository storeMetadataRepository;
  private final HelixExternalViewRepository routingDataRepository;
  private HelixCustomizedViewOfflinePushRepository customizedViewRepo;
  private final ReadWriteSchemaRepository schemaRepository;
  private final HelixStatusMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;
  private final PushMonitorDelegator pushMonitor;
  private final LeakedPushStatusCleanUpService leakedPushStatusCleanUpService;
  private final ZkRoutersClusterManager routersClusterManager;
  private final AggPartitionHealthStats aggPartitionHealthStats;
  private final ZkStoreConfigAccessor storeConfigAccessor;
  private final Optional<DynamicAccessController> accessController;
  private final ExecutorService errorPartitionResetExecutorService = Executors.newSingleThreadExecutor();
  private final StoragePersonaRepository storagePersonaRepository;

  private ErrorPartitionResetTask errorPartitionResetTask = null;
  private final Optional<MetaStoreWriter> metaStoreWriter;
  private final VeniceAdminStats veniceAdminStats;
  private final VeniceHelixAdmin admin;

  public HelixVeniceClusterResources(
      String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      SafeHelixManager helixManager,
      VeniceControllerConfig config,
      VeniceHelixAdmin admin,
      MetricsRepository metricsRepository,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      Optional<DynamicAccessController> accessController,
      HelixAdminClient helixAdminClient) {
    this.clusterName = clusterName;
    this.config = config;
    this.helixManager = helixManager;
    this.admin = admin;
    /**
     * So far, Meta system store doesn't support write from parent cluster.
     */
    if (!config.isParent()) {
      metaStoreWriter = Optional.of(admin.getMetaStoreWriter());
    } else {
      metaStoreWriter = Optional.empty();
    }
    /**
     * ClusterLockManager is created per cluster and shared between {@link VeniceHelixAdmin},
     * {@link com.linkedin.venice.pushmonitor.AbstractPushMonitor} and {@link HelixReadWriteStoreRepository}.
     */
    this.clusterLockManager = new ClusterLockManager(clusterName);
    HelixReadWriteStoreRepository readWriteStoreRepository = new HelixReadWriteStoreRepository(
        zkClient,
        adapterSerializer,
        clusterName,
        metaStoreWriter,
        clusterLockManager);
    this.storeMetadataRepository = new HelixReadWriteStoreRepositoryAdapter(
        admin.getReadOnlyZKSharedSystemStoreRepository(),
        readWriteStoreRepository,
        clusterName);
    this.schemaRepository = new HelixReadWriteSchemaRepositoryAdapter(
        admin.getReadOnlyZKSharedSchemaRepository(),
        new HelixReadWriteSchemaRepository(
            readWriteStoreRepository,
            zkClient,
            adapterSerializer,
            clusterName,
            metaStoreWriter));

    SafeHelixManager spectatorManager;
    if (this.helixManager.getInstanceType() == InstanceType.SPECTATOR) {
      // HAAS is enabled for storage clusters therefore the helix manager is connected as a spectator and it can be
      // used directly for external view purposes.
      spectatorManager = this.helixManager;
    } else {
      // Use a separate helix manger for listening on the external view to prevent it from blocking state transition and
      // messages.
      spectatorManager = getSpectatorManager(clusterName, zkClient.getServers());
    }
    this.routingDataRepository = new HelixExternalViewRepository(spectatorManager);
    this.customizedViewRepo = new HelixCustomizedViewOfflinePushRepository(this.helixManager);
    this.messageChannel = new HelixStatusMessageChannel(
        helixManager,
        new HelixMessageChannelStats(metricsRepository, clusterName),
        config.getHelixSendMessageTimeoutMs());
    VeniceOfflinePushMonitorAccessor offlinePushMonitorAccessor = new VeniceOfflinePushMonitorAccessor(
        clusterName,
        zkClient,
        adapterSerializer,
        config.getRefreshAttemptsForZkReconnect(),
        config.getRefreshIntervalForZkReconnectInMs());
    String aggregateRealTimeSourceKafkaUrl =
        config.getChildDataCenterKafkaUrlMap().get(config.getAggregateRealTimeSourceRegion());
    boolean unregisterMetricEnabled = config.isUnregisterMetricForDeletedStoreEnabled();

    this.pushMonitor = new PushMonitorDelegator(
        clusterName,
        routingDataRepository,
        offlinePushMonitorAccessor,
        admin,
        storeMetadataRepository,
        new AggPushHealthStats(clusterName, metricsRepository, storeMetadataRepository, unregisterMetricEnabled),
        realTimeTopicSwitcher,
        clusterLockManager,
        aggregateRealTimeSourceKafkaUrl,
        getActiveActiveRealTimeSourceKafkaURLs(config),
        helixAdminClient,
        config.isErrorLeaderReplicaFailOverEnabled(),
        config.getOffLineJobWaitTimeInMilliseconds());

    this.leakedPushStatusCleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        offlinePushMonitorAccessor,
        storeMetadataRepository,
        admin,
        new AggPushStatusCleanUpStats(clusterName, metricsRepository, storeMetadataRepository, unregisterMetricEnabled),
        this.config.getLeakedPushStatusCleanUpServiceSleepIntervalInMs(),
        this.config.getLeakedResourceAllowedLingerTimeInMs());
    // On controller side, router cluster manager is used as an accessor without maintaining any cache, so do not need
    // to refresh once zk reconnected.
    this.routersClusterManager = new ZkRoutersClusterManager(
        zkClient,
        adapterSerializer,
        clusterName,
        config.getRefreshAttemptsForZkReconnect(),
        config.getRefreshIntervalForZkReconnectInMs());
    this.aggPartitionHealthStats = new AggPartitionHealthStats(
        clusterName,
        metricsRepository,
        routingDataRepository,
        storeMetadataRepository,
        pushMonitor);
    this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer, metaStoreWriter);
    this.accessController = accessController;
    if (config.getErrorPartitionAutoResetLimit() > 0) {
      errorPartitionResetTask = new ErrorPartitionResetTask(
          clusterName,
          helixAdminClient,
          storeMetadataRepository,
          routingDataRepository,
          pushMonitor,
          metricsRepository,
          config.getErrorPartitionAutoResetLimit(),
          config.getErrorPartitionProcessingCycleDelay());
    }
    veniceAdminStats = new VeniceAdminStats(metricsRepository, "venice-admin-" + clusterName);
    this.storagePersonaRepository =
        new StoragePersonaRepository(clusterName, this.storeMetadataRepository, adapterSerializer, zkClient);
  }

  private List<String> getActiveActiveRealTimeSourceKafkaURLs(VeniceControllerConfig config) {
    List<String> kafkaURLs = new ArrayList<>(config.getActiveActiveRealTimeSourceFabrics().size());
    for (String fabric: config.getActiveActiveRealTimeSourceFabrics()) {
      String kafkaURL = config.getChildDataCenterKafkaUrlMap().get(fabric);
      if (kafkaURL == null) {
        throw new VeniceException(
            String.format(
                "No A/A source Kafka URL found for fabric %s in %s",
                fabric,
                config.getChildDataCenterKafkaUrlMap()));
      }
      kafkaURLs.add(kafkaURL);
    }
    return Collections.unmodifiableList(kafkaURLs);
  }

  /**
   * This function is used to repair all the stores with replication factor: 0.
   * And these stores will be updated to use the replication factor configured in cluster level.
   */
  private void repairStoreReplicationFactor(ReadWriteStoreRepository metadataRepository) {
    List<Store> stores = metadataRepository.getAllStores();
    for (Store store: stores) {
      String storeName = store.getName();
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
      if (systemStoreType == null || !systemStoreType.isStoreZkShared()) {
        /**
         * We only need to update the Vanilla Venice stores here since the updated zk shared store
         * will be reflected in all the derived system stores.
         */
        if (store.getReplicationFactor() <= 0) {
          int previousReplicationFactor = store.getReplicationFactor();
          store.setReplicationFactor(config.getReplicationFactor());
          metadataRepository.updateStore(store);
          LOGGER.info(
              "Updated replication factor from {} to {} for store: {}, in cluster: {}",
              previousReplicationFactor,
              config.getReplicationFactor(),
              store.getName(),
              clusterName);
        }
      }
    }
  }

  @Override
  public void refresh() {
    clear();
    // Make sure that metadataRepo is initialized first since schemaRepo and pushMonitor depend on it.
    storeMetadataRepository.refresh();
    repairStoreReplicationFactor(storeMetadataRepository);

    // Initialize the dynamic access client and also register the acl creation/deletion listener.
    if (accessController.isPresent()) {
      DynamicAccessController accessClient = accessController.get();
      accessClient
          .init(storeMetadataRepository.getAllStores().stream().map(Store::getName).collect(Collectors.toList()));
      storeMetadataRepository.registerStoreDataChangedListener(new AclCreationDeletionListener(accessClient));
    }
    schemaRepository.refresh();
    routingDataRepository.refresh();
    customizedViewRepo.refresh();
    pushMonitor.loadAllPushes();
    routersClusterManager.refresh();
    admin.startInstanceMonitor(clusterName);
  }

  @Override
  public void clear() {
    /**
     * Also stop monitoring all the pushes; otherwise, the standby controller host will still listen to
     * push status changes and act on the changes which should have been done by leader controller only,
     * like broadcasting StartOfBufferReplay/TopicSwitch messages.
     */
    pushMonitor.stopAllMonitoring();
    storeMetadataRepository.clear();
    schemaRepository.clear();
    routingDataRepository.clear();
    customizedViewRepo.clear();
    routersClusterManager.clear();
    admin.clearInstanceMonitor(clusterName);
  }

  /**
   * Cause {@link ErrorPartitionResetTask} service to begin executing.
   */
  public void startErrorPartitionResetTask() {
    if (errorPartitionResetTask != null) {
      errorPartitionResetExecutorService.submit(errorPartitionResetTask);
    }
  }

  /**
   * Cause {@link ErrorPartitionResetTask} service to stop executing.
   */
  public void stopErrorPartitionResetTask() {
    if (errorPartitionResetTask != null) {
      errorPartitionResetTask.close();
      errorPartitionResetExecutorService.shutdown();
      try {
        errorPartitionResetExecutorService.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Cause {@link LeakedPushStatusCleanUpService} service to begin executing.
   */
  public void startLeakedPushStatusCleanUpService() {
    if (leakedPushStatusCleanUpService != null) {
      leakedPushStatusCleanUpService.start();
    }
  }

  /**
   * Cause {@link LeakedPushStatusCleanUpService} service to stop executing.
   */
  public void stopLeakedPushStatusCleanUpService() {
    if (leakedPushStatusCleanUpService != null) {
      try {
        leakedPushStatusCleanUpService.stop();
      } catch (Exception e) {
        LOGGER.error("Error when stopping leaked push status clean-up service for cluster: {}", clusterName);
      }
    }
  }

  public ReadWriteStoreRepository getStoreMetadataRepository() {
    return storeMetadataRepository;
  }

  public ReadWriteSchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  public HelixExternalViewRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  public HelixCustomizedViewOfflinePushRepository getCustomizedViewRepository() {
    return customizedViewRepo;
  }

  // setCustomizedViewRepository is used for testing only.
  void setCustomizedViewRepository(HelixCustomizedViewOfflinePushRepository repo) {
    customizedViewRepo = repo;
  }

  public HelixStatusMessageChannel getMessageChannel() {
    return messageChannel;
  }

  public SafeHelixManager getHelixManager() {
    return helixManager;
  }

  public VeniceControllerClusterConfig getConfig() {
    return config;
  }

  public PushMonitorDelegator getPushMonitor() {
    return pushMonitor;
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public Optional<MetaStoreWriter> getMetaStoreWriter() {
    return metaStoreWriter;
  }

  public ZkStoreConfigAccessor getStoreConfigAccessor() {
    return storeConfigAccessor;
  }

  public ClusterLockManager getClusterLockManager() {
    return clusterLockManager;
  }

  public VeniceAdminStats getVeniceAdminStats() {
    return veniceAdminStats;
  }

  public StoragePersonaRepository getStoragePersonaRepository() {
    return storagePersonaRepository;
  }

  /**
   * Lock the resource for shutdown operation(leadership handle over and controller shutdown). Once
   * acquired the lock, no other thread could operate for this cluster.
   */
  public AutoCloseableLock lockForShutdown() {
    LOGGER.info(
        "lockForShutdown() called. Will log the current stacktrace and then attempt to acquire the lock.",
        new VeniceException("Not thrown, for logging purposes only."));
    return clusterLockManager.createClusterWriteLock();
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

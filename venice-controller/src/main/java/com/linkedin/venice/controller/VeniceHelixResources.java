package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadWriteStoreRepositoryAdapter;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.AggPushHealthStats;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.pushmonitor.AggPushStatusCleanUpStats;
import com.linkedin.venice.pushmonitor.LeakedPushStatusCleanUpService;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;


/**
 * Aggregate all of essentials resources which is required by controller in one place.
 * <p>
 * All resources in this class is dedicated for one Venice cluster.
 */
public class VeniceHelixResources implements VeniceResource {
  private static final Logger LOGGER = Logger.getLogger(VeniceHelixResources.class);

  private final String clusterName;
  private final SafeHelixManager controller;
  private final ClusterLockManager clusterLockManager;
  private final ReadWriteStoreRepository metadataRepository;
  private final HelixExternalViewRepository routingDataRepository;
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

  private ErrorPartitionResetTask errorPartitionResetTask = null;
  private final Optional<MetaStoreWriter> metaStoreWriter;

  public VeniceHelixResources(String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      SafeHelixManager helixManager,
      VeniceControllerConfig config,
      VeniceHelixAdmin admin,
      MetricsRepository metricsRepository,
      Optional<TopicReplicator> onlineOfflineTopicReplicator,
      Optional<TopicReplicator> leaderFollowerTopicReplicator,
      Optional<DynamicAccessController> accessController,
      MetadataStoreWriter metadataStoreWriter,
      HelixAdminClient helixAdminClient) {
    this.clusterName = clusterName;
    this.config = config;
    this.controller = helixManager;
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
    HelixReadWriteStoreRepository readWriteStoreRepository = new HelixReadWriteStoreRepository(zkClient, adapterSerializer,
        clusterName, metaStoreWriter, clusterLockManager);
    this.metadataRepository = new HelixReadWriteStoreRepositoryAdapter(
        admin.getReadOnlyZKSharedSystemStoreRepository(),
        readWriteStoreRepository
    );
    this.schemaRepository = new HelixReadWriteSchemaRepositoryAdapter(
        admin.getReadOnlyZKSharedSchemaRepository(),
        new HelixReadWriteSchemaRepository(readWriteStoreRepository, zkClient, adapterSerializer, clusterName, metaStoreWriter)
    );

    SafeHelixManager spectatorManager;
    if (controller.getInstanceType() == InstanceType.SPECTATOR) {
      // HAAS is enabled for storage clusters therefore the helix manager is connected as a spectator and it can be
      // used directly for external view purposes.
      spectatorManager = controller;
    } else {
      // Use a separate helix manger for listening on the external view to prevent it from blocking state transition and messages.
      spectatorManager = getSpectatorManager(clusterName, zkClient.getServers());
    }
    this.routingDataRepository = new HelixExternalViewRepository(spectatorManager);
    this.messageChannel = new HelixStatusMessageChannel(helixManager,
        new HelixMessageChannelStats(metricsRepository, clusterName), config.getHelixSendMessageTimeoutMs());
    VeniceOfflinePushMonitorAccessor offlinePushMonitorAccessor = new VeniceOfflinePushMonitorAccessor(clusterName, zkClient,
        adapterSerializer, config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    String aggregateRealTimeSourceKafkaUrl = config.getChildDataCenterKafkaUrlMap().get(config.getAggregateRealTimeSourceRegion());
    this.pushMonitor = new PushMonitorDelegator(config.getPushMonitorType(), clusterName, routingDataRepository,
        offlinePushMonitorAccessor, admin, metadataRepository, new AggPushHealthStats(clusterName, metricsRepository),
        config.isSkipBufferRelayForHybrid(), onlineOfflineTopicReplicator, leaderFollowerTopicReplicator,
        metadataStoreWriter, clusterLockManager, aggregateRealTimeSourceKafkaUrl);
    this.leakedPushStatusCleanUpService = new LeakedPushStatusCleanUpService(clusterName, offlinePushMonitorAccessor, metadataRepository,
        new AggPushStatusCleanUpStats(clusterName, metricsRepository), this.config.getLeakedPushStatusCleanUpServiceSleepIntervalInMs());
    // On controller side, router cluster manager is used as an accessor without maintaining any cache, so do not need to refresh once zk reconnected.
    this.routersClusterManager =
        new ZkRoutersClusterManager(zkClient, adapterSerializer, clusterName, config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs());
    this.aggPartitionHealthStats =
        new AggPartitionHealthStats(clusterName, metricsRepository, routingDataRepository, metadataRepository, pushMonitor);
    this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer, metaStoreWriter);
    this.accessController = accessController;
    if (config.getErrorPartitionAutoResetLimit() > 0) {
      errorPartitionResetTask = new ErrorPartitionResetTask(clusterName, helixAdminClient, metadataRepository,
          routingDataRepository, pushMonitor, metricsRepository, config.getErrorPartitionAutoResetLimit(),
          config.getErrorPartitionProcessingCycleDelay());
    }
  }

  @Override
  public void refresh() {
    clear();
    //make sure that metadataRepo is initialized first since schemaRepo and
    //pushMonitor depends on it
    metadataRepository.refresh();
    /**
     * Initialize the dynamic access client and also register the acl creation/deletion listener.
     */
    if (accessController.isPresent()) {
      DynamicAccessController accessClient = accessController.get();
      accessClient.init(metadataRepository.getAllStores().stream().map(Store::getName).collect(Collectors.toList()));
      metadataRepository.registerStoreDataChangedListener(new AclCreationDeletionListener(accessClient));
    }
    schemaRepository.refresh();
    routingDataRepository.refresh();
    pushMonitor.loadAllPushes();
    routersClusterManager.refresh();
  }

  @Override
  public void clear() {
    /**
     * Also stop monitoring all the pushes; otherwise, the standby controller host will still listen to
     * push status changes and act on the changes which should have been done by leader controller only,
     * like broadcasting StartOfBufferReplay/TopicSwitch messages.
     */
    pushMonitor.stopAllMonitoring();
    metadataRepository.clear();
    schemaRepository.clear();
    routingDataRepository.clear();
    routersClusterManager.clear();
  }

  public void startErrorPartitionResetTask() {
    if (errorPartitionResetTask != null) {
      errorPartitionResetExecutorService.submit(errorPartitionResetTask);
    }
  }

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

  public void startLeakedPushStatusCleanUpService() {
    if (leakedPushStatusCleanUpService != null) {
      leakedPushStatusCleanUpService.start();
    }
  }

  public void stopLeakedPushStatusCleanUpService() {
    if (leakedPushStatusCleanUpService != null) {
      try {
        leakedPushStatusCleanUpService.stop();
      } catch (Exception e) {
        LOGGER.error("Error when stopping leaked push status clean-up service for cluster " + clusterName);
      }
    }
  }

  public ReadWriteStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  public ReadWriteSchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  public HelixExternalViewRepository getRoutingDataRepository() {
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

  public PushMonitorDelegator getPushMonitor() {
    return pushMonitor;
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public AggPartitionHealthStats getAggPartitionHealthStats() {
    return aggPartitionHealthStats;
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

  /**
   * Lock the resource for shutdown operation(mastership handle over and controller shutdown). Once
   * acquired the lock, no other thread could operate for this cluster.
   */
  public AutoCloseableLock lockForShutdown() {
    LOGGER.info("lockForShutdown() called. Will log the current stacktrace and then attempt to acquire the lock.",
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

package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.pushmonitor.AggPushHealthStats;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.stats.AggStoreStats;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.utils.VeniceLock;
import com.linkedin.venice.utils.concurrent.VeniceReentrantReadWriteLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import java.util.stream.Collectors;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Aggregate all of essentials resources which is required by controller in one place.
 * <p>
 * All resources in this class is dedicated for one Venice cluster.
 */
public class VeniceHelixResources implements VeniceResource {
  private static final Logger LOGGER = Logger.getLogger(VeniceHelixResources.class);

  private final SafeHelixManager controller;
  private final HelixReadWriteStoreRepository metadataRepository;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixReadWriteSchemaRepository schemaRepository;
  private final HelixStatusMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;
  private final PushMonitorDelegator pushMonitor;
  private final ZkRoutersClusterManager routersClusterManager;
  private final AggPartitionHealthStats aggPartitionHealthStats;
  private final AggStoreStats aggStoreStats;
  private final ZkStoreConfigAccessor storeConfigAccessor;
  private final VeniceLock veniceHelixResourceReadLock;
  private final VeniceLock veniceHelixResourceWriteLock;
  private final Optional<DynamicAccessController> accessController;

  public VeniceHelixResources(String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      SafeHelixManager helixManager,
      VeniceControllerClusterConfig config,
      StoreCleaner storeCleaner,
      MetricsRepository metricsRepository,
      Optional<TopicReplicator> onlineOfflineTopicReplicator,
      Optional<TopicReplicator> leaderFollowerTopicReplicator,
      Optional<DynamicAccessController> accessController) {
    this(clusterName, zkClient, adapterSerializer, helixManager, config, storeCleaner, metricsRepository, new VeniceReentrantReadWriteLock(),
        onlineOfflineTopicReplicator, leaderFollowerTopicReplicator, accessController);
  }

  /**
   * Package-private on purpose. Used by tests only, to inject a different lock implementation.
   */
  protected VeniceHelixResources(String clusterName,
                              ZkClient zkClient,
                              HelixAdapterSerializer adapterSerializer,
                              SafeHelixManager helixManager,
                              VeniceControllerClusterConfig config,
                              StoreCleaner storeCleaner,
                              MetricsRepository metricsRepository,
                              VeniceReentrantReadWriteLock shutdownLock,
                              Optional<TopicReplicator> onlineOfflineTopicReplicator,
                              Optional<TopicReplicator> leaderFollowerTopicReplicator,
                              Optional<DynamicAccessController> accessController) {
    this.config = config;
    this.controller = helixManager;
    this.metadataRepository = new HelixReadWriteStoreRepository(zkClient, adapterSerializer, clusterName,
        config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.schemaRepository =
        new HelixReadWriteSchemaRepository(this.metadataRepository, zkClient, adapterSerializer, clusterName);
    SafeHelixManager spectatorManager;
    if (controller.getInstanceType() == InstanceType.SPECTATOR) {
      // HAAS is enabled for storage clusters therefore the helix manager is connected as a spectator and it can be
      // used directly for external view purposes.
      spectatorManager = controller;
    } else {
      // Use a separate helix manger for listening on the external view to prevent it from blocking state transition and messages.
      spectatorManager = getSpectatorManager(clusterName, zkClient.getServers());
    }
    this.routingDataRepository = new HelixRoutingDataRepository(spectatorManager);
    this.messageChannel = new HelixStatusMessageChannel(helixManager,
        new HelixMessageChannelStats(metricsRepository, clusterName), config.getHelixSendMessageTimeoutMs());
    this.pushMonitor = new PushMonitorDelegator(config.getPushMonitorType(), clusterName, routingDataRepository,
        new HelixOfflinePushMonitorAccessor(clusterName, zkClient, adapterSerializer,
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs()), storeCleaner,
        metadataRepository, new AggPushHealthStats(clusterName, metricsRepository), config.isSkipBufferRelayForHybrid(),
        onlineOfflineTopicReplicator, leaderFollowerTopicReplicator, metricsRepository);
    // On controller side, router cluster manager is used as an accessor without maintaining any cache, so do not need to refresh once zk reconnected.
    this.routersClusterManager =
        new ZkRoutersClusterManager(zkClient, adapterSerializer, clusterName, config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs());
    this.aggPartitionHealthStats =
        new AggPartitionHealthStats(clusterName, metricsRepository, routingDataRepository, metadataRepository,
            config.getReplicaFactor(), pushMonitor);
    this.aggStoreStats = new AggStoreStats(metricsRepository, metadataRepository);
    this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer);
    String readLockDescription = this.getClass().getSimpleName() + "-" + clusterName + "-readLock";
    this.veniceHelixResourceReadLock = new VeniceLock(shutdownLock.readLock(), readLockDescription, metricsRepository);
    String writeLockDescription = this.getClass().getSimpleName() + "-" + clusterName + "-writeLock";
    this.veniceHelixResourceWriteLock = new VeniceLock(shutdownLock.writeLock(), writeLockDescription, metricsRepository);
    this.accessController = accessController;
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

  public PushMonitorDelegator getPushMonitor() {
    return pushMonitor;
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public AggPartitionHealthStats getAggPartitionHealthStats() {
    return aggPartitionHealthStats;
  }

  /**
   * Lock the resource for metadata operation. Different operations could be executed in parallel.
   */
  public void lockForMetadataOperation() {
    veniceHelixResourceReadLock.lock();
  }

  public void unlockForMetadataOperation(){
    veniceHelixResourceReadLock.unlock();
  }

  /**
   * Lock the resource for shutdown operation(mastership handle over and controller shutdown). Once
   * acquired the lock, no metadata operation or shutdown operation could be executed.
   */
  public void lockForShutdown() {
    LOGGER.info("lockForShutdown() called. Will log the current stacktrace and then attempt to acquire the lock.",
        new VeniceException("Not thrown, for logging purposes only."));
    veniceHelixResourceWriteLock.lock();
  }

  public void unlockForShutdown() {
    veniceHelixResourceWriteLock.unlock();
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

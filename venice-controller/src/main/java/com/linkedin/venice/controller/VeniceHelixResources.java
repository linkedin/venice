package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.controller.stats.AggPartitionHealthStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.pushmonitor.AggPushHealthStats;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceReentrantReadWriteLock;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
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
  private static final int TRY_LOCK_WAIT_TIME_IN_SEC = 10;

  private final SafeHelixManager controller;
  private final HelixReadWriteStoreRepository metadataRepository;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixReadWriteSchemaRepository schemaRepository;
  private final HelixStatusMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;
  private final PushMonitor pushMonitor;
  private final ZkRoutersClusterManager routersClusterManager;
  private final AggPartitionHealthStats aggPartitionHealthStats;
  private final ZkStoreConfigAccessor storeConfigAccessor;
  private final VeniceReentrantReadWriteLock shutdownLock;
  private final LockStats lockStats;

  public VeniceHelixResources(String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      SafeHelixManager helixManager,
      VeniceControllerClusterConfig config,
      StoreCleaner storeCleaner,
      MetricsRepository metricsRepository) {
    this(clusterName, zkClient, adapterSerializer, helixManager, config, storeCleaner, metricsRepository, new VeniceReentrantReadWriteLock());
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
                              VeniceReentrantReadWriteLock shutdownLock) {
    this.config = config;
    this.controller = helixManager;
    this.metadataRepository = new HelixReadWriteStoreRepository(zkClient, adapterSerializer, clusterName,
        config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.schemaRepository =
        new HelixReadWriteSchemaRepository(this.metadataRepository, zkClient, adapterSerializer, clusterName);
    // Use the separate helix manger for listening on the external view to prevent it from blocking state transition and messages.
    SafeHelixManager spectatorManager = getSpectatorManager(clusterName, zkClient.getServers());
    this.routingDataRepository = new HelixRoutingDataRepository(spectatorManager);
    this.messageChannel = new HelixStatusMessageChannel(helixManager,
        new HelixMessageChannelStats(metricsRepository, clusterName), config.getHelixSendMessageTimeoutMs());
    this.pushMonitor = new PushMonitorDelegator(config.getPushMonitorType(), clusterName, routingDataRepository,
        new HelixOfflinePushMonitorAccessor(clusterName, zkClient, adapterSerializer,
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs()), storeCleaner,
        metadataRepository, new AggPushHealthStats(clusterName, metricsRepository), config.isSkipBufferRelayForHybrid());
    // On controller side, router cluster manager is used as an accessor without maintaining any cache, so do not need to refresh once zk reconnected.
    routersClusterManager =
        new ZkRoutersClusterManager(zkClient, adapterSerializer, clusterName, config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs());
    aggPartitionHealthStats =
        new AggPartitionHealthStats(clusterName, metricsRepository, routingDataRepository, metadataRepository,
            config.getReplicaFactor());
    this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer);
    this.shutdownLock = shutdownLock;
    this.lockStats = new LockStats(metricsRepository, clusterName);
  }

  @Override
  public void refresh() {
    clear();
    metadataRepository.refresh();
    schemaRepository.refresh();
    routingDataRepository.refresh();
    pushMonitor.loadAllPushes();
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

  public PushMonitor getPushMonitor() {
    return pushMonitor;
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public AggPartitionHealthStats getAggPartitionHealthStats() {
    return aggPartitionHealthStats;
  }

  private void tryLockWithLogging(Lock lock) {
    for (int attempt = 1; true; attempt++) {
      try {
        boolean lockAcquired = lock.tryLock(TRY_LOCK_WAIT_TIME_IN_SEC, TimeUnit.SECONDS);
        if (lockAcquired) {
          // Success!
          return;
        }

        if (attempt == 1){
          /**
           * We long only once when we have a failed locking attempt, though we will keep trying forever.
           *
           * We call {@link Object#toString()} in order to get the identity of this particular instance of
           * {@link VeniceHelixResources}. This is because there is some funky stuff where VeniceHelixResources
           * contained in {@link VeniceDistClusterControllerStateModel} can be set to null and re-instantiated.
           * Moreover, there are many such instances (one per cluster this controller is the leader of). For all
           * these reasons, it is useful to distinguish which particular instance is involved at any given time.
           */
          LOGGER.warn(super.toString() + " failed to acquire the '" + lock.getClass().getSimpleName()
              + "' within '" + TRY_LOCK_WAIT_TIME_IN_SEC + "' seconds. "
              + "Attempt #" + attempt + ". Will keep trying. "
              + "More lock state information: " + shutdownLock.toString());
        }

        // Record every failed attempt as a metric
        if (lock instanceof ReentrantReadWriteLock.ReadLock) {
          lockStats.failedReadLockAcquisition.record();
        } else if (lock instanceof ReentrantReadWriteLock.WriteLock) {
          lockStats.failedWriteLockAcquisition.record();
        } else {
          LOGGER.debug("UNEXPECTED: The lock is neither a read nor a write lock. Class: " + lock.getClass().getCanonicalName());
        }
      } catch (InterruptedException e) {
        String errorSummary = "Interrupted while trying to acquire the " + lock.getClass().getSimpleName();
        LOGGER.info(errorSummary + ". Will attempt to unlock, just in case. "
            + "This should print a IllegalMonitorStateException since we did not actually acquire the lock.", e);
        unlockSafely(lock);
        throw new VeniceException(errorSummary, e);
      }
    }
  }

  private void unlockSafely(Lock lock) {
    try {
      lock.unlock();
    } catch (IllegalMonitorStateException e) {
      LOGGER.info("Attempted to unlock the '" + lock.getClass().getSimpleName()
          + "' while not owning the thread. Moving on.", e);
    }
  }

  /**
   * Lock the resource for metadata operation. Different operations could be executed in parallel.
   */
  public void lockForMetadataOperation() {
    try {
      tryLockWithLogging(shutdownLock.readLock());
      lockStats.successfulReadLockAcquisition.record();
    } catch (VeniceException e) {
      lockStats.failedReadLockAcquisition.record();
      throw e;
    }
  }

  public void unlockForMetadataOperation(){
    unlockSafely(shutdownLock.readLock());
  }

  /**
   * Lock the resource for shutdown operation(mastership handle over and controller shutdown). Once
   * acquired the lock, no metadata operation or shutdown operation could be executed.
   */
  public void lockForShutdown() {
    LOGGER.info("lockForShutdown() called. Will log the current stacktrace and then attempt to acquire the lock.",
        new VeniceException("Not thrown, for logging purposes only."));
    try {
      tryLockWithLogging(shutdownLock.writeLock());
      lockStats.successfulWriteLockAcquisition.record();
    } catch (VeniceException e) {
      lockStats.failedWriteLockAcquisition.record();
      throw e;
    }
  }

  public void unlockForShutdown() {
    unlockSafely(shutdownLock.writeLock());
    if (shutdownLock.isWriteLockedByCurrentThread()) {
      LOGGER.fatal("We still hold the write lock, even though we unlocked. "
          + "This will probably cause deadlocks! More lock state information: " + shutdownLock.toString());
      lockStats.failedWriteLockRelease.record();
    } else {
      lockStats.successfulWriteLockRelease.record();
    }
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

  private static class LockStats extends AbstractVeniceStats {
    Supplier<MeasurableStat[]> stats = () -> new MeasurableStat[]{new Count()};
    public final Sensor failedWriteLockAcquisition = registerSensorWithAggregate("failed_write_lock_acquisition", stats);
    public final Sensor failedReadLockAcquisition = registerSensorWithAggregate("failed_read_lock_acquisition", stats);
    public final Sensor failedWriteLockRelease = registerSensorWithAggregate("failed_write_lock_release", stats);
    public final Sensor successfulWriteLockAcquisition = registerSensorWithAggregate("successful_write_lock_acquisition", stats);
    public final Sensor successfulReadLockAcquisition = registerSensorWithAggregate("successful_read_lock_acquisition", stats);
    public final Sensor successfulWriteLockRelease = registerSensorWithAggregate("successful_write_lock_release", stats);

    // N.B.: The failure or success of read lock release does not seem to be detectable, so these two combinations are absent.

    public LockStats(MetricsRepository metricsRepository, String clusterName) {
      super(metricsRepository, clusterName);
    }
  }
}

package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


public class LeaderFollowerPartitionStateModelDualPoolFactory extends LeaderFollowerPartitionStateModelFactory {
  private final ExecutorService futureVersionExecutorService;
  private final ParticipantStateTransitionStats futureVersionStateTransitionStats;

  public LeaderFollowerPartitionStateModelDualPoolFactory(
      IngestionBackend ingestionBackend,
      VeniceConfigLoader configService,
      ExecutorService executorService,
      ParticipantStateTransitionStats stateTransitionStats,
      ExecutorService futureVersionExecutorService,
      ParticipantStateTransitionStats futureVersionStateTransitionStats,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    super(
        ingestionBackend,
        configService,
        executorService,
        stateTransitionStats,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        instanceName,
        heartbeatMonitoringService);
    this.futureVersionExecutorService = futureVersionExecutorService;
    this.futureVersionStateTransitionStats = futureVersionStateTransitionStats;
  }

  @Override
  public ExecutorService getExecutorService(String resourceName) {
    /*
     * Allocate different thread pools for future and non-future version Helix state transitions to avoid an issue
     * that future version push is blocked when the long-running state transitions for current versions occupy all
     * threads in the thread pool.
     *
     * If it is a future version that is not ready to serve yet, it will use a separate thread pool.
     * If it is a future version that is ready to serve (PUSHED, ONLINE), it will use the same thread pool as current versions
     * pool to not block future pushes as it can be a long-running transition
     */
    if (Utils.isFutureVersion(resourceName, storeMetadataRepo)
        && !Utils.isFutureVersionReady(resourceName, storeMetadataRepo)) {
      return futureVersionExecutorService;
    }

    return executorService;
  }

  @Override
  public ParticipantStateTransitionStats getStateTransitionStats(String resourceName) {
    return Utils.isFutureVersion(resourceName, storeMetadataRepo)
        ? futureVersionStateTransitionStats
        : stateTransitionStats;
  }

  @Override
  public void shutDownExecutor() {
    executorService.shutdownNow();
    futureVersionExecutorService.shutdownNow();
  }

  @Override
  public void waitExecutorTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    executorService.awaitTermination(timeout, unit);
    long elapsedTime = System.currentTimeMillis() - startTime;
    long remainingTime = unit.toMillis(timeout) - elapsedTime;
    futureVersionExecutorService.awaitTermination(remainingTime, TimeUnit.MILLISECONDS);
  }
}

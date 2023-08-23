package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
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
      VeniceIngestionBackend ingestionBackend,
      VeniceConfigLoader configService,
      ExecutorService executorService,
      ParticipantStateTransitionStats stateTransitionStats,
      ExecutorService futureVersionExecutorService,
      ParticipantStateTransitionStats futureVersionStateTransitionStats,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName) {
    super(
        ingestionBackend,
        configService,
        executorService,
        stateTransitionStats,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        instanceName);
    this.futureVersionExecutorService = futureVersionExecutorService;
    this.futureVersionStateTransitionStats = futureVersionStateTransitionStats;
  }

  @Override
  public ExecutorService getExecutorService(String resourceName) {
    /*
     * Allocate different thread pools for future and non-future version Helix state transitions to avoid an issue
     * that future version push is blocked when the long-running state transitions for current versions occupy all
     * threads in the thread pool.
     */
    return Utils.isFutureVersion(resourceName, storeMetadataRepo) ? futureVersionExecutorService : executorService;
  }

  @Override
  public ParticipantStateTransitionStats getStateTransitionStats(String resourceName) {
    return Utils.isFutureVersion(resourceName, storeMetadataRepo)
        ? futureVersionStateTransitionStats
        : stateTransitionStats;
  }

  public ExecutorService getFutureVersionExecutorService() {
    return futureVersionExecutorService;
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

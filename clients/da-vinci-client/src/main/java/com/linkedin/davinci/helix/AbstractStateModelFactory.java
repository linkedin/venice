package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ModelFactory manages Helix state model creation. {@link #createNewStateModel(String, String)}
 * is called every time a new resource is created. Since Helix internal maintains a map between
 * model definition and model factory, each factory could only create 1 type of model.
 */
public abstract class AbstractStateModelFactory extends StateModelFactory<StateModel> {
  protected final Logger logger = LogManager.getLogger(getClass());
  private final IngestionBackend ingestionBackend;
  private final VeniceConfigLoader configService;
  protected final ReadOnlyStoreRepository storeMetadataRepo;

  // a dedicated thread pool for state transition execution that all state model created by the
  // same factory would share. If it's null, Helix would use a shared thread pool.
  protected final ExecutorService executorService;
  protected final ParticipantStateTransitionStats stateTransitionStats;

  protected CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;
  protected final String instanceName;

  public AbstractStateModelFactory(
      IngestionBackend ingestionBackend,
      VeniceConfigLoader configService,
      ExecutorService executorService,
      ParticipantStateTransitionStats stateTransitionStats,
      ReadOnlyStoreRepository storeMetadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName) {
    this.ingestionBackend = ingestionBackend;
    this.configService = configService;
    this.executorService = executorService;
    this.stateTransitionStats = stateTransitionStats;
    this.storeMetadataRepo = storeMetadataRepo;
    this.partitionPushStatusAccessorFuture = partitionPushStatusAccessorFuture;
    this.instanceName = instanceName;
  }

  @Override
  public ExecutorService getExecutorService(String resourceName) {
    return executorService;
  }

  /**
   * Use the right state transition stats for the resource. By default, use the regular one; when
   * dual state transition thread pool is enabled, use the future version stats for future version resource.
   */
  public ParticipantStateTransitionStats getStateTransitionStats(String resourceName) {
    return stateTransitionStats;
  }

  /**
   * This method will be invoked only once per partition per session
   * @param resourceName $(topic_name) where state transition is happening
   * @param partitionName $(topic_name)_$(partition) for which the State Transition Handler is required
   * @return VenicePartitionStateModel for the partition.
   */
  @Override
  public abstract StateModel createNewStateModel(String resourceName, String partitionName);

  public VeniceConfigLoader getConfigService() {
    return configService;
  }

  public ReadOnlyStoreRepository getStoreMetadataRepo() {
    return storeMetadataRepo;
  }

  public IngestionBackend getIngestionBackend() {
    return ingestionBackend;
  }

  public static String getStateModelID(String resourceName, int partitionId) {
    return resourceName + "_" + partitionId;
  }

  public void shutDownExecutor() {
    executorService.shutdownNow();
  }

  public void waitExecutorTermination(long timeout, TimeUnit unit) throws InterruptedException {
    executorService.awaitTermination(timeout, unit);
  }
}

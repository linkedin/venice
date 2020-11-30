package com.linkedin.davinci.helix;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.davinci.VeniceConfigLoader;
import com.linkedin.davinci.storage.StorageService;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * ModelFactory manages Helix state model creation. {@link #createNewStateModel(String, String)}
 * is called every time a new resource is created. Since Helix internal maintains a map between
 * model definition and model factory, each factory could only create 1 type of model.
 */
public abstract class AbstractParticipantModelFactory extends StateModelFactory<StateModel> {
  protected final Logger logger = Logger.getLogger(getClass().getSimpleName());
  private final StoreIngestionService storeIngestionService;
  private final StorageService storageService;
  private final VeniceConfigLoader configService;
  private final ReadOnlyStoreRepository metadataRepo;

  //a dedicated thread pool for state transition execution that all state model created by the
  //same factory would share. If it's null, Helix would use a shared thread pool.
  private final ExecutorService executorService;
  protected Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture;
  protected final String instanceName;

  public AbstractParticipantModelFactory(StoreIngestionService storeIngestionService, StorageService storageService,
      VeniceConfigLoader configService, ExecutorService executorService, ReadOnlyStoreRepository metadataRepo,
      Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture, String instanceName) {

    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
    this.configService = configService;
    this.executorService = executorService;
    this.metadataRepo = metadataRepo;
    this.partitionPushStatusAccessorFuture = partitionPushStatusAccessorFuture;
    this.instanceName = instanceName;
  }

  @Override
  public ExecutorService getExecutorService(String resourceName) {
    return executorService;
  }

  /**
   * This method will be invoked only once per partition per session
   * @param resourceName $(topic_name) where state transition is happening
   * @param partitionName $(topic_name)_$(partition) for which the State Transition Handler is required
   * @return VenicePartitionStateModel for the partition.
   */
  @Override
  public abstract StateModel createNewStateModel(String resourceName, String partitionName);

  public StoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public StorageService getStorageService() {
    return storageService;
  }

  public VeniceConfigLoader getConfigService() {
    return configService;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public ReadOnlyStoreRepository getMetadataRepo() {
    return metadataRepo;
  }

  public static String getStateModelIdentification(String resourceName, int partitionId) {
    return resourceName + "_" + partitionId;
  }
}

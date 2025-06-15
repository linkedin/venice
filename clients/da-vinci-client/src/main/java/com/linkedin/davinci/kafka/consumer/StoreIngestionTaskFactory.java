package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.helix.manager.zk.ZKHelixAdmin;


public class StoreIngestionTaskFactory {
  private final Builder builder;

  /**
   * Make constructor as private on purpose to force user build the factory
   * using the builder.
   */
  private StoreIngestionTaskFactory(Builder builder) {
    this.builder = builder;
  }

  public StoreIngestionTask getNewIngestionTask(
      StorageService storageService,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int partitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      Lazy<ZKHelixAdmin> zkHelixAdmin) {
    if (version.isActiveActiveReplicationEnabled()) {
      return new ActiveActiveStoreIngestionTask(
          storageService,
          builder,
          store,
          version,
          kafkaConsumerProperties,
          isCurrentVersion,
          storeConfig,
          partitionId,
          isIsolatedIngestion,
          cacheBackend,
          recordTransformerConfig,
          zkHelixAdmin);
    }
    return new LeaderFollowerStoreIngestionTask(
        storageService,
        builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        partitionId,
        isIsolatedIngestion,
        cacheBackend,
        recordTransformerConfig,
        zkHelixAdmin);
  }

  /**
   * @return a new builder for the {@link StoreIngestionTaskFactory}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for {@link StoreIngestionTaskFactory}; once the build() function is invoked,
   * no modification is allowed on any parameters.
   */
  public static class Builder {
    private volatile boolean built = false;

    private VeniceWriterFactory veniceWriterFactory;

    private HeartbeatMonitoringService heartbeatMonitoringService;
    private VeniceViewWriterFactory veniceViewWriterFactory;
    private StorageMetadataService storageMetadataService;
    private Queue<VeniceNotifier> leaderFollowerNotifiers;
    private ReadOnlySchemaRepository schemaRepo;
    private ReadOnlyStoreRepository metadataRepo;
    private TopicManagerRepository topicManagerRepository;
    private AggVersionedDaVinciRecordTransformerStats daVinciRecordTransformerStats;
    private AggHostLevelIngestionStats ingestionStats;
    private AggVersionedDIVStats versionedDIVStats;
    private AggVersionedIngestionStats versionedStorageIngestionStats;
    private AbstractStoreBufferService storeBufferService;
    private VeniceServerConfig serverConfig;
    private DiskUsage diskUsage;
    private AggKafkaConsumerService aggKafkaConsumerService;
    private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
    private boolean isDaVinciClient;
    private RemoteIngestionRepairService remoteIngestionRepairService;
    private MetaStoreWriter metaStoreWriter;
    private StorageEngineBackedCompressorFactory compressorFactory;
    private PubSubTopicRepository pubSubTopicRepository;
    private Runnable runnableForKillIngestionTasksForNonCurrentVersions;
    private ExecutorService aaWCWorkLoadProcessingThreadPool;
    private ExecutorService aaWCIngestionStorageLookupThreadPool;

    private interface Setter {
      void apply();
    }

    private Builder set(Setter setter) {
      if (!built) {
        setter.apply();
      }
      return this;
    }

    public StoreIngestionTaskFactory build() {
      // flip the build flag to true
      this.built = true;
      return new StoreIngestionTaskFactory(this);
    }

    public VeniceWriterFactory getVeniceWriterFactory() {
      return veniceWriterFactory;
    }

    public HeartbeatMonitoringService getHeartbeatMonitoringService() {
      return heartbeatMonitoringService;
    }

    public VeniceViewWriterFactory getVeniceViewWriterFactory() {
      return veniceViewWriterFactory;
    }

    public Builder setVeniceWriterFactory(VeniceWriterFactory writerFactory) {
      return set(() -> this.veniceWriterFactory = writerFactory);
    }

    public Builder setHeartbeatMonitoringService(HeartbeatMonitoringService heartbeatMonitoringService) {
      return set(() -> this.heartbeatMonitoringService = heartbeatMonitoringService);
    }

    public Builder setVeniceViewWriterFactory(VeniceViewWriterFactory viewWriterFactory) {
      return set(() -> this.veniceViewWriterFactory = viewWriterFactory);
    }

    public Builder setRemoteIngestionRepairService(RemoteIngestionRepairService repairService) {
      return set(() -> this.remoteIngestionRepairService = repairService);
    }

    public RemoteIngestionRepairService getRemoteIngestionRepairService() {
      return remoteIngestionRepairService;
    }

    public Builder setMetaStoreWriter(MetaStoreWriter metaStoreWriter) {
      return set(() -> this.metaStoreWriter = metaStoreWriter);
    }

    public MetaStoreWriter getMetaStoreWriter() {
      return this.metaStoreWriter;
    }

    public StorageMetadataService getStorageMetadataService() {
      return storageMetadataService;
    }

    public Builder setStorageMetadataService(StorageMetadataService storageMetadataService) {
      return set(() -> this.storageMetadataService = storageMetadataService);
    }

    public Queue<VeniceNotifier> getLeaderFollowerNotifiers() {
      return leaderFollowerNotifiers;
    }

    public Builder setLeaderFollowerNotifiersQueue(Queue<VeniceNotifier> leaderFollowerNotifiers) {
      return set(() -> this.leaderFollowerNotifiers = leaderFollowerNotifiers);
    }

    public ReadOnlySchemaRepository getSchemaRepo() {
      return schemaRepo;
    }

    public Builder setSchemaRepository(ReadOnlySchemaRepository schemaRepo) {
      return set(() -> this.schemaRepo = schemaRepo);
    }

    public ReadOnlyStoreRepository getMetadataRepo() {
      return metadataRepo;
    }

    public Builder setMetadataRepository(ReadOnlyStoreRepository metadataRepo) {
      return set(() -> this.metadataRepo = metadataRepo);
    }

    public TopicManagerRepository getTopicManagerRepository() {
      return topicManagerRepository;
    }

    public Builder setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
      return set(() -> this.topicManagerRepository = topicManagerRepository);
    }

    public AggVersionedDaVinciRecordTransformerStats getDaVinciRecordTransformerStats() {
      return daVinciRecordTransformerStats;
    }

    public Builder setDaVinciRecordTransformerStats(
        AggVersionedDaVinciRecordTransformerStats daVinciRecordTransformerStats) {
      return set(() -> this.daVinciRecordTransformerStats = daVinciRecordTransformerStats);
    }

    public AggHostLevelIngestionStats getIngestionStats() {
      return ingestionStats;
    }

    public Builder setHostLevelIngestionStats(AggHostLevelIngestionStats storeIngestionStats) {
      return set(() -> this.ingestionStats = storeIngestionStats);
    }

    public AggVersionedDIVStats getVersionedDIVStats() {
      return versionedDIVStats;
    }

    public Builder setVersionedDIVStats(AggVersionedDIVStats versionedDIVStats) {
      return set(() -> this.versionedDIVStats = versionedDIVStats);
    }

    public AggVersionedIngestionStats getVersionedStorageIngestionStats() {
      return versionedStorageIngestionStats;
    }

    public Builder setVersionedIngestionStats(AggVersionedIngestionStats versionedStorageIngestionStats) {
      return set(() -> this.versionedStorageIngestionStats = versionedStorageIngestionStats);
    }

    public AbstractStoreBufferService getStoreBufferService() {
      return storeBufferService;
    }

    public Builder setStoreBufferService(AbstractStoreBufferService storeBufferService) {
      return set(() -> this.storeBufferService = storeBufferService);
    }

    public VeniceServerConfig getServerConfig() {
      return serverConfig;
    }

    public Builder setServerConfig(VeniceServerConfig serverConfig) {
      return set(() -> this.serverConfig = serverConfig);
    }

    public DiskUsage getDiskUsage() {
      return diskUsage;
    }

    public Builder setDiskUsage(DiskUsage diskUsage) {
      return set(() -> this.diskUsage = diskUsage);
    }

    public AggKafkaConsumerService getAggKafkaConsumerService() {
      return aggKafkaConsumerService;
    }

    public Builder setAggKafkaConsumerService(AggKafkaConsumerService aggKafkaConsumerService) {
      return set(() -> this.aggKafkaConsumerService = aggKafkaConsumerService);
    }

    public InternalAvroSpecificSerializer<PartitionState> getPartitionStateSerializer() {
      return partitionStateSerializer;
    }

    public Builder setPartitionStateSerializer(
        InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
      return set(() -> this.partitionStateSerializer = partitionStateSerializer);
    }

    public boolean isDaVinciClient() {
      return isDaVinciClient;
    }

    public Builder setIsDaVinciClient(boolean isDaVinciClient) {
      return set(() -> this.isDaVinciClient = isDaVinciClient);
    }

    public StorageEngineBackedCompressorFactory getCompressorFactory() {
      return compressorFactory;
    }

    public Builder setCompressorFactory(StorageEngineBackedCompressorFactory compressorFactory) {
      return set(() -> this.compressorFactory = compressorFactory);
    }

    public PubSubTopicRepository getPubSubTopicRepository() {
      return pubSubTopicRepository;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      return set(() -> this.pubSubTopicRepository = pubSubTopicRepository);
    }

    public Runnable getRunnableForKillIngestionTasksForNonCurrentVersions() {
      return runnableForKillIngestionTasksForNonCurrentVersions;
    }

    public Builder setRunnableForKillIngestionTasksForNonCurrentVersions(Runnable runnable) {
      return set(() -> this.runnableForKillIngestionTasksForNonCurrentVersions = runnable);
    }

    public Builder setAAWCWorkLoadProcessingThreadPool(ExecutorService executorService) {
      return set(() -> this.aaWCWorkLoadProcessingThreadPool = executorService);
    }

    public Builder setAAWCIngestionStorageLookupThreadPool(ExecutorService executorService) {
      return set(() -> this.aaWCIngestionStorageLookupThreadPool = executorService);
    }

    public ExecutorService getAaWCIngestionStorageLookupThreadPool() {
      return aaWCIngestionStorageLookupThreadPool;
    }

    public ExecutorService getAAWCWorkLoadProcessingThreadPool() {
      return this.aaWCWorkLoadProcessingThreadPool;
    }
  }
}

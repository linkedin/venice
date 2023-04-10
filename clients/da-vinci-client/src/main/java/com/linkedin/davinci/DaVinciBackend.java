package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_DERIVED_SCHEMA_ID;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static java.lang.Thread.currentThread;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.StoreBackendConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.DaVinciIngestionBackend;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.MetadataUpdateStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DaVinciBackend implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciBackend.class);

  private final ZkClient zkClient;
  private final VeniceConfigLoader configLoader;
  private final SubscriptionBasedReadOnlyStoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final MetricsRepository metricsRepository;
  private final RocksDBMemoryStats rocksDBMemoryStats;
  private final StorageService storageService;
  private final KafkaStoreIngestionService ingestionService;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final Map<String, StoreBackend> storeByNameMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VersionBackend> versionByTopicMap = new VeniceConcurrentHashMap<>();
  private final StorageMetadataService storageMetadataService;
  private final PushStatusStoreWriter pushStatusStoreWriter;
  private final ExecutorService ingestionReportExecutor = Executors.newSingleThreadExecutor();
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final Optional<ObjectCacheBackend> cacheBackend;
  private DaVinciIngestionBackend ingestionBackend;
  private final AggVersionedStorageEngineStats aggVersionedStorageEngineStats;

  public DaVinciBackend(
      ClientConfig clientConfig,
      VeniceConfigLoader configLoader,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Optional<ObjectCacheConfig> cacheConfig) {
    LOGGER.info("Creating Da Vinci backend");
    try {
      VeniceServerConfig backendConfig = configLoader.getVeniceServerConfig();
      this.configLoader = configLoader;
      metricsRepository = Optional.ofNullable(clientConfig.getMetricsRepository())
          .orElse(TehutiUtils.getMetricsRepository("davinci-client"));
      VeniceMetadataRepositoryBuilder veniceMetadataRepositoryBuilder =
          new VeniceMetadataRepositoryBuilder(configLoader, clientConfig, metricsRepository, icProvider, false);
      ClusterInfoProvider clusterInfoProvider = veniceMetadataRepositoryBuilder.getClusterInfoProvider();
      ReadOnlyStoreRepository readOnlyStoreRepository = veniceMetadataRepositoryBuilder.getStoreRepo();
      if (!(readOnlyStoreRepository instanceof SubscriptionBasedReadOnlyStoreRepository)) {
        throw new VeniceException(
            "Da Vinci backend expects " + SubscriptionBasedReadOnlyStoreRepository.class.getName()
                + " for store repository!");
      }
      storeRepository = (SubscriptionBasedReadOnlyStoreRepository) readOnlyStoreRepository;
      schemaRepository = veniceMetadataRepositoryBuilder.getSchemaRepo();
      zkClient = veniceMetadataRepositoryBuilder.getZkClient();

      VeniceProperties backendProps = backendConfig.getClusterProperties();

      SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
          ClientConfig.cloneConfig(clientConfig)
              .setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()),
          null);
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
          AvroProtocolDefinition.PARTITION_STATE.getSerializer();
      partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);

      SchemaReader versionStateSchemaReader = ClientFactory.getSchemaReader(
          ClientConfig.cloneConfig(clientConfig)
              .setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()),
          null);
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
      storeVersionStateSerializer.setSchemaReader(versionStateSchemaReader);

      aggVersionedStorageEngineStats = new AggVersionedStorageEngineStats(
          metricsRepository,
          storeRepository,
          backendConfig.isUnregisterMetricForDeletedStoreEnabled());
      rocksDBMemoryStats = backendConfig.isDatabaseMemoryStatsEnabled()
          ? new RocksDBMemoryStats(
              metricsRepository,
              "RocksDBMemoryStats",
              backendConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled())
          : null;

      // Add extra safeguards here to ensure we have released RocksDB database locks before we initialize storage
      // services.
      IsolatedIngestionUtils.destroyLingeringIsolatedIngestionProcess(configLoader);
      storageService = new StorageService(
          configLoader,
          aggVersionedStorageEngineStats,
          rocksDBMemoryStats,
          storeVersionStateSerializer,
          partitionStateSerializer,
          storeRepository);
      storageService.start();

      VeniceWriterFactory writerFactory = new VeniceWriterFactory(backendProps.toProperties());
      String instanceName = Utils.getHostName() + "_" + Utils.getPid();
      int derivedSchemaID = backendProps.getInt(PUSH_STATUS_STORE_DERIVED_SCHEMA_ID, 1);
      pushStatusStoreWriter = new PushStatusStoreWriter(writerFactory, instanceName, derivedSchemaID);

      SchemaReader kafkaMessageEnvelopeSchemaReader = ClientFactory.getSchemaReader(
          ClientConfig.cloneConfig(clientConfig)
              .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()),
          null);

      /**
       * Verify that the latest {@link com.linkedin.venice.serialization.avro.AvroProtocolDefinition#KAFKA_MESSAGE_ENVELOPE}
       * version in the code base is registered in Venice backend; if not, fail fast in start phase before start writing
       * Kafka messages that Venice backend couldn't deserialize.
       */
      if (configLoader.getCombinedProperties().getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true)) {
        LOGGER.info("Start verifying the latest protocols at runtime are valid in Venice backend.");
        new SchemaPresenceChecker(kafkaMessageEnvelopeSchemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)
            .verifySchemaVersionPresentOrExit();
        LOGGER.info("Successfully verified the latest protocols at runtime are valid in Venice backend.");
      }

      storageMetadataService = backendConfig.getIngestionMode().equals(IngestionMode.ISOLATED)
          ? new MainIngestionStorageMetadataService(
              backendConfig.getIngestionServicePort(),
              partitionStateSerializer,
              new MetadataUpdateStats(metricsRepository),
              configLoader,
              storageService.getStoreVersionStateSyncer())
          : new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);
      // Start storage metadata service
      ((AbstractVeniceService) storageMetadataService).start();
      compressorFactory = new StorageEngineBackedCompressorFactory(storageMetadataService);

      cacheBackend = cacheConfig
          .map(objectCacheConfig -> new ObjectCacheBackend(clientConfig, objectCacheConfig, schemaRepository));
      ingestionService = new KafkaStoreIngestionService(
          storageService.getStorageEngineRepository(),
          configLoader,
          storageMetadataService,
          clusterInfoProvider,
          storeRepository,
          schemaRepository,
          Optional.empty(),
          Optional.empty(),
          null,
          metricsRepository,
          Optional.of(kafkaMessageEnvelopeSchemaReader),
          Optional.empty(),
          partitionStateSerializer,
          Optional.empty(),
          null,
          false,
          compressorFactory,
          cacheBackend,
          true,
          // TODO: consider how/if a repair task would be valid for Davinci users?
          null,
          new PubSubClientsFactory(new ApacheKafkaProducerAdapterFactory()));

      ingestionService.start();
      ingestionService.addIngestionNotifier(ingestionListener);

      if (isIsolatedIngestion() && cacheConfig.isPresent()) {
        // TODO: There are 'some' cases where this mix might be ok, (like a batch only store, or with certain TTL
        // settings),
        // could add further validation. If the process isn't ingesting data, then it can't maintain the object cache
        // with
        // a correct view of the data.
        throw new IllegalArgumentException(
            "Ingestion isolated and Cache are incompatible configs!!  Aborting start up!");
      }

      bootstrap(managedClients);

      storeRepository.registerStoreDataChangedListener(storeChangeListener);
      cacheBackend.ifPresent(
          objectCacheBackend -> storeRepository
              .registerStoreDataChangedListener(objectCacheBackend.getCacheInvalidatingStoreChangeListener()));
      LOGGER.info("Da Vinci backend created successfully");
    } catch (Throwable e) {
      String msg = "Unable to create Da Vinci backend";
      LOGGER.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  protected synchronized void bootstrap(Optional<Set<String>> managedClients) {
    List<AbstractStorageEngine> storageEngines =
        storageService.getStorageEngineRepository().getAllLocalStorageEngines();
    LOGGER.info("Starting bootstrap, storageEngines: {}, managedClients: {}", storageEngines, managedClients);
    Map<String, Set<Integer>> expectedBootstrapVersions = new HashMap<>();
    Map<String, Version> storeNameToBootstrapVersionMap = new HashMap<>();
    Map<String, List<Integer>> storeNameToPartitionListMap = new HashMap<>();
    Set<String> unusedStores = new HashSet<>();
    for (AbstractStorageEngine storageEngine: storageEngines) {
      String kafkaTopicName = storageEngine.getStoreName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);

      try {
        StoreBackend storeBackend = getStoreOrThrow(storeName); // throws VeniceNoStoreException
        if (managedClients.isPresent() && !managedClients.get().contains(storeName) && storeBackend.isManaged()) {
          // If the store is not-managed, all its versions will be removed.
          LOGGER.info("Deleting unused managed version: {}", kafkaTopicName);
          unusedStores.add(storeName);
          storageService.removeStorageEngine(kafkaTopicName);
          continue;
        }
      } catch (VeniceNoStoreException e) {
        // The store does not exist in Venice anymore, so it will be deleted.
        LOGGER.info("Store does not exist, deleting invalid local version: {}", kafkaTopicName);
        unusedStores.add(storeName);
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      // Initialize expected version numbers for each store.
      if (!expectedBootstrapVersions.containsKey(storeName)) {
        Set<Integer> validVersionNumbers = new HashSet<>();
        Optional<Version> latestVersion = getVeniceLatestNonFaultyVersion(storeName, Collections.emptySet());
        Optional<Version> currentVersion = getVeniceCurrentVersion(storeName);
        currentVersion.ifPresent(version -> validVersionNumbers.add(version.getNumber()));
        latestVersion.ifPresent(version -> validVersionNumbers.add(version.getNumber()));
        expectedBootstrapVersions.put(storeName, validVersionNumbers);
      }

      int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopicName);
      // The version is no longer valid (stale version), it will be deleted.
      if (!expectedBootstrapVersions.get(storeName).contains(versionNumber)) {
        LOGGER.info("Deleting obsolete local version: {}", kafkaTopicName);
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      Version version = storeRepository.getStoreOrThrow(storeName)
          .getVersion(versionNumber)
          .orElseThrow(
              () -> new VeniceException(
                  "Could not find version: " + versionNumber + " for store: " + storeName + " in storeRepository!"));

      /**
       * Set the target bootstrap version for the store in the below order:
       * 1. CURRENT_VERSION: store's CURRENT_VERSION exists locally.
       * 2. FUTURE_VERSION: store's CURRENT_VERSION does not exist locally, but FUTURE_VERSION exists locally.
       * In most case, we will choose 1, as the CURRENT_VERSION will always exists locally regardless of the FUTURE_VERSION
       * Case 2 will only exist when store version retention policy is > 2, and rollback happens on Venice side.
       */
      if (!(storeNameToBootstrapVersionMap.containsKey(storeName)
          && (storeNameToBootstrapVersionMap.get(storeName).getNumber() < versionNumber))) {
        storeNameToBootstrapVersionMap.put(storeName, version);
        storeNameToPartitionListMap.put(storeName, storageService.getUserPartitions(kafkaTopicName));
      }
    }
    /**
     * Remove unused store's {@link StoreBackend} to avoid duplicate metrics registration issues when iterating storage
     * engines for different versions of the same store.
     */
    for (String unusedStoreName: unusedStores) {
      deleteStore(unusedStoreName);
    }

    // Cleanup stale StaleBackendConfig
    String baseDataPath = configLoader.getVeniceServerConfig().getDataBasePath();
    for (String storeName: StoreBackendConfig.listConfigs(baseDataPath)) {
      if (!storeByNameMap.containsKey(storeName)) {
        new StoreBackendConfig(baseDataPath, storeName).delete();
      }
    }

    /**
     * In order to make bootstrap logic compatible with ingestion isolation, we first scan all local storage engines,
     * record all store versions that are up-to-date and close all storage engines. This will make sure child process
     * can open RocksDB stores.
     */
    if (isIsolatedIngestion()) {
      if (configLoader.getVeniceServerConfig().freezeIngestionIfReadyToServeOrLocalDataExists()) {
        /**
         * In this case we will only need to close metadata partition, as it is supposed to be opened and managed by
         * forked ingestion process via following subscribe call.
         */
        for (AbstractStorageEngine storageEngine: storageService.getStorageEngineRepository()
            .getAllLocalStorageEngines()) {
          storageEngine.closeMetadataPartition();
        }
      } else {
        storageService.closeAllStorageEngines();
      }
    }

    ingestionBackend = isIsolatedIngestion()
        ? new IsolatedIngestionBackend(
            configLoader,
            storeRepository,
            metricsRepository,
            storageMetadataService,
            ingestionService,
            storageService)
        : new DefaultIngestionBackend(storageMetadataService, ingestionService, storageService);
    ingestionBackend.addIngestionNotifier(ingestionListener);

    // Subscribe all bootstrap version partitions.
    storeNameToBootstrapVersionMap.forEach((storeName, version) -> {
      List<Integer> partitions = storeNameToPartitionListMap.get(storeName);
      String versionTopic = version.kafkaTopicName();
      LOGGER.info("Bootstrapping partitions {} for {}", partitions, versionTopic);
      aggVersionedStorageEngineStats.setStorageEngine(versionTopic, storageService.getStorageEngine(versionTopic));
      StoreBackend storeBackend = getStoreOrThrow(storeName);
      storeBackend.subscribe(ComplementSet.newSet(partitions), Optional.of(version));
    });
  }

  @Override
  public synchronized void close() {
    LOGGER.info("Closing Da Vinci backend");
    storeRepository.unregisterStoreDataChangedListener(storeChangeListener);
    cacheBackend.ifPresent(
        objectCacheBackend -> storeRepository
            .unregisterStoreDataChangedListener(objectCacheBackend.getCacheInvalidatingStoreChangeListener()));
    for (StoreBackend storeBackend: storeByNameMap.values()) {
      storeBackend.close();
    }
    storeByNameMap.clear();
    versionByTopicMap.clear();
    compressorFactory.close();

    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    ingestionReportExecutor.shutdown();
    try {
      if (!ingestionReportExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        ingestionReportExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    try {
      ingestionBackend.close();
      ingestionService.stop();
      storageService.stop();
      if (zkClient != null) {
        zkClient.close();
      }
      metricsRepository.close();
      storeRepository.clear();
      schemaRepository.clear();
      pushStatusStoreWriter.close();
      LOGGER.info("Da Vinci backend is closed successfully");
    } catch (Throwable e) {
      String msg = "Unable to stop Da Vinci backend";
      LOGGER.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public synchronized StoreBackend getStoreOrThrow(String storeName) {
    StoreBackend storeBackend = storeByNameMap.get(storeName);
    if (storeBackend == null) {
      storeBackend = new StoreBackend(this, storeName);
      storeByNameMap.put(storeName, storeBackend);
    }
    return storeBackend;
  }

  ScheduledExecutorService getExecutor() {
    return executor;
  }

  VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public SubscriptionBasedReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public ObjectCacheBackend getObjectCache() {
    return cacheBackend.get();
  }

  public ReadOnlySchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  StorageService getStorageService() {
    return storageService;
  }

  StoreIngestionService getIngestionService() {
    return ingestionService;
  }

  public DaVinciIngestionBackend getIngestionBackend() {
    return ingestionBackend;
  }

  public boolean compareCacheConfig(Optional<ObjectCacheConfig> config) {
    return cacheBackend.map(ObjectCacheBackend::getStoreCacheConfig).equals(config);
  }

  final Map<String, VersionBackend> getVersionByTopicMap() {
    return versionByTopicMap;
  }

  PushStatusStoreWriter getPushStatusStoreWriter() {
    return pushStatusStoreWriter;
  }

  public StorageEngineBackedCompressorFactory getCompressorFactory() {
    return compressorFactory;
  }

  protected void reportPushStatus(String kafkaTopic, int subPartition, ExecutionStatus status) {
    reportPushStatus(kafkaTopic, subPartition, status, Optional.empty());
  }

  protected void reportPushStatus(
      String kafkaTopic,
      int subPartition,
      ExecutionStatus status,
      Optional<String> incrementalPushVersion) {
    VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
    if (versionBackend != null && versionBackend.isReportingPushStatus()) {
      Version version = versionBackend.getVersion();
      pushStatusStoreWriter
          .writePushStatus(version.getStoreName(), version.getNumber(), subPartition, status, incrementalPushVersion);
    }
  }

  protected void deleteStore(String storeName) {
    StoreBackend storeBackend = storeByNameMap.remove(storeName);
    if (storeBackend != null) {
      storeBackend.delete();
    }
  }

  protected final boolean isIsolatedIngestion() {
    return configLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED);
  }

  // Move the logic to this protected method to make it visible for unit test.
  protected void handleStoreChanged(StoreBackend storeBackend) {
    storeBackend.validateDaVinciAndVeniceCurrentVersion();
    storeBackend.tryDeleteInvalidDaVinciFutureVersion();
    /**
     * Future version may not meet the swapping condition when local partitions finished ingestion, thus everytime
     * when store config has been changed in the Venice backend, we need to check if we could swap the future version
     * to current.
     */
    storeBackend.trySwapDaVinciCurrentVersion(null);
    storeBackend.trySubscribeDaVinciFutureVersion();
  }

  Optional<Version> getVeniceLatestNonFaultyVersion(String storeName, Set<Integer> faultyVersions) {
    try {
      return getVeniceLatestNonFaultyVersion(getStoreRepository().getStoreOrThrow(storeName), faultyVersions);
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  Optional<Version> getVeniceCurrentVersion(String storeName) {
    try {
      return getVeniceCurrentVersion(getStoreRepository().getStoreOrThrow(storeName));
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  private Optional<Version> getVeniceLatestNonFaultyVersion(Store store, Set<Integer> faultyVersions) {
    return store.getVersions()
        .stream()
        .filter(v -> !faultyVersions.contains(v.getNumber()))
        .max(Comparator.comparing(Version::getNumber));
  }

  private Optional<Version> getVeniceCurrentVersion(Store store) {
    return store.getVersion(store.getCurrentVersion());
  }

  private final StoreDataChangedListener storeChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreChanged(Store store) {
      StoreBackend storeBackend = storeByNameMap.get(store.getName());
      if (storeBackend != null) {
        DaVinciBackend.this.handleStoreChanged(storeBackend);
      }
    }

    @Override
    public void handleStoreDeleted(Store store) {
      deleteStore(store.getName());
    }
  };

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset, String message) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          versionBackend.completePartition(partitionId);
          versionBackend.tryStopHeartbeat();
          reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.COMPLETED);
        }
      });
    }

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception e) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          versionBackend.completePartitionExceptionally(partitionId, e);
          versionBackend.tryStopHeartbeat();
          reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.ERROR);
        }
      });
    }

    @Override
    public void started(String kafkaTopic, int partitionId, String message) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.STARTED, Optional.empty());
          versionBackend.tryStartHeartbeat();
        }
      });
    }

    @Override
    public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          versionBackend.tryStartHeartbeat();
        }
      });
    }

    @Override
    public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
      ingestionReportExecutor.submit(() -> {
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.END_OF_PUSH_RECEIVED);
      });
    }

    @Override
    public void startOfIncrementalPushReceived(
        String kafkaTopic,
        int partitionId,
        long offset,
        String incrementalPushVersion) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          reportPushStatus(
              kafkaTopic,
              partitionId,
              ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
              Optional.of(incrementalPushVersion));
          versionBackend.tryStartHeartbeat();
        }
      });
    }

    @Override
    public void endOfIncrementalPushReceived(
        String kafkaTopic,
        int partitionId,
        long offset,
        String incrementalPushVersion) {
      ingestionReportExecutor.submit(() -> {
        VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
        if (versionBackend != null) {
          versionBackend.tryStopHeartbeat();
          reportPushStatus(
              kafkaTopic,
              partitionId,
              ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
              Optional.of(incrementalPushVersion));
        }
      });
    }
  };
}

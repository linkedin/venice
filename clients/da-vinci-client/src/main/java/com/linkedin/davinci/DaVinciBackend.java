package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_INSTANCE_NAME_SUFFIX;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static java.lang.Thread.currentThread;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.StoreBackendConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IngestionBackend;
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
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.blobtransfer.BlobTransferUtil;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
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
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DaVinciBackend implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciBackend.class);
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
  private IngestionBackend ingestionBackend;
  private final AggVersionedStorageEngineStats aggVersionedStorageEngineStats;
  private final boolean useDaVinciSpecificExecutionStatusForError;
  private final ClientConfig clientConfig;
  private BlobTransferManager<Void> blobTransferManager;
  private final boolean writeBatchingPushStatus;

  public DaVinciBackend(
      ClientConfig clientConfig,
      VeniceConfigLoader configLoader,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Optional<ObjectCacheConfig> cacheConfig,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) {
    LOGGER.info("Creating Da Vinci backend with managed clients: {}", managedClients);
    try {
      VeniceServerConfig backendConfig = configLoader.getVeniceServerConfig();
      useDaVinciSpecificExecutionStatusForError = backendConfig.useDaVinciSpecificExecutionStatusForError();
      writeBatchingPushStatus = backendConfig.getDaVinciPushStatusCheckIntervalInMs() >= 0;
      this.configLoader = configLoader;
      this.clientConfig = clientConfig;
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
      /**
       * The constructor of {@link #storageService} will take care of unused store/store version cleanup.
       *
       * When Ingestion Isolation is enabled, we don't want to restore data partitions here:
       * 1. It is a waste of effort since all the opened partitioned will be closed right after.
       * 2. When DaVinci memory limiter is enabled, currently, SSTFileManager doesn't clean up the entries belonging
       *    to the closed database, which means when the Isolated process hands the database back to the main process,
       *    some removed SST files (some SST files can be removed in Isolated Process because of log compaction) will
       *    remain in SSTFileManager tracked file list.
       * 3. We still want to open metadata partition, otherwise {@link StorageService} won't scan the local db folder.
       *    Also opening metadata partition in main process won't cause much side effect from DaVinci memory limiter's
       *    POV, since metadata partition won't be handed back to main process in the future.
       * 4. When Ingestion Isolation is enabled with suppressing live update feature, main process needs to open all the
       *    data partitions since Isolated Process won't re-ingest the existing partitions.
       */
      boolean whetherToRestoreDataPartitions = !isIsolatedIngestion()
          || configLoader.getVeniceServerConfig().freezeIngestionIfReadyToServeOrLocalDataExists();
      LOGGER.info("DaVinci {} restore data partitions.", whetherToRestoreDataPartitions ? "will" : "won't");
      storageService = new StorageService(
          configLoader,
          aggVersionedStorageEngineStats,
          rocksDBMemoryStats,
          storeVersionStateSerializer,
          partitionStateSerializer,
          storeRepository,
          whetherToRestoreDataPartitions,
          true,
          functionToCheckWhetherStorageEngineShouldBeKeptOrNot(managedClients));
      storageService.start();
      PubSubClientsFactory pubSubClientsFactory = configLoader.getVeniceServerConfig().getPubSubClientsFactory();
      VeniceWriterFactory writerFactory =
          new VeniceWriterFactory(backendProps.toProperties(), pubSubClientsFactory.getProducerAdapterFactory(), null);
      String pid = Utils.getPid();
      String instanceSuffix =
          configLoader.getCombinedProperties().getString(PUSH_STATUS_INSTANCE_NAME_SUFFIX, (pid == null ? "NA" : pid));
      String instanceName = Utils.getHostName() + "_" + instanceSuffix;

      // Fetch latest update schema's protocol ID for Push Status Store from Router.
      ClientConfig pushStatusStoreClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName());
      try (StoreSchemaFetcher schemaFetcher = ClientFactory.createStoreSchemaFetcher(pushStatusStoreClientConfig)) {
        SchemaEntry valueSchemaEntry = schemaFetcher.getLatestValueSchemaEntry();
        DerivedSchemaEntry updateSchemaEntry = schemaFetcher.getUpdateSchemaEntry(valueSchemaEntry.getId());
        pushStatusStoreWriter =
            new PushStatusStoreWriter(writerFactory, instanceName, valueSchemaEntry, updateSchemaEntry);
      }

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
          getRecordTransformer,
          true,
          // TODO: consider how/if a repair task would be valid for Davinci users?
          null,
          pubSubClientsFactory,
          Optional.empty(),
          // TODO: It would be good to monitor heartbeats like this from davinci, but needs some work
          null);

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

      if (backendConfig.isBlobTransferManagerEnabled()) {
        blobTransferManager = BlobTransferUtil.getP2PBlobTransferManagerAndStart(
            configLoader.getVeniceServerConfig().getDvcP2pBlobTransferServerPort(),
            configLoader.getVeniceServerConfig().getDvcP2pBlobTransferClientPort(),
            configLoader.getVeniceServerConfig().getRocksDBPath(),
            clientConfig);
      } else {
        blobTransferManager = null;
      }

      bootstrap();

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

  private Function<String, Boolean> functionToCheckWhetherStorageEngineShouldBeKeptOrNot(
      Optional<Set<String>> managedClients) {
    return storageEngineName -> {
      String storeName = Version.parseStoreFromKafkaTopicName(storageEngineName);
      if (VeniceSystemStoreType.META_STORE.isSystemStore(storeName)) {
        // Do not bootstrap meta system store via DaVinci backend initialization since the operation is not supported by
        // ThinClientMetaStoreBasedRepository. This shouldn't happen normally, but it's possible if the user was using
        // DVC based metadata for the same store and switched to thin client based metadata.
        return true;
      }
      /**
       * If the corresponding Venice store doesn't even exist, the local storage engine will be removed.
       * If the managed client feature is enabled, but the storage engine is not on the list, the local
       * storage engine will be removed.
       */
      boolean storeShouldBeDeleted = false;
      try {
        StoreBackend storeBackend = getStoreOrThrow(storeName); // throws VeniceNoStoreException
        if (managedClients.isPresent() && !managedClients.get().contains(storeName) && storeBackend.isManaged()) {
          // If the store is not-managed, all its versions will be removed.
          LOGGER.info("Will delete unused managed version: {}", storageEngineName);
          storeShouldBeDeleted = true;
        }
      } catch (VeniceNoStoreException e) {
        // The store does not exist in Venice anymore, so it will be deleted.
        LOGGER.warn("Store does not exist, will delete invalid local version: {}", storageEngineName);
        storeShouldBeDeleted = true;
      }
      if (storeShouldBeDeleted) {
        /**
         * Clean up the local state.
         * Since it is possible there could multiple versions for the same store, the following cleanup
         * logic should work for multiple times of cleanup for the same store.
         */
        deleteStore(storeName);
        String baseDataPath = configLoader.getVeniceServerConfig().getDataBasePath();
        new StoreBackendConfig(baseDataPath, storeName).delete();
        return false;
      }

      // Check whether the local storage engine belongs to any valid store version or not
      Set<Integer> validVersionNumbers = new HashSet<>();
      Version currentVersion = getVeniceCurrentVersion(storeName);
      if (currentVersion != null) {
        validVersionNumbers.add(currentVersion.getNumber());
      }
      Version latestVersion = getVeniceLatestNonFaultyVersion(storeName, Collections.emptySet());
      if (latestVersion != null) {
        validVersionNumbers.add(latestVersion.getNumber());
      }

      int versionNumber = Version.parseVersionFromKafkaTopicName(storageEngineName);
      // The version is no longer valid (stale version), it will be deleted.
      if (!validVersionNumbers.contains(versionNumber)) {
        LOGGER.info("Will delete obsolete local version: {}", storageEngineName);
        return false;
      }
      return true;
    };
  }

  private synchronized void bootstrap() {
    List<AbstractStorageEngine> storageEngines =
        storageService.getStorageEngineRepository().getAllLocalStorageEngines();
    LOGGER.info("Starting bootstrap, storageEngines: {}", storageEngines);
    Map<String, Version> storeNameToBootstrapVersionMap = new HashMap<>();
    Map<String, List<Integer>> storeNameToPartitionListMap = new HashMap<>();

    for (AbstractStorageEngine storageEngine: storageEngines) {
      String kafkaTopicName = storageEngine.getStoreVersionName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);
      if (VeniceSystemStoreType.META_STORE.isSystemStore(storeName)) {
        // Do not bootstrap meta system store via DaVinci backend initialization since the operation is not supported by
        // ThinClientMetaStoreBasedRepository. This shouldn't happen normally, but it's possible if the user was using
        // DVC based metadata for the same store and switched to thin client based metadata.
        continue;
      }

      try {
        getStoreOrThrow(storeName); // throws VeniceNoStoreException
      } catch (VeniceNoStoreException e) {
        throw new VeniceException("Unexpected to encounter non-existing store here: " + storeName);
      }

      int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopicName);

      Version version = storeRepository.getStoreOrThrow(storeName).getVersion(versionNumber);
      if (version == null) {
        throw new VeniceException(
            "Could not find version: " + versionNumber + " for store: " + storeName + " in storeRepository!");
      }

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
            metricsRepository,
            storageMetadataService,
            ingestionService,
            storageService,
            blobTransferManager)
        : new DefaultIngestionBackend(
            storageMetadataService,
            ingestionService,
            storageService,
            blobTransferManager,
            configLoader.getVeniceServerConfig());
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
    ExecutorService storeBackendCloseExecutor =
        Executors.newCachedThreadPool(new DaemonThreadFactory("DaVinciBackend-StoreBackend-Close"));
    for (StoreBackend storeBackend: storeByNameMap.values()) {
      /**
       * {@link StoreBackend#close()} is time-consuming since the internal {@link VersionBackend#close()} call triggers
       * {@link KafkaStoreIngestionService#shutdownStoreIngestionTask}, which can take up to 10s to return.
       * So here we use a thread pool to shut down all the subscribed stores concurrently.
       */
      storeBackendCloseExecutor.submit(storeBackend::close);
    }
    storeBackendCloseExecutor.shutdown();
    try {
      storeBackendCloseExecutor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      currentThread().interrupt();
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
      metricsRepository.close();
      storeRepository.clear();
      schemaRepository.clear();
      pushStatusStoreWriter.close();

      if (blobTransferManager != null) {
        blobTransferManager.close();
      }

      LOGGER.info("Da Vinci backend is closed successfully");
    } catch (Throwable e) {
      String msg = "Unable to stop Da Vinci backend";
      LOGGER.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public StoreBackend getStoreOrThrow(String storeName) {
    return storeByNameMap.computeIfAbsent(storeName, s -> new StoreBackend(this, s));
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

  public IngestionBackend getIngestionBackend() {
    return ingestionBackend;
  }

  public void verifyCacheConfigEquality(@Nullable ObjectCacheConfig newObjectCacheConfig, String storeName) {
    ObjectCacheConfig existingObjectCacheConfig =
        cacheBackend.isPresent() ? cacheBackend.get().getStoreCacheConfig() : null;
    if (!Objects.equals(existingObjectCacheConfig, newObjectCacheConfig)) {
      throw new VeniceClientException(
          "Cache config conflicts with existing backend, storeName=" + storeName + "; existing cache config: "
              + existingObjectCacheConfig + "; new cache config: " + newObjectCacheConfig);
    }
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

  protected void reportPushStatus(String kafkaTopic, int partition, ExecutionStatus status) {
    reportPushStatus(kafkaTopic, partition, status, Optional.empty());
  }

  protected void reportPushStatus(
      String kafkaTopic,
      int partition,
      ExecutionStatus status,
      Optional<String> incrementalPushVersion) {
    VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
    if (versionBackend != null && versionBackend.isReportingPushStatus()) {
      Version version = versionBackend.getVersion();
      if (writeBatchingPushStatus && !incrementalPushVersion.isPresent()) {
        // Batching the push statuses from all partitions for batch pushes;
        // VersionBackend will handle the push status update to Venice backend
        versionBackend.updatePartitionStatus(partition, status);
      } else {
        pushStatusStoreWriter
            .writePushStatus(version.getStoreName(), version.getNumber(), partition, status, incrementalPushVersion);
      }
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

  Version getVeniceLatestNonFaultyVersion(String storeName, Set<Integer> faultyVersions) {
    try {
      return getVeniceLatestNonFaultyVersion(getStoreRepository().getStoreOrThrow(storeName), faultyVersions);
    } catch (VeniceNoStoreException e) {
      return null;
    }
  }

  Version getVeniceCurrentVersion(String storeName) {
    try {
      return getVeniceCurrentVersion(getStoreRepository().getStoreOrThrow(storeName));
    } catch (VeniceNoStoreException e) {
      return null;
    }
  }

  private Version getVeniceLatestNonFaultyVersion(Store store, Set<Integer> faultyVersions) {
    Version latestNonFaultyVersion = null;
    for (Version version: store.getVersions()) {
      if (faultyVersions.contains(version.getNumber())) {
        continue;
      }
      if (latestNonFaultyVersion == null || latestNonFaultyVersion.getNumber() < version.getNumber()) {
        latestNonFaultyVersion = version;
      }
    }
    return latestNonFaultyVersion;
  }

  private Version getVeniceCurrentVersion(Store store) {
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
          /**
           * Report push status needs to be executed before deleting the {@link VersionBackend}.
           */
          ExecutionStatus status = getDaVinciErrorStatus(e, useDaVinciSpecificExecutionStatusForError);
          reportPushStatus(kafkaTopic, partitionId, status);

          versionBackend.completePartitionExceptionally(partitionId, e);
          versionBackend.tryStopHeartbeat();
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

  static ExecutionStatus getDaVinciErrorStatus(Exception e, boolean useDaVinciSpecificExecutionStatusForError) {
    ExecutionStatus status;
    if (useDaVinciSpecificExecutionStatusForError) {
      status = DVC_INGESTION_ERROR_OTHER;
      if (e instanceof VeniceException) {
        if (e instanceof MemoryLimitExhaustedException
            || (e.getCause() != null && e.getCause() instanceof MemoryLimitExhaustedException)) {
          status = DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
        } else if (e instanceof DiskLimitExhaustedException
            || (e.getCause() != null && e.getCause() instanceof DiskLimitExhaustedException)) {
          status = DVC_INGESTION_ERROR_DISK_FULL;
        }
      }
    } else {
      status = ExecutionStatus.ERROR;
    }
    return status;
  }
}

package com.linkedin.davinci;

import com.linkedin.davinci.config.StoreBackendConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.IngestionReportListener;
import com.linkedin.davinci.ingestion.IngestionRequestClient;
import com.linkedin.davinci.ingestion.IngestionStorageMetadataService;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.RelayNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.MetadataUpdateStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.SubscriptionBasedStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.StaticClusterInfoProvider;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static java.lang.Thread.*;


public class DaVinciBackend implements Closeable {
  private static final Logger logger = Logger.getLogger(DaVinciBackend.class);

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

  private IngestionRequestClient ingestionRequestClient;
  private IngestionReportListener ingestionReportListener;
  private VeniceNotifier isolatedIngestionListener;
  private Process isolatedIngestionService;

  public DaVinciBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader, Optional<Set<String>> managedClients) {
    VeniceServerConfig backendConfig = configLoader.getVeniceServerConfig();
    this.configLoader = configLoader;
    metricsRepository = Optional.ofNullable(clientConfig.getMetricsRepository())
                            .orElse(TehutiUtils.getMetricsRepository("davinci-client"));

    ClusterInfoProvider clusterInfoProvider;
    VeniceProperties backendProps = backendConfig.getClusterProperties();
    if (backendProps.getBoolean(CLIENT_USE_SYSTEM_STORE_REPOSITORY, false)) {
      logger.info("Initializing DaVinciBackend repositories with " + NativeMetadataRepository.class.getSimpleName());
      NativeMetadataRepository
          metadataStoreBasedStoreRepository = NativeMetadataRepository.getInstance(clientConfig, backendProps);
      clusterInfoProvider = metadataStoreBasedStoreRepository;
      storeRepository = metadataStoreBasedStoreRepository;
      schemaRepository = metadataStoreBasedStoreRepository;
      zkClient = null;
    } else {
      String clusterName = backendConfig.getClusterName();
      clusterInfoProvider = new StaticClusterInfoProvider(Collections.singleton(clusterName));

      HelixAdapterSerializer adapter = new HelixAdapterSerializer();
      zkClient = ZkClientFactory.newZkClient(backendConfig.getZookeeperAddress());
      zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, ".davinci-zk-client"));

      storeRepository = new SubscriptionBasedStoreRepository(zkClient, adapter, clusterName);
      storeRepository.refresh();

      schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
      schemaRepository.refresh();
    }

    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()));
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);

    SchemaReader versionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()));
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSerializer.setSchemaReader(versionStateSchemaReader);

    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    rocksDBMemoryStats = backendConfig.isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", backendConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) : null;
    storageService = new StorageService(configLoader, storageEngineStats, rocksDBMemoryStats, storeVersionStateSerializer, partitionStateSerializer);
    storageService.start();

    VeniceWriterFactory writerFactory = new VeniceWriterFactory(backendProps.toProperties());
    String instanceName = Utils.getHostName() + "_" + Utils.getPid();
    int derivedSchemaID = backendProps.getInt(PUSH_STATUS_STORE_DERIVED_SCHEMA_ID, 1);
    pushStatusStoreWriter = new PushStatusStoreWriter(writerFactory, instanceName, derivedSchemaID);

    SchemaReader kafkaMessageEnvelopeSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()));

    // TODO: May need to reorder the object to make it looks cleaner.
    storageMetadataService = backendConfig.getIngestionMode().equals(IngestionMode.ISOLATED)
        ? new IngestionStorageMetadataService(backendConfig.getIngestionServicePort(), partitionStateSerializer, new MetadataUpdateStats(metricsRepository))
        : new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);

    ingestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        configLoader,
        storageMetadataService,
        clusterInfoProvider,
        storeRepository,
        schemaRepository,
        metricsRepository,
        rocksDBMemoryStats,
        Optional.of(kafkaMessageEnvelopeSchemaReader),
        Optional.empty(),
        partitionStateSerializer);
    ingestionService.start();
    ingestionService.addCommonNotifier(ingestionListener);

    /**
     * In order to make bootstrap logic compatible with ingestion isolation, we first scan all local storage engines,
     * record all store versions that are up-to-date and close all storage engines. This will make sure child process
     * can open RocksDB stores.
     */
    Map<Version, List<Integer>> bootstrapVersions = new HashMap<>();
    bootstrap(managedClients, bootstrapVersions);

    if (configLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED)) {
      // We will close all storage engines only when live update suppression is NOT turned on.
      if (!configLoader.getVeniceServerConfig().freezeIngestionIfReadyToServeOrLocalDataExists()) {
        // Close all opened store engines so child process can open them.
        StorageEngineRepository engineRepository = storageService.getStorageEngineRepository();
        logger.info("Storage service has " + engineRepository.getAllLocalStorageEngines().size() + " storage engines before cleanup.");
        for (AbstractStorageEngine storageEngine : engineRepository.getAllLocalStorageEngines()) {
          storageService.closeStorageEngine(storageEngine.getName());
        }
        logger.info("Storage service has " + engineRepository.getAllLocalStorageEngines().size() + " storage engines after cleanup.");
      }

      // Initialize isolated ingestion service.
      int ingestionServicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
      int ingestionListenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
      ingestionRequestClient = new IngestionRequestClient(ingestionServicePort);
      isolatedIngestionService = ingestionRequestClient.startForkedIngestionProcess(configLoader);

      // Isolated ingestion listener handles status report from isolated ingestion service.
      isolatedIngestionListener = new RelayNotifier(ingestionListener) {
        @Override
        public void completed(String kafkaTopic, int partitionId, long offset, String message) {
          VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
          if (versionBackend != null) {
            versionBackend.completeSubPartitionSubscription(partitionId);
          }
        }
      };

      try {
        ingestionReportListener = new IngestionReportListener(ingestionListenerPort, ingestionServicePort, partitionStateSerializer);
        ingestionReportListener.setIngestionNotifier(isolatedIngestionListener);
        ingestionReportListener.setMetricsRepository(metricsRepository);
        ingestionReportListener.setStorageMetadataService((IngestionStorageMetadataService) storageMetadataService);
        ingestionReportListener.setConfigLoader(configLoader);
        ingestionReportListener.startInner();
      } catch (Exception e) {
        throw new VeniceException("Unable to start ingestion report listener.", e);
      }

      // Send out subscribe requests to child process to complete bootstrap process.
      completeBootstrapRemotely(bootstrapVersions);
    }

    storeRepository.registerStoreDataChangedListener(storeChangeListener);
  }

  protected synchronized void bootstrap(Optional<Set<String>> managedClients, Map<Version, List<Integer>> bootstrapVersions) {
    List<AbstractStorageEngine> storageEngines = storageService.getStorageEngineRepository().getAllLocalStorageEngines();
    logger.info("Starting bootstrap, storageEngines=" + storageEngines + ", managedClients=" + managedClients);
    for (AbstractStorageEngine storageEngine : storageEngines) {
      String kafkaTopicName = storageEngine.getName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);

      if (storeByNameMap.containsKey(storeName)) {
        // The latest version has been already discovered, so all other local versions will be deleted.
        logger.info("Deleting obsolete local version " + kafkaTopicName);
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      try {
        storeRepository.subscribe(storeName);
      } catch (VeniceNoStoreException e) {
        // The version does not exist in Venice anymore, so it will be deleted.
        logger.info("Deleting invalid local version " + kafkaTopicName);
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      } catch (InterruptedException e) {
        logger.info("StoreRepository::subscribe was interrupted", e);
        currentThread().interrupt();
      }

      Version version = getLatestVersion(storeName, Collections.emptySet()).orElse(null);
      if (version == null || !version.kafkaTopicName().equals(kafkaTopicName)) {
        // The version is not the latest, so it will be deleted.
        logger.info("Deleting obsolete local version " + kafkaTopicName);
        storeRepository.unsubscribe(storeName);
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      StoreBackend storeBackend = getStoreOrThrow(storeName);
      if (managedClients.isPresent()) {
        if (storeBackend.isManaged() && !managedClients.get().contains(storeName)) {
          logger.info("Deleting unused managed version " + kafkaTopicName);
          deleteStore(storeName);
          storageService.removeStorageEngine(kafkaTopicName);
          continue;
        }
      }

      int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
      List<Integer> partitions = PartitionUtils.getUserPartitions(storageEngine.getPartitionIds(), amplificationFactor);
      logger.info("Bootstrapping partitions " + partitions + " of " + kafkaTopicName);

      if (configLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED)) {
        // If ingestion isolation is turned on, we will not subscribe to versions immediately, but instead save all versions of interest.
        bootstrapVersions.put(version, partitions);
      } else {
        storeBackend.subscribe(ComplementSet.newSet(partitions), Optional.of(version));
      }
    }

    String baseDataPath = configLoader.getVeniceServerConfig().getDataBasePath();
    for (String storeName : StoreBackendConfig.listConfigs(baseDataPath)) {
      if (!storeByNameMap.containsKey(storeName)) {
        new StoreBackendConfig(baseDataPath, storeName).delete();
      }
    }
  }

  protected synchronized void completeBootstrapRemotely(Map<Version, List<Integer>> bootstrapVersions) {
    bootstrapVersions.forEach((version, partitions) -> {
      logger.info("Bootstrapping partitions " + partitions + " of " + version.kafkaTopicName() + " via isolated ingestion service.");
      StoreBackend storeBackend = getStoreOrThrow(version.getStoreName());
      for (Integer partition : partitions) {
        ingestionReportListener.addVersionPartitionToIngestionMap(version.kafkaTopicName(), partition);
      }
      storeBackend.subscribe(ComplementSet.newSet(partitions), Optional.of(version));
    });
  }

  @Override
  public synchronized void close() {
    storeRepository.unregisterStoreDataChangedListener(storeChangeListener);
    for (StoreBackend storeBackend : storeByNameMap.values()) {
      storeBackend.close();
    }
    storeByNameMap.clear();
    versionByTopicMap.clear();

    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    try {
      if (ingestionReportListener != null) {
        ingestionReportListener.stopInner();
      }
      if (isolatedIngestionService != null) {
        ingestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.KAFKA_INGESTION_SERVICE);
        ingestionService.stop();
        ingestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.STORAGE_SERVICE);
        isolatedIngestionService.destroy();
        if (ingestionRequestClient != null) {
          ingestionRequestClient.close();
        }
      } else {
        ingestionService.stop();
      }
      storageService.stop();
      if (zkClient != null) {
        zkClient.close();
      }
      metricsRepository.close();
      storeRepository.clear();
      schemaRepository.clear();
      pushStatusStoreWriter.close();
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Da Vinci backend", e);
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

  SubscriptionBasedReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public ReadOnlySchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  StorageService getStorageService() {
    return storageService;
  }

  IngestionReportListener getIngestionReportListener() {
    return ingestionReportListener;
  }

  StoreIngestionService getIngestionService() {
    return ingestionService;
  }

  Map<String, VersionBackend> getVersionByTopicMap() {
    return versionByTopicMap;
  }

  void setMemoryLimit(String storeName, long memoryLimit) {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.registerStore(storeName, memoryLimit);
    }
  }

  IngestionRequestClient getIngestionRequestClient() {
    return ingestionRequestClient;
  }

  PushStatusStoreWriter getPushStatusStoreWriter() {
    return pushStatusStoreWriter;
  }

  Optional<Version> getLatestVersion(String storeName, Set<Integer> faultyVersions) {
    try {
      return getLatestVersion(getStoreRepository().getStoreOrThrow(storeName), faultyVersions);
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  static Optional<Version> getLatestVersion(Store store, Set<Integer> faultyVersions) {
    return store.getVersions().stream().filter(v -> !faultyVersions.contains(v.getNumber()))
               .max(Comparator.comparing(Version::getNumber));
  }

  Optional<Version> getCurrentVersion(String storeName, Set<Integer> faultyVersions) {
    try {
      return getCurrentVersion(getStoreRepository().getStoreOrThrow(storeName), faultyVersions);
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  static Optional<Version> getCurrentVersion(Store store, Set<Integer> faultyVersions) {
    int versionNumber = store.getCurrentVersion();
    return faultyVersions.contains(versionNumber) ? Optional.empty() : store.getVersion(versionNumber);
  }

  protected void reportPushStatus(String kafkaTopic, int subPartition, ExecutionStatus status) {
    reportPushStatus(kafkaTopic, subPartition, status, Optional.empty());
  }

  protected void reportPushStatus(String kafkaTopic, int subPartition, ExecutionStatus status, Optional<String> incrementalPushVersion) {
    VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
    if (versionBackend != null && versionBackend.isReportingPushStatus()) {
      Version version = versionBackend.getVersion();
      pushStatusStoreWriter.writePushStatus(version.getStoreName(), version.getNumber(), subPartition, status,
          incrementalPushVersion);
    }
  }

  protected void deleteStore(String storeName) {
    StoreBackend storeBackend = storeByNameMap.remove(storeName);
    if (storeBackend != null) {
      storeBackend.delete();
    }
  }

  private final StoreDataChangedListener storeChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreChanged(Store store) {
      StoreBackend storeBackend = storeByNameMap.get(store.getName());
      if (storeBackend != null) {
        storeBackend.deleteOldVersions();
        storeBackend.trySubscribeFutureVersion();
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
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.completeSubPartition(partitionId);
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.COMPLETED);
      }
    }

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception e) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.completeSubPartitionExceptionally(partitionId, e);
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.ERROR);
      }
    }

    @Override
    public void started(String kafkaTopic, int partitionId, String message) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.STARTED, Optional.empty());
        versionBackend.tryStartHeartbeat();
      }
    }

    @Override
    public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.tryStartHeartbeat();
      }
    }

    @Override
    public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
      reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.END_OF_PUSH_RECEIVED);
    }

    @Override
    public void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset, String message) {
      reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED);
    }

    @Override
    public void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(incrementalPushVersion));
        versionBackend.tryStartHeartbeat();
      }
    }

    @Override
    public void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.tryStopHeartbeat();
        reportPushStatus(kafkaTopic, partitionId, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, Optional.of(incrementalPushVersion));
      }
    }
  };
}

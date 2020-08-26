package com.linkedin.davinci;

import com.linkedin.venice.MetadataStoreBasedStoreRepository;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.storage.StorageEngineMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import io.tehuti.metrics.MetricsRepository;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.client.store.ClientFactory.*;
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
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Map<String, StoreBackend> storeByNameMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VersionBackend> versionByTopicMap = new VeniceConcurrentHashMap<>();

  public DaVinciBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader, boolean useSystemStoreBasedRepository) {
    this.configLoader = configLoader;

    metricsRepository =
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository("davinci-client"));

    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    zkClient = ZkClientFactory.newZkClient(configLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, ".davinci-zk-client"));

    String clusterName = configLoader.getVeniceClusterConfig().getClusterName();

    if (useSystemStoreBasedRepository) {
      MetadataStoreBasedStoreRepository metadataStoreBasedStoreRepository = new MetadataStoreBasedStoreRepository(clusterName, clientConfig, 60);
      storeRepository = metadataStoreBasedStoreRepository;
      schemaRepository = metadataStoreBasedStoreRepository;
    } else {
      storeRepository = new DaVinciStoreRepository(zkClient, adapter, clusterName);
      storeRepository.refresh();

      schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
      schemaRepository.refresh();
    }
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    rocksDBMemoryStats = configLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats") : null;
    storageService = new StorageService(configLoader, s -> {},
        null, storageEngineStats, rocksDBMemoryStats);
    storageService.start();

    SchemaReader schemaReader = getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()));

    ingestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        configLoader,
        new StorageEngineMetadataService(storageService.getStorageEngineRepository()),
        storeRepository,
        schemaRepository,
        metricsRepository,
        rocksDBMemoryStats,
        Optional.of(schemaReader),
        Optional.of(clientConfig));
    ingestionService.start();
    ingestionService.addCommonNotifier(ingestionListener);

    bootstrap();
    storeRepository.registerStoreDataChangedListener(storeChangeListener);
  }

  protected synchronized void bootstrap() {
    for (AbstractStorageEngine storageEngine : storageService.getStorageEngineRepository().getAllLocalStorageEngines()) {
      String kafkaTopicName = storageEngine.getName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);

      if (storeByNameMap.containsKey(storeName)) {
        // We have discovered the current version for the store, so we will delete all other local versions.
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      try {
        storeRepository.subscribe(storeName);
      } catch (InterruptedException e) {
        logger.info("Subscribe method is interrupted " + e.getMessage());
        Thread.currentThread().interrupt();
      }
      Version version = getLatestVersion(storeName).orElse(null);
      if (version == null || version.kafkaTopicName().equals(kafkaTopicName) == false) {
        logger.info("Deleting obsolete local version " + kafkaTopicName);
        // If the version is not the latest, it should be removed.
        storageService.removeStorageEngine(kafkaTopicName);
        storeRepository.unsubscribe(storeName);
        continue;
      }

      logger.info("Bootstrapping local version " + kafkaTopicName);
      StoreBackend store = getStoreOrThrow(storeName);
      int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
      Set<Integer> partitions = PartitionUtils.getUserPartitions(storageEngine.getPartitionIds(), amplificationFactor);
      store.subscribe(ComplementSet.wrap(partitions), Optional.of(version));
    }
  }

  @Override
  public synchronized void close() {
    storeRepository.unregisterStoreDataChangedListener(storeChangeListener);
    for (StoreBackend store : storeByNameMap.values()) {
      store.close();
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
      ingestionService.stop();
      storageService.stop();
      zkClient.close();
      metricsRepository.close();
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Da Vinci backend", e);
    }
  }

  public synchronized StoreBackend getStoreOrThrow(String storeName) {
    return storeByNameMap.computeIfAbsent(storeName, k -> new StoreBackend(this, storeName));
  }

  ExecutorService getExecutor() {
    return executor;
  }

  VeniceConfigLoader getConfigLoader() {
    return configLoader;
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

  StoreIngestionService getIngestionService() {
    return ingestionService;
  }

  Map<String, VersionBackend> getVersionByTopicMap() {
    return versionByTopicMap;
  }

  public void registerRocksDBMemoryLimit(String storeName, long limit) {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.registerStore(storeName, limit);
    }
  }

  Optional<Version> getLatestVersion(String storeName) {
    try {
      return getLatestVersion(storeRepository.getStoreOrThrow(storeName));
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  static Optional<Version> getLatestVersion(Store store) {
    return store.getVersions().stream().max(Comparator.comparing(Version::getNumber));
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
      StoreBackend storeBackend = storeByNameMap.remove(store.getName());
      if (storeBackend != null) {
        storeBackend.delete();
      }
    }
  };

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.completeSubPartition(partitionId);
      }
    }
  };
}

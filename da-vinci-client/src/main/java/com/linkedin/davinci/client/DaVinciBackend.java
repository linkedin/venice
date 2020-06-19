package com.linkedin.davinci.client;

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
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.storage.StorageEngineMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import io.tehuti.metrics.MetricsRepository;

import org.apache.helix.zookeeper.impl.client.ZkClient;

import java.io.Closeable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.client.store.ClientFactory.*;
import static java.lang.Thread.*;


public class DaVinciBackend implements Closeable {
  private final ZkClient zkClient;
  private final VeniceConfigLoader configLoader;
  private final DaVinciStoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final MetricsRepository metricsRepository;
  private final StorageService storageService;
  private final KafkaStoreIngestionService ingestionService;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Map<String, StoreBackend> storeByNameMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VersionBackend> versionByTopicMap = new VeniceConcurrentHashMap<>();

  public DaVinciBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader) {
    this.configLoader = configLoader;

    metricsRepository =
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository("davinci-client"));

    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    zkClient = ZkClientFactory.newZkClient(configLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, ".davinci-zk-client"));

    String clusterName = configLoader.getVeniceClusterConfig().getClusterName();
    storeRepository = new DaVinciStoreRepository(zkClient, adapter, clusterName);
    storeRepository.refresh();

    schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
    schemaRepository.refresh();

    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    storageService = new StorageService(configLoader, storageEngineStats);
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
        Optional.of(schemaReader),
        Optional.of(clientConfig));
    ingestionService.start();
    ingestionService.addNotifier(ingestionListener);

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

      storeRepository.subscribe(storeName);
      Version version = getLatestVersion(storeName).orElse(null);
      if (version == null || version.kafkaTopicName().equals(kafkaTopicName) == false) {
        // If the version is not the latest, it should be removed.
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      StoreBackend store = storeByNameMap.computeIfAbsent(storeName, k -> new StoreBackend(this, storeName));
      store.subscribe(Optional.of(version), storageEngine.getPartitionIds());
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
    } catch (Exception e) {
      throw new VeniceException("Unable to stop Da Vinci backend", e);
    }
  }

  public synchronized StoreBackend getStoreOrThrow(String storeName) {
    StoreBackend store = storeByNameMap.computeIfAbsent(storeName, k -> new StoreBackend(this, storeName));
    store.subscribe(Optional.empty(), Collections.emptySet());
    return store;
  }

  ExecutorService getExecutor() {
    return executor;
  }

  VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  public DaVinciStoreRepository getStoreRepository() {
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

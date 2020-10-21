package com.linkedin.davinci;

import com.linkedin.venice.MetadataStoreBasedStoreRepository;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.SubscriptionBasedStoreRepository;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.ingestion.IngestionReportListener;
import com.linkedin.venice.ingestion.IngestionRequestClient;
import com.linkedin.venice.ingestion.IngestionService;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.IngestionIsolationMode;
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
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.Charsets;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.client.store.ClientFactory.*;
import static com.linkedin.venice.ingestion.IngestionUtils.*;
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

  private IngestionRequestClient ingestionRequestClient;
  private IngestionReportListener ingestionReportListener;
  private VeniceNotifier isolatedIngestionListener;
  private Process isolatedIngestionService;

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
      storeRepository = new SubscriptionBasedStoreRepository(zkClient, adapter, clusterName);
      storeRepository.refresh();

      schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
      schemaRepository.refresh();
    }
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    rocksDBMemoryStats = configLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", configLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) : null;
    storageService = new StorageService(configLoader, storageEngineStats, rocksDBMemoryStats);
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
    Map<String, Pair<Version, Set<Integer>>> bootstrapStoreNameToVersionPartitionsMap = new HashMap<>();
    // Start ingestion service in child process and ingestion listener service.
    if (configLoader.getVeniceServerConfig().getIngestionIsolationMode().equals(IngestionIsolationMode.PARENT_CHILD)) {
      /**
       * In order to make bootstrap logic compatible with ingestion isolation, we first scan all local storage engines,
       * record all store versions that are up-to-date and close all storage engines. This will make sure child process
       * can open RocksDB stores.
       */
      bootstrap(bootstrapStoreNameToVersionPartitionsMap);

      // Initialize isolated ingestion service.
      int ingestionServicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
      int ingestionListenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
      ingestionRequestClient = new IngestionRequestClient(ingestionServicePort);
      try {
        isolatedIngestionService = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(ingestionServicePort));
        // Wait for server in forked child process to bind the listening port.
        waitPortBinding(ingestionServicePort, 3000);

        // Isolated ingestion listener handles status report from isolated ingestion service.
        isolatedIngestionListener = new VeniceNotifier() {
          @Override
          public void completed(String kafkaTopic, int partitionId, long offset) {
            VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
            if (versionBackend != null) {
              versionBackend.completeSubPartitionByIsolatedIngestionService(partitionId);
            }
          }
        };

        ingestionReportListener = new IngestionReportListener(ingestionListenerPort, ingestionServicePort);
        ingestionReportListener.setIngestionNotifier(isolatedIngestionListener);
        ingestionReportListener.setMetricsRepository(metricsRepository);
        ingestionReportListener.startInner();

        InitializationConfigs initializationConfigs = new InitializationConfigs();
        initializationConfigs.aggregatedConfigs = new HashMap<>();
        configLoader.getCombinedProperties().toProperties().forEach((key, value) -> initializationConfigs.aggregatedConfigs.put(key.toString(), value.toString()));
        logger.info("Sending initialization aggregatedConfigs to child process: " + initializationConfigs.aggregatedConfigs);
        byte[] content = serializeInitializationConfigs(initializationConfigs);
        HttpRequest httpRequest = ingestionRequestClient.buildHttpRequest(IngestionAction.INIT, content);
        FullHttpResponse response = ingestionRequestClient.sendRequest(httpRequest);
        if (response.status() != HttpResponseStatus.OK) {
          ByteBuf message = response.content();
          String stringMessage = message.readCharSequence(message.readableBytes(), Charsets.UTF_8).toString();
          throw new VeniceException("Isolated ingestion service initialization failed: " + stringMessage);
        }

        logger.info("Isolated ingestion service initialization finished.");
      } catch (Exception e) {
        throw new VeniceException("Exception caught during initialization of ingestion service.", e);
      }
      // Send out subscribe requests to child process to complete bootstrap process.
      bootstrapWithIngestionIsolation(bootstrapStoreNameToVersionPartitionsMap);
    } else {
      bootstrap(bootstrapStoreNameToVersionPartitionsMap);
    }
    storeRepository.registerStoreDataChangedListener(storeChangeListener);
  }

  protected synchronized void bootstrap(Map<String, Pair<Version, Set<Integer>>> bootstrapStoreNameToVersionPartitionsMap) {
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
      if (version == null || !version.kafkaTopicName().equals(kafkaTopicName)) {
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
      if (configLoader.getVeniceServerConfig().getIngestionIsolationMode().equals(IngestionIsolationMode.PARENT_CHILD)) {
        logger.info("Will bootstrap store " + storeName + " with version " + version.kafkaTopicName() + " with partitions: " + partitions);
        // If ingestion isolation is turned on, we will not subscribe to versions immediately, but instead stores all versions of interest.
        bootstrapStoreNameToVersionPartitionsMap.put(storeName, new Pair<>(version, partitions));
      } else {
        store.subscribe(ComplementSet.wrap(partitions), Optional.of(version));
      }
    }

    if (configLoader.getVeniceServerConfig().getIngestionIsolationMode().equals(IngestionIsolationMode.PARENT_CHILD)) {
      // Close all opened store engines so child process can open them.
      logger.info(
          "Storage service has " + storageService.getStorageEngineRepository().getAllLocalStorageEngines().size() + " storage engine before clean up.");
      for (AbstractStorageEngine storageEngine : storageService.getStorageEngineRepository().getAllLocalStorageEngines()) {
        storageService.getStorageEngineRepository().removeLocalStorageEngine(storageEngine.getName()).close();
      }
      logger.info(
          "Storage service has " + storageService.getStorageEngineRepository().getAllLocalStorageEngines().size() + " storage engine after clean up.");
    }
  }

  // bootstrapWithIngestionIsolation sends out store subscribe request to isolated ingestion service to complete bootstrap.
  protected synchronized void bootstrapWithIngestionIsolation(Map<String, Pair<Version, Set<Integer>>> bootstrapStoreNameToVersionPartitionsMap) {
    bootstrapStoreNameToVersionPartitionsMap.forEach((name, versionPartitionPair) -> {
      StoreBackend store = getStoreOrThrow(name);
      Version bootstrapVersion = versionPartitionPair.getFirst();
      logger.info("Bootstrap sending subscribe request to isolated ingestion service" + name + " " + bootstrapVersion + " " + versionPartitionPair.getSecond().toString());
      store.subscribe(ComplementSet.wrap(versionPartitionPair.getSecond()), Optional.of(bootstrapVersion));
    });
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
      if (ingestionReportListener != null) {
        ingestionReportListener.stopInner();
      }
      if (ingestionRequestClient != null) {
        ingestionRequestClient.close();
      }
      ingestionService.stop();
      storageService.stop();
      zkClient.close();
      metricsRepository.close();

      if (isolatedIngestionService != null) {
        isolatedIngestionService.destroy();
      }

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

  public IngestionRequestClient getIngestionRequestClient() {
    return ingestionRequestClient;
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

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception e) {
      VersionBackend versionBackend = versionByTopicMap.get(kafkaTopic);
      if (versionBackend != null) {
        versionBackend.completeErrorSubPartition(partitionId, e);
      }
    }
  };
}

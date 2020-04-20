package com.linkedin.davinci.client;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.controller.init.SystemSchemaInitializationRoutine;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.storage.StorageEngineMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.venice.storage.chunking.GenericChunkingAdapter;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Objects;
import java.util.HashSet;
import org.apache.avro.Schema;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.venice.client.store.ClientFactory.*;


public class AvroGenericDaVinciClientImpl<K, V> implements DaVinciClient<K, V> {
  private static String DAVINCI_CLIENT_NAME = "davinci_client";
  private static int REFRESH_ATTEMPTS_FOR_ZK_RECONNECT = 1;
  private static int REFRESH_INTERVAL_FOR_ZK_RECONNECT_IN_MS = 1;
  private static final Logger logger = Logger.getLogger(AvroGenericDaVinciClientImpl.class);

  private final String storeName;
  protected final boolean useFastAvro;
  private final MetricsRepository metricsRepository;
  private final DaVinciConfig daVinciConfig;
  private final ClientConfig clientConfig;
  private final List<AbstractVeniceService> services = new ArrayList<>();
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private ZkClient zkClient;
  private ReadOnlyStoreRepository metadataReposotory;
  private ReadOnlySchemaRepository schemaRepository;
  private StorageService storageService;
  private StorageEngineMetadataService storageMetadataService;
  private KafkaStoreIngestionService kafkaStoreIngestionService;
  private RecordSerializer<K> keySerializer;
  private DaVinciPartitioner partitioner;
  private IngestionController ingestionController;
  private TransportClient transportClient;
  private AvroGenericStoreClient<K, V> veniceClient;

  public AvroGenericDaVinciClientImpl(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig) {
    this.daVinciConfig = daVinciConfig;
    this.clientConfig = clientConfig;
    this.storeName = clientConfig.getStoreName();
    this.useFastAvro = clientConfig.isUseFastAvro();
    this.metricsRepository = Optional.ofNullable(clientConfig.getMetricsRepository())
        .orElse(TehutiUtils.getMetricsRepository(DAVINCI_CLIENT_NAME));
  }

  private D2TransportClient getD2TransportClient() {
    transportClient = ClientFactory.getTransportClient(clientConfig);
    if (!(transportClient instanceof D2TransportClient)) {
      throw new VeniceClientException("Da Vinci only supports D2 client.");
    }
    return (D2TransportClient) transportClient;
  }

  private VeniceConfigLoader buildVeniceConfigLoader() {
    final D2ServiceDiscoveryResponseV2 d2ServiceDiscoveryResponse;
    try (D2TransportClient d2TransportClient = getD2TransportClient()) {
      D2ServiceDiscovery d2ServiceDiscovery = new D2ServiceDiscovery();
      d2ServiceDiscoveryResponse = d2ServiceDiscovery.discoverD2Service(d2TransportClient, getStoreName());
    }
    String zkAddress = d2ServiceDiscoveryResponse.getZkAddress();
    String kafkaZkAddress = Objects.requireNonNull(d2ServiceDiscoveryResponse.getKafkaZkAddress());
    String kafkaBootstrapServers = Objects.requireNonNull(d2ServiceDiscoveryResponse.getKafkaBootstrapServers());
    String clusterName = Objects.requireNonNull(d2ServiceDiscoveryResponse.getCluster());

    VeniceProperties clusterProperties = new PropertyBuilder()
        // Helix-related config
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, zkAddress)
        // Kafka-related config
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
        .put(ConfigKeys.KAFKA_ZK_ADDRESS, kafkaZkAddress)
        // Other configs
        .put(ConfigKeys.CLUSTER_NAME, clusterName)
        .put(ConfigKeys.PERSISTENCE_TYPE, daVinciConfig.getPersistenceType())
        .build();
    // Generate server.properties in config directory
    VeniceProperties serverProperties = new PropertyBuilder()
        .put(ConfigKeys.LISTENER_PORT, 0) // not used by Da Vinci
        .put(ConfigKeys.DATA_BASE_PATH, daVinciConfig.getDataBasePath())
        .put(RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, daVinciConfig.isRocksDBPlainTableFormatEnabled())
        .build();
    VeniceProperties serverOverrideProperties = new VeniceProperties(new Properties());
    VeniceConfigLoader
        veniceConfigLoader = new VeniceConfigLoader(clusterProperties, serverProperties, serverOverrideProperties);
    return veniceConfigLoader;
  }

  @Override
  public CompletableFuture<Void> subscribeToAllPartitions() {
    // TODO: add non-static partitioning support
    Store store = metadataReposotory.getStoreOrThrow(storeName);
    String msg = "Cannot subscribe to an empty store " + storeName + ". Please push data to the store first.";
    Version version = store.getVersions().stream().findAny().orElseThrow(() -> new VeniceClientException(msg));
    Set<Integer> partitions = IntStream.range(0, version.getPartitionCount()).boxed().collect(Collectors.toSet());
    return subscribe(partitions);
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return ingestionController.subscribe(getStoreName(), partitions);
  }

  @Override
  public CompletableFuture<Void> unsubscribe(Set<Integer> partitions) {
    return ingestionController.unsubscribe(getStoreName(), partitions);
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    if (!isStarted()) {
      throw new VeniceClientException("Client is not started.");
    }

    // TODO: refactor IngestionController to use StoreBackend directly
    try (ReferenceCounted<IngestionController.VersionBackend> versionRef =
              ingestionController.getStoreOrThrow(storeName).getCurrentVersion()) {
      if (null == versionRef.get()) {
        throw new VeniceClientException("Failed to find a ready store version.");
      }
      Version version = versionRef.get().getVersion();

      V value = getValue(version, key);
      if (value == null && isRemoteQueryAllowed()) {
        return veniceClient.get(key);
      }
      return CompletableFuture.completedFuture(value);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    if (!isStarted()) {
      throw new VeniceClientException("Client is not started.");
    }

    // TODO: refactor IngestionController to use StoreBackend directly
    try (ReferenceCounted<IngestionController.VersionBackend> versionRef =
              ingestionController.getStoreOrThrow(storeName).getCurrentVersion()) {
      if (null == versionRef.get()) {
        throw new VeniceClientException("Failed to find a ready store version.");
      }
      Version version = versionRef.get().getVersion();

      Set<K> nonLocalKeys = new HashSet<>();
      Map<K, V> result = new HashMap<>();
      for (K key : keys) {
        V value = getValue(version, key);
        if (value != null) {
          // The returned map will only contain entries for the keys which have a value associated with them
          result.put(key, value);
        } else {
          nonLocalKeys.add(key);
        }
      }

      if (nonLocalKeys.isEmpty() || !isRemoteQueryAllowed()) {
        return CompletableFuture.completedFuture(result);
      }

      return veniceClient.batchGet(nonLocalKeys).thenApply(remoteResult -> {
        result.putAll(remoteResult);
        return result;
      });

    }
  }

  private boolean isRemoteQueryAllowed() {
    return daVinciConfig.getNonLocalReadsPolicy().equals(DaVinciConfig.NonLocalReadsPolicy.QUERY_REMOTELY);
  }

  private AbstractStorageEngine getStorageEngine(Version version) {
    String topic = version.kafkaTopicName();
    AbstractStorageEngine storageEngine = storageService.getStorageEngineRepository().getLocalStorageEngine(topic);
    if (null == storageEngine) {
      throw new VeniceClientException("Failed to find a ready store version.");
    }
    return storageEngine;
  }

  private V getValue(Version version, K key) {
    byte[] keyBytes = keySerializer.serialize(key);
    int partitionId = partitioner.getPartitionId(keyBytes, version.getPartitionCount());
    AbstractStorageEngine<?> storageEngine = getStorageEngine(version);
    if (!storageEngine.containsPartition(partitionId)) {
      if (isRemoteQueryAllowed()) {
        return null;
      }
      throw new VeniceClientException("Da Vinci client does not contain partition " + partitionId + " in version " + version.getNumber());
    }

    return getChunkingAdapter().get(
        storageEngine,
        partitionId,
        keyBytes,
        version.isChunkingEnabled(),
        null, // TODO: Consider extending the API to allow object reuse
        null,
        null,
        version.getCompressionStrategy(),
        useFastAvro,
        schemaRepository,
        storeName);
  }

  @Override
  public synchronized void start() throws VeniceClientException {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new VeniceClientException("Client is already started!");
    }
    VeniceConfigLoader veniceConfigLoader = buildVeniceConfigLoader();
    String clusterName = veniceConfigLoader.getVeniceClusterConfig().getClusterName();
    zkClient = ZkClientFactory.newZkClient(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "davinci-zk-client"));
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    metadataReposotory = new HelixReadOnlyStoreRepository(zkClient, adapter, clusterName,
        REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, REFRESH_INTERVAL_FOR_ZK_RECONNECT_IN_MS);
    metadataReposotory.refresh();
    schemaRepository = new HelixReadOnlySchemaRepository(metadataReposotory, zkClient, adapter, clusterName,
        REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, REFRESH_INTERVAL_FOR_ZK_RECONNECT_IN_MS);
    schemaRepository.refresh();

    AggVersionedStorageEngineStats
        storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, metadataReposotory);
    storageService = new StorageService(veniceConfigLoader, storageEngineStats);
    services.add(storageService);

    storageMetadataService = new StorageEngineMetadataService(storageService.getStorageEngineRepository());
    services.add(storageMetadataService);

    // SchemaReader of Kafka protocol
    SchemaReader schemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(SystemSchemaInitializationRoutine.getSystemStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)));
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        veniceConfigLoader,
        storageMetadataService,
        metadataReposotory,
        schemaRepository,
        metricsRepository,
        Optional.of(schemaReader),
        Optional.of(clientConfig));
    services.add(kafkaStoreIngestionService);
    this.keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(getKeySchema());
    // TODO: initiate ingestion service. pass in ingestionService as null to make it compile.
    PartitionerConfig partitionerConfig = metadataReposotory.getStore(getStoreName()).getPartitionerConfig();
    this.partitioner = new DaVinciPartitioner(partitionerConfig);

    // For now we only support FAIL_FAST and QUERY_REMOTELY. SUBSCRIBE_AND_QUERY_REMOTELY policy is not supported.
    if (isRemoteQueryAllowed()) {
      // Remote query is only supported for DefaultVenicePartitioner before custom partitioner is supported in Router.
      if (!partitionerConfig.getPartitionerClass().equals(DefaultVenicePartitioner.class.getName())) {
        throw new VeniceClientException("Da Vinci remote query is only supported for DefaultVenicePartitioner.");
      }
      veniceClient = getAndStartAvroClient(clientConfig);
    }

    ingestionController = new IngestionController(
        veniceConfigLoader,
        metadataReposotory,
        storageService,
        kafkaStoreIngestionService);

    logger.info("Starting " + services.size() + " services.");
    long start = System.currentTimeMillis();
    for (AbstractVeniceService service : services) {
      service.start();
    }
    ingestionController.start();
    long end = System.currentTimeMillis();
    logger.info("Startup completed in " + (end - start) + " ms.");
  }

  @Override
  public synchronized void close() {
    List<Exception> exceptions = new ArrayList<>();
    logger.info("Stopping all services ");

    /* Stop in reverse order */
    if (!isStarted()) {
      logger.info("The client is already stopped, ignoring duplicate attempt.");
      return;
    }
    ingestionController.close();
    for (AbstractVeniceService service : Utils.reversed(services)) {
      try {
        service.stop();
      } catch (Exception e) {
        exceptions.add(e);
        logger.error("Exception in stopping service: " + service.getName(), e);
      }
    }
    logger.info("All services stopped");

    if (veniceClient != null) {
      veniceClient.close();
    }

    if (exceptions.size() > 0) {
      throw new VeniceException(exceptions.get(0));
    }
    isStarted.set(false);

    metricsRepository.close();
    zkClient.close();
    isStarted.set(false);
  }

  @Override
  public String getStoreName() {
    return storeName;
  }

  @Override
  public Schema getKeySchema() {
    return schemaRepository.getKeySchema(getStoreName()).getSchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemaRepository.getLatestValueSchema(getStoreName()).getSchema();
  }

  /**
   * @return true if the {@link AvroGenericDaVinciClientImpl} and all of its inner services are fully started
   *         false if the {@link AvroGenericDaVinciClientImpl} was not started or if any of its inner services
   *         are not finished starting.
   */
  public boolean isStarted() {
    return isStarted.get() && services.stream().allMatch(abstractVeniceService -> abstractVeniceService.isStarted());
  }

  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return GenericChunkingAdapter.INSTANCE;
  }
}

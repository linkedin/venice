package com.linkedin.davinci.client;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
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
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
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
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.client.store.ClientFactory.*;


public class AvroGenericDaVinciClientImpl<K, V> implements DaVinciClient<K, V> {
  private static final Logger logger = Logger.getLogger(AvroGenericDaVinciClientImpl.class);
  private static String CLIENT_NAME = "davinci-client";

  protected final DaVinciConfig daVinciConfig;
  protected final ClientConfig clientConfig;
  protected final VeniceProperties backendConfig;
  private final List<AbstractVeniceService> services = new ArrayList<>();
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(() -> new ReusableObjects());

  private static class ReusableObjects {
    final ByteBuffer reusedRawValue = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[16], null);
    final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  }

  // TODO: refactor the code and move these to DaVinciBackend
  private ZkClient zkClient;
  private ReadOnlyStoreRepository storeReposotory;
  private ReadOnlySchemaRepository schemaRepository;
  private StorageService storageService;
  private StorageEngineMetadataService storageMetadataService;
  private KafkaStoreIngestionService kafkaStoreIngestionService;
  private MetricsRepository metricsRepository;
  private IngestionController ingestionController;
  private AvroGenericStoreClient<K, V> veniceClient;
  private VenicePartitioner partitioner;
  private RecordSerializer<K> keySerializer;

  public AvroGenericDaVinciClientImpl(
      DaVinciConfig config,
      ClientConfig clientConfig,
      VeniceProperties backendConfig) {
    this.daVinciConfig = config;
    this.clientConfig = clientConfig;
    this.backendConfig = backendConfig;
  }

  private D2ServiceDiscoveryResponseV2 discoverService() {
    try (TransportClient client = ClientFactory.getTransportClient(clientConfig)) {
      if (!(client instanceof D2TransportClient)) {
        throw new VeniceClientException("Venice service discovery requires D2 client, got " + client.getClass());
      }
      D2ServiceDiscovery serviceDiscovery = new D2ServiceDiscovery();
      D2ServiceDiscoveryResponseV2 response = serviceDiscovery.discoverD2Service((D2TransportClient) client, getStoreName());
      logger.info("Venice service discovered" +
                       ", clusterName=" + response.getCluster() +
                       ", zkAddress=" + response.getZkAddress() +
                       ", kafkaZkAddress=" + response.getKafkaZkAddress() +
                       ", kafkaBootstrapServers=" + response.getKafkaBootstrapServers());
      return response;
    } catch (Exception e) {
      throw new VeniceClientException("Failed venice service discovery", e);
    }
  }

  private VeniceConfigLoader buildVeniceConfig() {
    D2ServiceDiscoveryResponseV2 discoveryResponse = discoverService();
    String clusterName = backendConfig.getString(ConfigKeys.CLUSTER_NAME, discoveryResponse.getCluster());
    String zkAddress = backendConfig.getString(ConfigKeys.ZOOKEEPER_ADDRESS, discoveryResponse.getZkAddress());
    String kafkaZkAddress = backendConfig.getString(ConfigKeys.KAFKA_ZK_ADDRESS, discoveryResponse.getKafkaZkAddress());
    String kafkaBootstrapServers = backendConfig.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, discoveryResponse.getKafkaBootstrapServers());

    VeniceProperties clusterProperties = new PropertyBuilder()
        .put(backendConfig.toProperties())
        .put(ConfigKeys.CLUSTER_NAME, clusterName)
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, zkAddress)
        .put(ConfigKeys.KAFKA_ZK_ADDRESS, kafkaZkAddress)
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
        .build();
    logger.info("clusterConfig=" + clusterProperties.toString(true));

    VeniceProperties serverProperties = new PropertyBuilder()
        .put(backendConfig.toProperties())
        .put(ConfigKeys.LISTENER_PORT, 0) // not used by Da Vinci
        .put(RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED,
            daVinciConfig.getStorageClass() == StorageClass.DISK_BACKED_MEMORY)
        .build();
    logger.info("serverConfig=" + serverProperties.toString(true));

    return new VeniceConfigLoader(clusterProperties, serverProperties, new VeniceProperties(new Properties()));
  }

  @Override
  public CompletableFuture<Void> subscribeToAllPartitions() {
    // TODO: add non-static partitioning support
    Store store = storeReposotory.getStoreOrThrow(getStoreName());
    String msg = "Cannot subscribe to an empty store " + getStoreName() + ". Please push data to the store first.";
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
    return get(key, null);
  }

  @Override
  public CompletableFuture<V> get(K key, V reusedValue) throws VeniceClientException {

    if (!isStarted()) {
      throw new VeniceClientException("Client is not started.");
    }

    // TODO: refactor IngestionController to use StoreBackend directly
    try (ReferenceCounted<IngestionController.VersionBackend> versionRef =
              ingestionController.getStoreOrThrow(getStoreName()).getCurrentVersion()) {
      if (null == versionRef.get()) {
        throw new VeniceClientException("Failed to find a ready store version.");
      }
      Version version = versionRef.get().getVersion();

      V value = getValue(version, key, reusedValue);
      if (value == null && isRemoteQueryAllowed()) {
        // TODO: Refactor this... using null as a sentinel value is not good, because if the partition is local but the value is truly null, we would do a useless remote call...
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
              ingestionController.getStoreOrThrow(getStoreName()).getCurrentVersion()) {
      if (null == versionRef.get()) {
        throw new VeniceClientException("Failed to find a ready store version.");
      }
      Version version = versionRef.get().getVersion();

      Set<K> nonLocalKeys = new HashSet<>();
      Map<K, V> result = new HashMap<>();
      for (K key : keys) {
        // TODO: Consider supporting object re-use for batch get as well.
        V value = getValue(version, key, null);
        if (value != null) {
          // The returned map will only contain entries for the keys which have a value associated with them
          result.put(key, value);
        } else {
          // TODO: Refactor this... using null as a sentinel value is not good, because if the partition is local but the value is truly null, we would do a useless remote call...
          nonLocalKeys.add(key);
        }
      }

      if (nonLocalKeys.isEmpty() || !isRemoteQueryAllowed()) {
        return CompletableFuture.completedFuture(result);
      }

      return veniceClient.batchGet(nonLocalKeys).thenApply(remoteResult -> {
        // TODO: Refactor this... using null as a sentinel value is not good, because if the partition is local but the value is truly null, we would do a useless remote call...
        result.putAll(remoteResult);
        return result;
      });

    }
  }

  private boolean isRemoteQueryAllowed() {
    return daVinciConfig.getRemoteReadPolicy().equals(RemoteReadPolicy.QUERY_REMOTELY);
  }

  private AbstractStorageEngine getStorageEngine(Version version) {
    String topic = version.kafkaTopicName();
    AbstractStorageEngine storageEngine = storageService.getStorageEngineRepository().getLocalStorageEngine(topic);
    if (null == storageEngine) {
      throw new VeniceClientException("Failed to find a ready store version.");
    }
    return storageEngine;
  }

  private V getValue(Version version, K key, V reusedValue) {
    ReusableObjects reusableObjects = threadLocalReusableObjects.get();

    byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
    // TODO: Align the implementation with the design, which expects the following sub partition calculation:
    // int subPartitionId = partitioner.getPartitionId(keyBytes, version.getPartitionCount()) * version.getPartitionerConfig().getAmplificationFactor()
    //    + partitioner.getPartitionId(keyBytes, version.getPartitionerConfig().getAmplificationFactor());
    int subPartitionId = partitioner.getPartitionId(keyBytes, version.getPartitionCount()
        * version.getPartitionerConfig().getAmplificationFactor());
    AbstractStorageEngine<?> storageEngine = getStorageEngine(version);
    if (!storageEngine.containsPartition(subPartitionId)) {
      if (isRemoteQueryAllowed()) {
        // TODO: Refactor this... using null as a sentinel value is not good, because if the partition is local but the value is truly null, we would do a useless remote call...
        return null;
      }
      throw new VeniceClientException("Da Vinci client does not contain partition " + subPartitionId + " in version " + version.getNumber());
    }

    return getChunkingAdapter().get(
        clientConfig.getStoreName(),
        storageEngine,
        subPartitionId,
        keyBytes,
        reusableObjects.reusedRawValue,
        reusedValue,
        reusableObjects.binaryDecoder,
        version.isChunkingEnabled(),
        version.getCompressionStrategy(),
        clientConfig.isUseFastAvro(),
        schemaRepository,
        null);
  }

  @Override
  public synchronized void start() throws VeniceClientException {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new VeniceClientException("Client is already started!");
    }

    VeniceConfigLoader veniceConfigLoader = buildVeniceConfig();
    String clusterName = veniceConfigLoader.getVeniceClusterConfig().getClusterName();

    metricsRepository =
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository(CLIENT_NAME));

    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    zkClient = ZkClientFactory.newZkClient(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "davinci-zk-client"));

    storeReposotory = new HelixReadOnlyStoreRepository(zkClient, adapter, clusterName,3, 1000);
    storeReposotory.refresh();

    schemaRepository = new HelixReadOnlySchemaRepository(storeReposotory, zkClient, adapter, clusterName,3, 1000);
    schemaRepository.refresh();

    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeReposotory);
    storageService = new StorageService(veniceConfigLoader, storageEngineStats);
    services.add(storageService);

    storageMetadataService = new StorageEngineMetadataService(storageService.getStorageEngineRepository());
    services.add(storageMetadataService);

    SchemaReader schemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(
            SystemSchemaInitializationRoutine.getSystemStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)));

    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        veniceConfigLoader,
        storageMetadataService,
        storeReposotory,
        schemaRepository,
        metricsRepository,
        Optional.of(schemaReader),
        Optional.of(clientConfig));
    services.add(kafkaStoreIngestionService);
    this.keySerializer = clientConfig.isUseFastAvro()
        ? FastSerializerDeserializerFactory.getFastAvroGenericSerializer(getKeySchema(), false)
        : SerializerDeserializerFactory.getAvroGenericSerializer(getKeySchema(), false);

    // TODO: initiate ingestion service. pass in ingestionService as null to make it compile.
    PartitionerConfig partitionerConfig = storeReposotory.getStore(getStoreName()).getPartitionerConfig();
    Properties params = new Properties();
    params.putAll(partitionerConfig.getPartitionerParams());
    VeniceProperties partitionerProperties = new VeniceProperties(params);
    partitioner = PartitionUtils.getVenicePartitioner(
        partitionerConfig.getPartitionerClass(),
        partitionerConfig.getAmplificationFactor(),
        partitionerProperties);

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
        storeReposotory,
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
    return clientConfig.getStoreName();
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

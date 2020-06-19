package com.linkedin.davinci.client;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.venice.storage.chunking.GenericChunkingAdapter;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.venice.client.store.ClientFactory.*;


public class AvroGenericDaVinciClientImpl<K, V> implements DaVinciClient<K, V> {
  private static final Logger logger = Logger.getLogger(AvroGenericDaVinciClientImpl.class);

  protected final DaVinciConfig daVinciConfig;
  protected final ClientConfig clientConfig;
  protected final VeniceProperties backendConfig;

  private VenicePartitioner partitioner;
  private RecordSerializer<K> keySerializer;
  private AvroGenericStoreClient<K, V> veniceClient;
  private StoreBackend storeBackend;
  private static ReferenceCounted<DaVinciBackend> daVinciBackend;

  private static class ReusableObjects {
    final ByteBuffer reusedRawValue = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[16], null);
    final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  }

  private final AtomicBoolean isReady = new AtomicBoolean(false);
  private final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(() -> new ReusableObjects());

  public AvroGenericDaVinciClientImpl(DaVinciConfig daVinciConfig, ClientConfig clientConfig, VeniceProperties backendConfig) {
    this.daVinciConfig = daVinciConfig;
    this.clientConfig = clientConfig;
    this.backendConfig = backendConfig;
  }

  @Override
  public String getStoreName() {
    return clientConfig.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    throwIfNotReady();
    return daVinciBackend.get().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    throwIfNotReady();
    return daVinciBackend.get().getSchemaRepository().getLatestValueSchema(getStoreName()).getSchema();
  }

  @Override
  public CompletableFuture<Void> subscribeToAllPartitions() {
    throwIfNotReady();
    // TODO: add non-static partitioning support
    Store store = daVinciBackend.get().getStoreRepository().getStoreOrThrow(getStoreName());
    String msg = "Cannot subscribe to an empty store " + getStoreName() + ". Please push data to the store first.";
    Version version = store.getVersions().stream().findAny().orElseThrow(() -> new VeniceException(msg));
    Set<Integer> partitions = IntStream.range(0, version.getPartitionCount()).boxed().collect(Collectors.toSet());
    return subscribe(partitions);
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    throwIfNotReady();
    return storeBackend.subscribe(Optional.empty(), partitions);
  }

  @Override
  public void unsubscribeFromAllPartitions() {
    throwIfNotReady();
    storeBackend.unsubscribeAll();
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    throwIfNotReady();
    storeBackend.unsubscribe(partitions);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return get(key, null);
  }

  @Override
  public CompletableFuture<V> get(K key, V reusedValue) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (null == versionBackend) {
        if (isRemoteReadAllowed()) {
          return veniceClient.get(key);
        }
        throw new VeniceException("Unable to find a ready version, storeName=" + getStoreName());
      }

      Version version = versionBackend.getVersion();
      AbstractStorageEngine<?> storageEngine = versionBackend.getStorageEngine();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
      int subPartitionId = getSubPartitionId(version, keyBytes);

      if (versionBackend.isReadyToServe(subPartitionId)) {
        V value = getValueFromStorageEngine(storageEngine, version, subPartitionId, keyBytes, reusableObjects, reusedValue);
        return CompletableFuture.completedFuture(value);
      }

      if (isRemoteReadAllowed()) {
        return veniceClient.get(key);
      }

      if (!storageEngine.containsPartition(subPartitionId)) {
        throw new VeniceException("Cannot access not-subscribed partition, version=" + version.kafkaTopicName() + ", partition=" + subPartitionId);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (null == versionBackend) {
        if (isRemoteReadAllowed()) {
          return veniceClient.batchGet(keys);
        }
        throw new VeniceException("Unable to find a ready version, storeName=" + getStoreName());
      }

      Version version = versionBackend.getVersion();
      AbstractStorageEngine<?> storageEngine = versionBackend.getStorageEngine();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();

      Map<K, V> result = new HashMap<>();
      Set<K> remoteKeys = new HashSet<>();
      for (K key : keys) {
        byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
        int subPartitionId = getSubPartitionId(version, keyBytes);

        if (versionBackend.isReadyToServe(subPartitionId)) {
          // TODO: Consider supporting object re-use for batch get as well.
          V value = getValueFromStorageEngine(storageEngine, version, subPartitionId, keyBytes, reusableObjects, null);
          // The returned map will only contain entries for the keys which have a value associated with them
          if (value != null) {
            result.put(key, value);
          }
        } else if (isRemoteReadAllowed()) {
          remoteKeys.add(key);
        } else if (!storageEngine.containsPartition(subPartitionId)) {
          throw new VeniceException("Cannot access not-subscribed partition, version=" + version.kafkaTopicName() + ", partition=" + subPartitionId);
        }
      }

      if (remoteKeys.isEmpty()) {
        return CompletableFuture.completedFuture(result);
      }

      return veniceClient.batchGet(remoteKeys).thenApply(remoteResult -> {
        result.putAll(remoteResult);
        return result;
      });
    }
  }

  protected boolean isReady() {
    return isReady.get();
  }

  protected boolean isRemoteReadAllowed() {
    return daVinciConfig.getRemoteReadPolicy().equals(RemoteReadPolicy.QUERY_REMOTELY);
  }

  protected void throwIfNotReady() {
    if (!isReady()) {
      throw new VeniceException("Da Vinci client is not ready, storeName=" + getStoreName());
    }
  }

  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return GenericChunkingAdapter.INSTANCE;
  }

  private int getSubPartitionId(Version version, byte[] keyBytes) {
    // TODO: Align the implementation with the design, which expects the following sub partition calculation:
    // int subPartitionId = partitioner.getPartitionId(keyBytes, version.getPartitionCount()) * version.getPartitionerConfig().getAmplificationFactor()
    //    + partitioner.getPartitionId(keyBytes, version.getPartitionerConfig().getAmplificationFactor());
    return partitioner.getPartitionId(keyBytes,
        version.getPartitionCount() * version.getPartitionerConfig().getAmplificationFactor());
  }

  private V getValueFromStorageEngine(AbstractStorageEngine<?> storageEngine, Version version, int subPartitionId, byte[] keyBytes, ReusableObjects reusableObjects, V reusedValue) {
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
        daVinciBackend.get().getSchemaRepository(),
        null);
  }

  private D2ServiceDiscoveryResponseV2 discoverService() {
    try (TransportClient client = getTransportClient(clientConfig)) {
      if (!(client instanceof D2TransportClient)) {
        throw new VeniceException("Venice service discovery requires D2 client, " +
                                            "storeName=" + getStoreName() +
                                            "clientClass=" + client.getClass());
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
      throw new VeniceException("Failed venice service discovery for " + getStoreName(), e);
    }
  }

  private VeniceConfigLoader buildVeniceConfig() {
    D2ServiceDiscoveryResponseV2 discoveryResponse = discoverService();
    String clusterName = discoveryResponse.getCluster();
    String zkAddress = discoveryResponse.getZkAddress();
    String kafkaZkAddress = discoveryResponse.getKafkaZkAddress();
    String kafkaBootstrapServers = discoveryResponse.getKafkaBootstrapServers();
    if (zkAddress == null) {
      zkAddress = backendConfig.getString(ConfigKeys.ZOOKEEPER_ADDRESS);
    }
    if (kafkaZkAddress == null) {
      kafkaZkAddress = backendConfig.getString(ConfigKeys.KAFKA_ZK_ADDRESS);
    }
    if (kafkaBootstrapServers == null) {
      kafkaBootstrapServers = backendConfig.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
    }
    VeniceProperties config = new PropertyBuilder()
            /** Allows {@link com.linkedin.venice.kafka.TopicManager} to work Scala-free */
            .put(ConfigKeys.KAFKA_ADMIN_CLASS, KafkaAdminClient.class.getName())
            .put(ConfigKeys.SERVER_ENABLE_KAFKA_OPENSSL, false)
            .put(backendConfig.toProperties())
            .put(ConfigKeys.CLUSTER_NAME, clusterName)
            .put(ConfigKeys.ZOOKEEPER_ADDRESS, zkAddress)
            .put(ConfigKeys.KAFKA_ZK_ADDRESS, kafkaZkAddress)
            .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
            .put(RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED,
                daVinciConfig.getStorageClass() == StorageClass.DISK_BACKED_MEMORY)
            .build();
    logger.info("backendConfig=" + config.toString(true));
    return new VeniceConfigLoader(config, config);
  }

  private static synchronized void initBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader) {
    if (daVinciBackend == null) {
      daVinciBackend = new ReferenceCounted<>(new DaVinciBackend(clientConfig, configLoader), backend -> {
        daVinciBackend = null;
        backend.close();
      });
    } else {
      daVinciBackend.retain();
    }
  }

  @Override
  public synchronized void start() {
    if (isReady()) {
      throw new VeniceException("Da Vinci client is already started, storeName=" + getStoreName());
    }

    logger.info("Starting Da Vinci client, storeName=" + getStoreName());
    VeniceConfigLoader configLoader = buildVeniceConfig();
    initBackend(clientConfig, configLoader);
    storeBackend = daVinciBackend.get().getStoreOrThrow(getStoreName());

    PartitionerConfig partitionerConfig = daVinciBackend.get().getStoreRepository().getStoreOrThrow(getStoreName()).getPartitionerConfig();
    // TODO: Remove this check when routers fully support custom partitioners
    if (isRemoteReadAllowed() && !partitionerConfig.getPartitionerClass().equals(DefaultVenicePartitioner.class.getName())) {
      throw new VeniceException("Da Vinci remote query is only supported when DefaultVenicePartitioner is used.");
    }
    partitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);

    Schema keySchema = daVinciBackend.get().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
    keySerializer = clientConfig.isUseFastAvro()
                        ? FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema, false)
                        : SerializerDeserializerFactory.getAvroGenericSerializer(keySchema, false);

    if (isRemoteReadAllowed()) {
      veniceClient = getAndStartAvroClient(clientConfig);
    }

    isReady.set(true);
    logger.info("Da Vinci client started successfully, storeName=" + getStoreName());
  }

  @Override
  public synchronized void close() {
    throwIfNotReady();
    try {
      logger.info("Closing Da Vinci client, storeName=" + getStoreName());
      isReady.set(false);
      if (veniceClient != null) {
        veniceClient.close();
      }
      daVinciBackend.release();
      logger.info("Closed Da Vinci client, storeName=" + getStoreName());
    } catch (Exception e) {
      throw new VeniceException("Unable to close Da Vinci client, storeName=" + getStoreName(), e);
    }
  }
}

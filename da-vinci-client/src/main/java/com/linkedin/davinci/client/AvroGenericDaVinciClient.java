package com.linkedin.davinci.client;

import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;

import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
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

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.client.store.ClientFactory.*;


public class AvroGenericDaVinciClient<K, V> implements DaVinciClient<K, V> {
  protected final Logger logger = Logger.getLogger(getClass());

  private static class ReusableObjects {
    final ByteBuffer rawValue = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[16], null);
    final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  }
  private static final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(() -> new ReusableObjects());

  private final DaVinciConfig daVinciConfig;
  private final ClientConfig clientConfig;
  private final VeniceProperties backendConfig;
  private final Optional<Set<String>> managedClients;
  private final ICProvider icProvider;
  private final AtomicBoolean ready = new AtomicBoolean(false);
  // TODO: Implement copy-on-write ComplementSet to support concurrent modification and reading.
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();

  private RecordSerializer<K> keySerializer;
  private AvroGenericStoreClient<K, V> veniceClient;
  private StoreBackend storeBackend;
  private static ReferenceCounted<DaVinciBackend> daVinciBackend;
  private final CompressorFactory compressorFactory;
  private final GenericChunkingAdapter chunkingAdapter;

  public AvroGenericDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients) {
    this(daVinciConfig, clientConfig, backendConfig, managedClients, null);
  }

  public AvroGenericDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider) {
    logger.info("Creating client, storeName=" + clientConfig.getStoreName() + ", daVinciConfig=" + daVinciConfig);
    this.daVinciConfig = daVinciConfig;
    this.clientConfig = clientConfig;
    this.backendConfig = backendConfig;
    this.managedClients = managedClients;
    this.icProvider = icProvider;
    this.compressorFactory = new CompressorFactory();
    this.chunkingAdapter = new GenericChunkingAdapter(compressorFactory);
  }

  @Override
  public String getStoreName() {
    return clientConfig.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    throwIfNotReady();
    return getBackend().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    throwIfNotReady();
    return getBackend().getSchemaRepository().getLatestValueSchema(getStoreName()).getSchema();
  }

  @Override
  public int getPartitionCount() {
    throwIfNotReady();
    Store store = getBackend().getStoreRepository().getStoreOrThrow(getStoreName());
    return store.getVersion(store.getCurrentVersion()).map(Version::getPartitionCount).orElseGet(store::getPartitionCount);
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    return subscribe(ComplementSet.universalSet());
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return subscribe(ComplementSet.wrap(partitions));
  }

  protected CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    throwIfNotReady();
    subscription.addAll(partitions);
    return storeBackend.subscribe(partitions);
  }

  @Override
  public void unsubscribeAll() {
    unsubscribe(ComplementSet.universalSet());
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    unsubscribe(ComplementSet.wrap(partitions));
  }

  protected void unsubscribe(ComplementSet<Integer> partitions) {
    throwIfNotReady();
    if (daVinciConfig.isIsolated()) {
      ComplementSet<Integer> notSubscribedPartitions = ComplementSet.newSet(partitions);
      notSubscribedPartitions.removeAll(subscription);
      if (!notSubscribedPartitions.isEmpty()) {
        logger.warn("Partitions " + notSubscribedPartitions + " of " + getStoreName() +
                         " are not subscribed, ignoring unsubscribe request.");
        partitions = ComplementSet.newSet(partitions);
        partitions.removeAll(notSubscribedPartitions);
      }
    }
    subscription.removeAll(partitions);
    storeBackend.unsubscribe(partitions);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return get(key, null);
  }

  @Override
  public CompletableFuture<V> get(K key, V reusableValue) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.get(key);
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }

      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
      int subPartition = versionBackend.getSubPartition(keyBytes);

      if (isSubPartitionReadyToServe(versionBackend, subPartition)) {
        V value = versionBackend.read(
            subPartition,
            keyBytes,
            getChunkingAdapter(),
            reusableObjects.binaryDecoder,
            reusableObjects.rawValue,
            reusableValue);
        return CompletableFuture.completedFuture(value);
      }

      if (isVeniceQueryAllowed()) {
        return veniceClient.get(key);
      }

      if (!isSubPartitionSubscribed(versionBackend, subPartition)) {
        storeBackend.getStats().recordBadRequest();
        throw new NonLocalAccessException(versionBackend.toString(), subPartition);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.batchGet(keys);
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }

      Map<K, V> result = new HashMap<>();
      Set<K> missingKeys = new HashSet<>();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      for (K key : keys) {
        byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
        int subPartition = versionBackend.getSubPartition(keyBytes);

        if (isSubPartitionReadyToServe(versionBackend, subPartition)) {
          V value = versionBackend.read(
              subPartition,
              keyBytes,
              getChunkingAdapter(),
              reusableObjects.binaryDecoder,
              reusableObjects.rawValue,
              null); // TODO: Consider supporting object re-use for batch get as well.
          // The result should only contain entries for the keys that have a value associated with them
          if (value != null) {
            result.put(key, value);
          }

        } else if (isVeniceQueryAllowed()) {
          missingKeys.add(key);

        } else if (!isSubPartitionSubscribed(versionBackend, subPartition)) {
          storeBackend.getStats().recordBadRequest();
          throw new NonLocalAccessException(versionBackend.toString(), subPartition);
        }
      }

      if (missingKeys.isEmpty()) {
        return CompletableFuture.completedFuture(result);
      }

      return veniceClient.batchGet(missingKeys).thenApply(veniceResult -> {
        result.putAll(veniceResult);
        return result;
      });
    }
  }

  public boolean isReady() {
    return ready.get();
  }

  protected boolean isVeniceQueryAllowed() {
    return daVinciConfig.getNonLocalAccessPolicy().equals(NonLocalAccessPolicy.QUERY_VENICE);
  }

  protected boolean isSubPartitionReadyToServe(VersionBackend versionBackend, int subPartition) {
    if (daVinciConfig.isIsolated() && !subscription.contains(versionBackend.getUserPartition(subPartition))) {
      return false;
    }
    return versionBackend.isSubPartitionReadyToServe(subPartition);
  }

  protected boolean isSubPartitionSubscribed(VersionBackend versionBackend, int subPartition) {
    if (daVinciConfig.isIsolated()) {
      return subscription.contains(versionBackend.getUserPartition(subPartition));
    }
    return versionBackend.isSubPartitionSubscribed(subPartition);
  }

  protected void throwIfNotReady() {
    if (!isReady()) {
      throw new VeniceClientException("Da Vinci client is not ready, storeName=" + getStoreName());
    }
  }

  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return chunkingAdapter;
  }

  private D2ServiceDiscoveryResponseV2 discoverService() {
    try (TransportClient client = getTransportClient(clientConfig)) {
      if (!(client instanceof D2TransportClient)) {
        throw new VeniceClientException("Venice service discovery requires D2 client" +
                                            ", storeName=" + getStoreName() +
                                            ", clientClass=" + client.getClass());
      }
      D2ServiceDiscovery serviceDiscovery = new D2ServiceDiscovery();
      D2ServiceDiscoveryResponseV2 response;
      Callable<D2ServiceDiscoveryResponseV2> discoveryCallable = () -> serviceDiscovery.discoverD2Service((D2TransportClient) client, getStoreName());
      if (icProvider != null) {
        response = icProvider.call(this.getClass().getCanonicalName(), discoveryCallable);
      } else {
        response = discoveryCallable.call();
      }
      logger.info("Venice service discovered" +
                      ", clusterName=" + response.getCluster() +
                      ", zkAddress=" + response.getZkAddress() +
                      ", kafkaZkAddress=" + response.getKafkaZkAddress() +
                      ", kafkaBootstrapServers=" + response.getKafkaBootstrapServers());
      return response;
    } catch (Throwable e) {
      throw new VeniceClientException("Unable to discover Venice service, storeName=" + getStoreName(), e);
    }
  }

  private VeniceConfigLoader buildVeniceConfig() {
    D2ServiceDiscoveryResponseV2 discoveryResponse = discoverService();
    String clusterName = discoveryResponse.getCluster();
    String zkAddress = discoveryResponse.getZkAddress();
    String kafkaZkAddress = discoveryResponse.getKafkaZkAddress();
    String kafkaBootstrapServers = discoveryResponse.getKafkaBootstrapServers();
    if (zkAddress == null) {
      zkAddress = backendConfig.getString(ZOOKEEPER_ADDRESS);
    }
    if (kafkaZkAddress == null) {
      kafkaZkAddress = backendConfig.getString(KAFKA_ZK_ADDRESS);
    }
    if (kafkaBootstrapServers == null) {
      kafkaBootstrapServers = backendConfig.getString(KAFKA_BOOTSTRAP_SERVERS);
    }
    VeniceProperties config = new PropertyBuilder()
            .put(KAFKA_ADMIN_CLASS, KafkaAdminClient.class.getName())
            .put(SERVER_ENABLE_KAFKA_OPENSSL, false)
            .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 4) // RocksDB default config
            .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20) // RocksDB default config
            .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 36) // RocksDB default config
            .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40)
            .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60)
            .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 80)
            .put(backendConfig.toProperties())
            .put(CLUSTER_NAME, clusterName)
            .put(ZOOKEEPER_ADDRESS, zkAddress)
            .put(KAFKA_ZK_ADDRESS, kafkaZkAddress)
            .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
            .put(RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED,
                daVinciConfig.getStorageClass() == StorageClass.MEMORY_BACKED_BY_DISK)
            .put(INGESTION_USE_DA_VINCI_CLIENT, true)
            .build();
    logger.info("backendConfig=" + config.toString(true));
    return new VeniceConfigLoader(config, config);
  }

  private synchronized void initBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader,
      Optional<Set<String>> managedClients, ICProvider icProvider) {
    if (daVinciBackend == null) {
      GenericRecordChunkingAdapter chunkingAdapter = new GenericRecordChunkingAdapter(getCompressorFactory());
      daVinciBackend = new ReferenceCounted<>(new DaVinciBackend(clientConfig, configLoader, managedClients, icProvider, chunkingAdapter),
          backend -> {
        daVinciBackend = null;
        backend.close();
      });
    } else if (VeniceSystemStoreType.getSystemStoreType(clientConfig.getStoreName()) != VeniceSystemStoreType.META_STORE) {
      // Do not increment DaVinciBackend reference count for meta system store da-vinci clients. Once the last user da-vinci
      // client is released the backend can be safely deleted since meta system stores are meaningless without user stores
      // and they are cheap to re-bootstrap.
      daVinciBackend.retain();
    }
  }

  // Visible for testing
  public static synchronized DaVinciBackend getBackend() {
    return daVinciBackend.get();
  }

  protected CompressorFactory getCompressorFactory() {
    return compressorFactory;
  }

  @Override
  public synchronized void start() {
    if (isReady()) {
      throw new VeniceClientException("Da Vinci client is already started, storeName=" + getStoreName());
    }
    logger.info("Starting client, storeName=" + getStoreName());
    VeniceConfigLoader configLoader = buildVeniceConfig();
    initBackend(clientConfig, configLoader, managedClients, icProvider);

    try {
      storeBackend = getBackend().getStoreOrThrow(getStoreName());
      if (managedClients.isPresent()) {
        storeBackend.setManaged(daVinciConfig.isManaged());
      }
      storeBackend.setMemoryLimit(daVinciConfig.getMemoryLimit());

      Schema keySchema = getBackend().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
      keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema, false);

      if (isVeniceQueryAllowed()) {
        veniceClient = getAndStartAvroClient(clientConfig);
      }

      ready.set(true);
      logger.info("Client is started successfully, storeName=" + getStoreName());
    } catch (Throwable e) {
      daVinciBackend.release();
      throw new VeniceClientException("Unable to start Da Vinci client, storeName=" + getStoreName(), e);
    }
  }

  @Override
  public synchronized void close() {
    throwIfNotReady();
    try {
      logger.info("Closing client, storeName=" + getStoreName());
      ready.set(false);
      if (veniceClient != null) {
        veniceClient.close();
      }
      IOUtils.closeQuietly(compressorFactory);
      daVinciBackend.release();
      logger.info("Client is closed successfully, storeName=" + getStoreName());
    } catch (Throwable e) {
      throw new VeniceClientException("Unable to close Da Vinci client, storeName=" + getStoreName(), e);
    }
  }
}

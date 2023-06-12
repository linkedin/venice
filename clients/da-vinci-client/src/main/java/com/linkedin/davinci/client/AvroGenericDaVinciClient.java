package com.linkedin.davinci.client;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.INGESTION_ISOLATION_CONFIG_PREFIX;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_CLASS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.client.store.ClientFactory.getAndStartAvroClient;
import static com.linkedin.venice.client.store.ClientFactory.getTransportClient;
import static org.apache.avro.Schema.Type.RECORD;

import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.AvroComputeRequestBuilderV4;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ComputeUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AvroGenericDaVinciClient<K, V> implements DaVinciClient<K, V>, AvroGenericReadComputeStoreClient<K, V> {
  protected final Logger logger = LogManager.getLogger(getClass());

  private static class ReusableObjects {
    final ByteBuffer rawValue = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[16], null);
    private static final int REUSABLE_MAP_CAPACITY = 100;
    private static final float REUSABLE_MAP_LOAD_FACTOR = 0.75f;
    // LRU cache for storing schema->record map for object reuse of value and result record
    final LinkedHashMap<Schema, GenericRecord> reuseValueRecordMap =
        new LinkedHashMap<Schema, GenericRecord>(REUSABLE_MAP_CAPACITY, REUSABLE_MAP_LOAD_FACTOR, true) {
          protected boolean removeEldestEntry(Map.Entry<Schema, GenericRecord> eldest) {
            return size() > REUSABLE_MAP_CAPACITY;
          }
        };
  }

  private static final ThreadLocal<ReusableObjects> REUSABLE_OBJECTS = ThreadLocal.withInitial(ReusableObjects::new);

  private final DaVinciConfig daVinciConfig;
  private final ClientConfig clientConfig;
  private final VeniceProperties backendConfig;
  private final Optional<Set<String>> managedClients;
  private final ICProvider icProvider;
  private final AtomicBoolean ready = new AtomicBoolean(false);
  // TODO: Implement copy-on-write ComplementSet to support concurrent modification and reading.
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();

  private RecordSerializer<K> keySerializer;
  private RecordDeserializer<K> keyDeserializer;
  private AvroGenericReadComputeStoreClient<K, V> veniceClient;
  private StoreBackend storeBackend;
  private static ReferenceCounted<DaVinciBackend> daVinciBackend;
  private ObjectCacheBackend cacheBackend;
  private static final Map<CharSequence, Schema> computeResultSchemaCache = new VeniceConcurrentHashMap<>();

  private final AbstractAvroChunkingAdapter<V> chunkingAdapter;

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
    this(
        daVinciConfig,
        clientConfig,
        backendConfig,
        managedClients,
        icProvider,
        GenericChunkingAdapter.INSTANCE,
        () -> {});
  }

  protected AvroGenericDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      AbstractAvroChunkingAdapter<V> chunkingAdapter,
      Runnable preValidation) {
    logger.info("Creating client, storeName={}, daVinciConfig={}", clientConfig.getStoreName(), daVinciConfig);
    this.daVinciConfig = daVinciConfig;
    this.clientConfig = clientConfig;
    this.backendConfig = backendConfig;
    this.managedClients = managedClients;
    this.icProvider = icProvider;
    this.chunkingAdapter = chunkingAdapter;
    preValidation.run();
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
    return getBackend().getSchemaRepository().getSupersetOrLatestValueSchema(getStoreName()).getSchema();
  }

  @Override
  public int getPartitionCount() {
    throwIfNotReady();
    Store store = getBackend().getStoreRepository().getStoreOrThrow(getStoreName());
    return store.getVersion(store.getCurrentVersion())
        .map(Version::getPartitionCount)
        .orElseGet(store::getPartitionCount);
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
    if (daVinciConfig.isCacheEnabled()) {
      dropAllCachePartitions();
    }
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
        logger.warn(
            "Partitions {} of {} are not subscribed, ignoring unsubscribe request.",
            notSubscribedPartitions,
            getStoreName());
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

  // TODO: This is 'almost' the same logic for the batchGet path. We could probably wrap this function and adapt it to
  // the batch-get api (where sometimes the Batch-get is just for a single key). Advantages would be to remove duplicate
  // code.
  private CompletableFuture<V> readFromLocalStorage(K key, V reusableValue) {
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      byte[] keyBytes = keySerializer.serialize(key);
      int partition = versionBackend.getPartition(keyBytes);

      if (isPartitionReadyToServe(versionBackend, partition)) {
        ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
        V value = versionBackend.read(
            partition,
            keyBytes,
            getAvroChunkingAdapter(),
            reusableObjects.binaryDecoder,
            reusableObjects.rawValue,
            reusableValue);
        return CompletableFuture.completedFuture(value);
      }

      if (isVeniceQueryAllowed()) {
        return veniceClient.get(key);
      }

      if (!isPartitionSubscribed(versionBackend, partition)) {
        storeBackend.getStats().recordBadRequest();
        throw new NonLocalAccessException(versionBackend.toString(), partition);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<V> get(K key, V reusableValue) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.get(key);
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }

      if (daVinciConfig.isCacheEnabled()) {
        return cacheBackend.get(key, versionBackend.getVersion(), (k, executor) -> this.readFromLocalStorage(k, null));
      } else {
        return readFromLocalStorage(key, reusableValue);
      }
    }
  }

  CompletableFuture<Map<K, V>> batchGetFromLocalStorage(Iterable<K> keys) {
    // expose underlying getAll functionality.
    Map<K, V> result = new HashMap<>();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.batchGet(new HashSet<>((Collection<K>) keys));
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }
      Set<K> missingKeys = new HashSet<>();
      ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
      for (K key: keys) {
        byte[] keyBytes = keySerializer.serialize(key);
        int partition = versionBackend.getPartition(keyBytes);

        if (isPartitionReadyToServe(versionBackend, partition)) {
          V value = versionBackend.read(
              partition,
              keyBytes,
              getAvroChunkingAdapter(),
              reusableObjects.binaryDecoder,
              reusableObjects.rawValue,
              null); // TODO: Consider supporting object re-use for batch get as well.
          // The result should only contain entries for the keys that have a value associated with them
          if (value != null) {
            result.put(key, value);
          }

        } else if (isVeniceQueryAllowed()) {
          missingKeys.add(key);

        } else if (!isPartitionSubscribed(versionBackend, partition)) {
          storeBackend.getStats().recordBadRequest();
          throw new NonLocalAccessException(versionBackend.toString(), partition);
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

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (daVinciConfig.isCacheEnabled()) {
        return cacheBackend.getAll(keys, versionBackend.getVersion(), (ks) -> {
          try {
            return batchGetFromLocalStorage(ks).get();
          } catch (InterruptedException | ExecutionException e) {
            throw new VeniceClientException("Error performing batch get while loading cache!!", e);
          }
        }, (k, executor) -> this.readFromLocalStorage(k, null));
      } else {
        return this.batchGetFromLocalStorage(keys);
      }
    }
  }

  @Override
  public ComputeRequestBuilder<K> compute() throws VeniceClientException {
    return compute(Optional.empty(), Optional.empty(), 0);
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      long preRequestTimeInNS) {
    return new AvroComputeRequestBuilderV4<K>(this, getLatestValueSchema()).setStats(streamingStats);
  }

  private Schema getComputeResultSchema(ComputeRequestWrapper computeRequestWrapper) {
    // try to get the result schema from the cache
    CharSequence computeResultSchemaStr = computeRequestWrapper.getResultSchemaStr();
    Schema computeResultSchema = computeResultSchemaCache.get(computeResultSchemaStr);
    if (computeResultSchema == null) {
      computeResultSchema = Schema.parse(computeResultSchemaStr.toString());
      // sanity check on the result schema
      ComputeUtils.checkResultSchema(
          computeResultSchema,
          computeRequestWrapper.getValueSchema(),
          computeRequestWrapper.getComputeRequestVersion(),
          computeRequestWrapper.getOperations());
      computeResultSchemaCache.putIfAbsent(computeResultSchemaStr, computeResultSchema);
    }
    return computeResultSchema;
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      return;
    }

    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          veniceClient.compute(computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
          return;
        }
        storeBackend.getStats().recordBadRequest();
        callback.onCompletion(
            Optional.of(new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName())));
        return;
      }

      Set<K> missingKeys = new HashSet<>();

      ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
      Schema valueSchema = computeRequestWrapper.getValueSchema();
      GenericRecord reuseValueRecord =
          reusableObjects.reuseValueRecordMap.computeIfAbsent(valueSchema, k -> new GenericData.Record(valueSchema));

      Map<String, Object> globalContext = new HashMap<>();
      Schema computeResultSchema = getComputeResultSchema(computeRequestWrapper);

      for (K key: keys) {
        byte[] keyBytes = keySerializer.serialize(key);
        int partition = versionBackend.getPartition(keyBytes);

        if (isPartitionReadyToServe(versionBackend, partition)) {
          GenericRecord computeResultValue = versionBackend.compute(
              partition,
              keyBytes,
              getGenericRecordChunkingAdapter(),
              reusableObjects.binaryDecoder,
              reusableObjects.rawValue,
              reuseValueRecord,
              globalContext,
              computeRequestWrapper,
              computeResultSchema);

          if (computeResultValue != null) {
            callback.onRecordReceived(
                key,
                new ComputeGenericRecord(computeResultValue, computeRequestWrapper.getValueSchema()));
          } else {
            callback.onRecordReceived(key, null);
          }
        } else if (isVeniceQueryAllowed()) {
          missingKeys.add(key);
        } else if (!isPartitionSubscribed(versionBackend, partition)) {
          storeBackend.getStats().recordBadRequest();
          callback.onCompletion(Optional.of(new NonLocalAccessException(versionBackend.toString(), partition)));
          return;
        }
      }

      if (missingKeys.isEmpty()) {
        callback.onCompletion(Optional.empty());
        return;
      }

      veniceClient.compute(computeRequestWrapper, missingKeys, resultSchema, callback, preRequestTimeInNS);
    }
  }

  @Override
  public void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        storeBackend.getStats().recordBadRequest();
        callback.onCompletion(
            Optional.of(new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName())));
        return;
      }

      if (RECORD != getKeySchema().getType()) {
        callback.onCompletion(
            Optional.of(
                new VeniceClientException("Key schema must be of type Record to execute with a filter on key fields")));
        return;
      }

      ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
      Schema valueSchema = computeRequestWrapper.getValueSchema();
      GenericRecord reuseValueRecord =
          reusableObjects.reuseValueRecordMap.computeIfAbsent(valueSchema, k -> new GenericData.Record(valueSchema));

      Map<String, Object> globalContext = new HashMap<>();
      Schema computeResultSchema = getComputeResultSchema(computeRequestWrapper);

      int partitionCount = versionBackend.getPartitionCount();
      for (int currPartition = 0; currPartition < partitionCount; currPartition++) {
        if (isPartitionReadyToServe(versionBackend, currPartition)) {
          try {
            versionBackend.computeWithKeyPrefixFilter(
                keyPrefix,
                currPartition,
                callback,
                computeRequestWrapper,
                getGenericRecordChunkingAdapter(),
                (RecordDeserializer<GenericRecord>) keyDeserializer,
                reuseValueRecord,
                reusableObjects.binaryDecoder,
                globalContext,
                computeResultSchema);
          } catch (VeniceException e) {
            callback.onCompletion(Optional.of(e));
            return;
          }
        }
      }
      callback.onCompletion(Optional.empty());
    }
  }

  private boolean handleCallbackForEmptyKeySet(Set<K> keys, StreamingCallback callback) {
    if (keys.isEmpty()) {
      // no result for empty key set
      callback.onCompletion(Optional.empty());
      return true;
    }
    return false;
  }

  public boolean isReady() {
    return ready.get();
  }

  protected boolean isVeniceQueryAllowed() {
    return daVinciConfig.getNonLocalAccessPolicy().equals(NonLocalAccessPolicy.QUERY_VENICE);
  }

  /**
   * Check if user partition is ready to serve traffic.
   */
  protected boolean isPartitionReadyToServe(VersionBackend versionBackend, int partition) {
    if (daVinciConfig.isIsolated() && !subscription.contains(partition)) {
      return false;
    }
    return versionBackend.isPartitionReadyToServe(partition);
  }

  protected boolean isPartitionSubscribed(VersionBackend versionBackend, int partition) {
    if (daVinciConfig.isIsolated()) {
      return subscription.contains(partition);
    }
    return versionBackend.isPartitionSubscribed(partition);
  }

  private void dropAllCachePartitions() {
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getDaVinciCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend != null) {
        cacheBackend.clearCachedPartitions(versionBackend.getVersion());
      }
    }
  }

  protected void throwIfNotReady() {
    if (!isReady()) {
      throw new VeniceClientException("Da Vinci client is not ready, storeName=" + getStoreName());
    }
  }

  protected AbstractAvroChunkingAdapter<V> getAvroChunkingAdapter() {
    return chunkingAdapter;
  }

  protected GenericRecordChunkingAdapter getGenericRecordChunkingAdapter() {
    return GenericRecordChunkingAdapter.INSTANCE;
  }

  private D2ServiceDiscoveryResponse discoverService() {
    try (TransportClient client = getTransportClient(clientConfig)) {
      if (!(client instanceof D2TransportClient)) {
        throw new VeniceClientException(
            "Venice service discovery requires D2 client" + ", storeName=" + getStoreName() + ", clientClass="
                + client.getClass());
      }
      D2ServiceDiscovery serviceDiscovery = new D2ServiceDiscovery();
      D2ServiceDiscoveryResponse response = serviceDiscovery.find((D2TransportClient) client, getStoreName());
      logger.info(
          "Venice service discovered, clusterName={}, zkAddress={}, kafkaBootstrapServers={}",
          response.getCluster(),
          response.getZkAddress(),
          response.getKafkaBootstrapServers());
      return response;
    } catch (Throwable e) {
      throw new ServiceDiscoveryException("Failed to discover Venice service, storeName=" + getStoreName(), e);
    }
  }

  private VeniceConfigLoader buildVeniceConfig() {
    D2ServiceDiscoveryResponse discoveryResponse = discoverService();
    String clusterName = discoveryResponse.getCluster();
    String zkAddress = discoveryResponse.getZkAddress();
    String kafkaBootstrapServers = discoveryResponse.getKafkaBootstrapServers();
    if (zkAddress == null) {
      zkAddress = backendConfig.getString(ZOOKEEPER_ADDRESS);
    }
    if (kafkaBootstrapServers == null) {
      kafkaBootstrapServers = backendConfig.getString(KAFKA_BOOTSTRAP_SERVERS);
    }
    VeniceProperties config = new PropertyBuilder().put(KAFKA_ADMIN_CLASS, ApacheKafkaAdminAdapter.class.getName())
        .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 4) // RocksDB default config
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20) // RocksDB default config
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 36) // RocksDB default config
        .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40)
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60)
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 80)
        .put(backendConfig.toProperties())
        .put(CLUSTER_NAME, clusterName)
        .put(ZOOKEEPER_ADDRESS, zkAddress)
        .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, daVinciConfig.getStorageClass() == StorageClass.MEMORY_BACKED_BY_DISK)
        .put(INGESTION_USE_DA_VINCI_CLIENT, true)
        .put(INGESTION_ISOLATION_CONFIG_PREFIX + "." + INGESTION_MEMORY_LIMIT, -1) // Explicitly disable memory limiter
                                                                                   // in Isolated Process
        .build();
    logger.info("backendConfig=" + config.toString(true));
    return new VeniceConfigLoader(config, config);
  }

  private void initBackend(
      ClientConfig clientConfig,
      VeniceConfigLoader configLoader,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Optional<ObjectCacheConfig> cacheConfig) {
    synchronized (AvroGenericDaVinciClient.class) {
      if (daVinciBackend == null) {
        logger
            .info("Da Vinci Backend does not exist, creating a new backend for client: " + clientConfig.getStoreName());
        daVinciBackend = new ReferenceCounted<>(
            new DaVinciBackend(clientConfig, configLoader, managedClients, icProvider, cacheConfig),
            backend -> {
              // Ensure that existing backend is fully closed before a new one can be created.
              synchronized (AvroGenericDaVinciClient.class) {
                daVinciBackend = null;
                backend.close();
              }
            });
      } else if (VeniceSystemStoreType
          .getSystemStoreType(clientConfig.getStoreName()) != VeniceSystemStoreType.META_STORE) {
        logger.info("Da Vinci Backend exists, reusing existing backend for client: " + clientConfig.getStoreName());
        // Do not increment DaVinciBackend reference count for meta system store da-vinci clients. Once the last user
        // da-vinci
        // client is released the backend can be safely deleted since meta system stores are meaningless without user
        // stores,
        // and they are cheap to re-bootstrap.
        daVinciBackend.retain();
      }
    }
  }

  // Visible for testing
  public static DaVinciBackend getBackend() {
    synchronized (AvroGenericDaVinciClient.class) {
      return daVinciBackend.get();
    }
  }

  @Override
  public synchronized void start() {
    if (isReady()) {
      return;
    }
    logger.info("Starting client, storeName=" + getStoreName());
    VeniceConfigLoader configLoader = buildVeniceConfig();
    Optional<ObjectCacheConfig> cacheConfig = Optional.ofNullable(daVinciConfig.getCacheConfig());
    initBackend(clientConfig, configLoader, managedClients, icProvider, cacheConfig);

    try {
      if (!getBackend().compareCacheConfig(cacheConfig)) {
        throw new VeniceClientException("Cache config conflicts with existing backend, storeName=" + getStoreName());
      }

      if (daVinciConfig.isCacheEnabled()) {
        cacheBackend = getBackend().getObjectCache();
      }

      storeBackend = getBackend().getStoreOrThrow(getStoreName());
      if (managedClients.isPresent()) {
        storeBackend.setManaged(daVinciConfig.isManaged());
      }

      Schema keySchema = getBackend().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
      keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema, false);
      keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);

      if (isVeniceQueryAllowed()) {
        veniceClient = (AvroGenericReadComputeStoreClient<K, V>) getAndStartAvroClient(clientConfig);
      }

      ready.set(true);
      logger.info("Client is started successfully, storeName=" + getStoreName());
    } catch (Throwable e) {
      String msg = "Unable to start Da Vinci client, storeName=" + getStoreName();
      logger.error(msg, e);
      daVinciBackend.release();
      throw new VeniceClientException(msg, e);
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
      if (cacheBackend != null) {
        cacheBackend.close();
      }
      daVinciBackend.release();
      logger.info("Client is closed successfully, storeName=" + getStoreName());
    } catch (Throwable e) {
      String msg = "Unable to close Da Vinci client, storeName=" + getStoreName();
      logger.error(msg, e);
      throw new VeniceClientException(msg, e);
    }
  }
}

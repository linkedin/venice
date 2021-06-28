package com.linkedin.davinci.client;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.AvroComputeRequestBuilderV3;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ComputeUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.client.store.ClientFactory.*;


public class AvroGenericDaVinciClient<K, V> implements DaVinciClient<K, V>, AvroGenericReadComputeStoreClient<K,V> {
  protected final Logger logger = Logger.getLogger(getClass());

  private static class ReusableObjects {
    final ByteBuffer rawValue = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(new byte[16], null);
    final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private static final int REUSABLE_MAP_CAPACITY = 100;
    private static final float REUSABLE_MAP_LOAD_FACTOR = 0.75f;
    // LRU cache for storing schema->record map for object reuse of value and result record
    final LinkedHashMap<Schema, GenericRecord>
        reuseValueRecordMap = new LinkedHashMap<Schema, GenericRecord>(REUSABLE_MAP_CAPACITY, REUSABLE_MAP_LOAD_FACTOR, true){
      protected boolean removeEldestEntry(Map.Entry <Schema, GenericRecord> eldest) {
        return size() > REUSABLE_MAP_CAPACITY;
      }
    };
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
  private AvroGenericReadComputeStoreClient<K, V> veniceClient;
  private StoreBackend storeBackend;
  private static ReferenceCounted<DaVinciBackend> daVinciBackend;
  private ObjectCacheBackend cacheBackend;
  private static final Map<CharSequence, Schema> computeResultSchemaCache = new VeniceConcurrentHashMap<>();

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
    if (isOnHeapCacheEnabled()) {
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


  // TODO: This is 'almost' the same logic for the batchGet path.  We could probably wrap this function and adapt it to the
  // batchget api (where sometimes the batchget is just for a single key).  Advantages would be to remove duplicate code.
  private CompletableFuture<V> readFromLocalStorage(K key, V reusableValue) {
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
      int partition = versionBackend.getPartition(keyBytes);

      if (isPartitionReadyToServe(versionBackend, partition)) {
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
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.get(key);
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }

      if (isOnHeapCacheEnabled()) {
        return cacheBackend.get(key, versionBackend.getVersion(), (k, executor) -> this.readFromLocalStorage(k, null));
      } else {
        return readFromLocalStorage(key, reusableValue);
      }
    }
  }

  CompletableFuture<Map<K,V>> batchGetFromLocalStorage(Iterable<K> keys) {
    // expose underlying getAll functionality.
    Map<K, V> result = new HashMap<>();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (versionBackend == null) {
        if (isVeniceQueryAllowed()) {
          return veniceClient.batchGet(new HashSet<>((Collection<K>) keys));
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }
      Set<K> missingKeys = new HashSet<>();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      for (K key : keys) {
        byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
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
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (isOnHeapCacheEnabled()) {
        return cacheBackend.getAll(keys, versionBackend.getVersion(), (ks) -> {
          try {
            return batchGetFromLocalStorage(ks).get();
          } catch (InterruptedException|ExecutionException e) {
            throw new VeniceClientException("Error performing batch get while loading cache!!", e);
          }
        },(k, executor) -> this.readFromLocalStorage(k, null));
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
  public ComputeRequestBuilder<K> compute(Optional<ClientStats> stats, Optional<ClientStats> streamingStats, final long preRequestTimeInNS){
    return new AvroComputeRequestBuilderV3<>(getLatestValueSchema(), this, stats, streamingStats);
  }

  @Override
  public CompletableFuture<Map<K, GenericRecord>> compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys,
      Schema resultSchema, Optional<ClientStats> stats, long preRequestTimeInNS) throws VeniceClientException {
    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (null == versionBackend){
        if (isVeniceQueryAllowed()){
          return veniceClient.compute(computeRequestWrapper, keys, resultSchema, stats, preRequestTimeInNS);
        }
        storeBackend.getStats().recordBadRequest();
        throw new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName());
      }

      Map<K, GenericRecord> result = new HashMap<>(keys.size());
      Set<K> missingKeys = new HashSet<>();

      ReusableObjects reusableObjects = threadLocalReusableObjects.get();

      Schema valueSchema = computeRequestWrapper.getValueSchema();
      GenericRecord reuseValueRecord = reusableObjects.reuseValueRecordMap.computeIfAbsent(valueSchema, k -> new GenericData.Record(valueSchema));

      Map<String, Object> globalContext = new HashMap<>();
      Schema computeResultSchema = getComputeResultSchema(computeRequestWrapper);
      for (K key : keys) {
        byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
        int partition = versionBackend.getPartition(keyBytes);

        if (isPartitionReadyToServe(versionBackend, partition)) {
          GenericRecord computeResultValue =
              versionBackend.compute(partition, keyBytes, getGenericRecordChunkingAdapter(), reusableObjects.binaryDecoder,
                  reusableObjects.rawValue, reuseValueRecord, globalContext, computeRequestWrapper, computeResultSchema);

          if (null != computeResultValue) {
            result.put(key, computeResultValue);
          }
        } else if (isVeniceQueryAllowed()){
          missingKeys.add(key);
        } else if (!isPartitionSubscribed(versionBackend, partition)){
          storeBackend.getStats().recordBadRequest();
          throw new NonLocalAccessException(versionBackend.toString(), partition);
        }
      }

      if (missingKeys.isEmpty()){
        return CompletableFuture.completedFuture(result);
      }

      return veniceClient.compute(computeRequestWrapper, missingKeys, resultSchema, stats, preRequestTimeInNS)
          .thenApply(veniceResult -> {
            result.putAll(veniceResult);
            return result;
          });
    }
  }

  private Schema getComputeResultSchema(ComputeRequestWrapper computeRequestWrapper){
    // try to get the result schema from the cache
    CharSequence computeResultSchemaStr = computeRequestWrapper.getResultSchemaStr();
    Schema computeResultSchema = computeResultSchemaCache.get(computeResultSchemaStr);
    if (computeResultSchema == null) {
      computeResultSchema = Schema.parse(computeResultSchemaStr.toString());
      // sanity check on the result schema
      ComputeUtils.checkResultSchema(computeResultSchema, computeRequestWrapper.getValueSchema(), computeRequestWrapper.getComputeRequestVersion(), computeRequestWrapper.getOperations());
      computeResultSchemaCache.putIfAbsent(computeResultSchemaStr, computeResultSchema);
    }
    return computeResultSchema;
  }

  @Override
  public void compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys, Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback, long preRequestTimeInNS) throws VeniceClientException {
    compute(computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS, null, null);
  }

  @Override
  public void compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys, Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback, long preRequestTimeInNS, BinaryEncoder reusedEncoder,
      ByteArrayOutputStream reusedOutputStream) throws VeniceClientException {

    if (handleCallbackForEmptyKeySet(keys, callback)){
      return;
    }

    throwIfNotReady();
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (null == versionBackend) {
        if (isVeniceQueryAllowed()) {
          veniceClient.compute(computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS,
              reusedEncoder, reusedOutputStream);
          return;
        }
        storeBackend.getStats().recordBadRequest();
        callback.onCompletion(Optional.of(new VeniceClientException("Da Vinci client is not subscribed, storeName=" + getStoreName())));
        return;
      }

      Set<K> missingKeys = new HashSet<>();

      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      Schema valueSchema = computeRequestWrapper.getValueSchema();
      GenericRecord reuseValueRecord = reusableObjects.reuseValueRecordMap.computeIfAbsent(valueSchema, k -> new GenericData.Record(valueSchema));

      Map<String, Object> globalContext = new HashMap<>();
      Schema computeResultSchema = getComputeResultSchema(computeRequestWrapper);

      for (K key : keys) {
        byte[] keyBytes = keySerializer.serialize(key, reusableObjects.binaryEncoder, reusableObjects.byteArrayOutputStream);
        int partition = versionBackend.getPartition(keyBytes);

        if (isPartitionReadyToServe(versionBackend, partition)) {
          GenericRecord computeResultValue =
              versionBackend.compute(partition, keyBytes, getGenericRecordChunkingAdapter(), reusableObjects.binaryDecoder,
                  reusableObjects.rawValue, reuseValueRecord, globalContext, computeRequestWrapper, computeResultSchema);

            callback.onRecordReceived(key, computeResultValue);
        } else if (isVeniceQueryAllowed()){
          missingKeys.add(key);
        } else if (!isPartitionSubscribed(versionBackend, partition)){
          storeBackend.getStats().recordBadRequest();
          callback.onCompletion(Optional.of(new NonLocalAccessException(versionBackend.toString(), partition)));
          return;
        }
      }

      if (missingKeys.isEmpty()){
        callback.onCompletion(Optional.empty());
        return;
      }

      veniceClient.compute(computeRequestWrapper, missingKeys, resultSchema, callback, preRequestTimeInNS,
          reusedEncoder, reusedOutputStream);
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

  protected boolean isOnHeapCacheEnabled() {
    return daVinciConfig.isHeapObjectCacheEnabled();
  }

  private void dropAllCachePartitions() {
    try (ReferenceCounted<VersionBackend> versionRef = storeBackend.getCurrentVersion()) {
      VersionBackend versionBackend = versionRef.get();
      if (null != versionBackend) {
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
    return GenericChunkingAdapter.INSTANCE;
  }

  protected GenericRecordChunkingAdapter getGenericRecordChunkingAdapter() {
    return GenericRecordChunkingAdapter.INSTANCE;
  }

  private D2ServiceDiscoveryResponseV2 discoverService() {
    try (TransportClient client = getTransportClient(clientConfig)) {
      if (!(client instanceof D2TransportClient)) {
        throw new VeniceClientException("Venice service discovery requires D2 client" +
                                            ", storeName=" + getStoreName() +
                                            ", clientClass=" + client.getClass());
      }
      D2ServiceDiscovery serviceDiscovery = new D2ServiceDiscovery();
      D2ServiceDiscoveryResponseV2 response = serviceDiscovery.discoverD2Service((D2TransportClient) client, getStoreName());
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
            .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED,
                daVinciConfig.getStorageClass() == StorageClass.MEMORY_BACKED_BY_DISK)
            .put(INGESTION_USE_DA_VINCI_CLIENT, true)
            .build();
    logger.info("backendConfig=" + config.toString(true));
    return new VeniceConfigLoader(config, config);
  }


  private static synchronized void initBackend(ClientConfig clientConfig, VeniceConfigLoader configLoader,
      Optional<Set<String>> managedClients, ICProvider icProvider, Optional<ObjectCacheConfig> cacheBackend) {
    Logger staticLogger = Logger.getLogger(AvroGenericDaVinciClient.class);
    if (daVinciBackend == null) {
      staticLogger.info("Da Vinci Backend does not exist, initializing it for client: " + clientConfig.getStoreName());
      daVinciBackend = new ReferenceCounted<>(new DaVinciBackend(clientConfig, configLoader, managedClients, icProvider, cacheBackend),
          backend -> {
            daVinciBackend = null;
            backend.close();
          });
    } else if (VeniceSystemStoreType.getSystemStoreType(clientConfig.getStoreName()) != VeniceSystemStoreType.META_STORE) {
      staticLogger.info("Da Vinci Backend exists, reusing exsiting backend for client: " + clientConfig.getStoreName());
      // Do not increment DaVinciBackend reference count for meta system store da-vinci clients. Once the last user da-vinci
      // client is released the backend can be safely deleted since meta system stores are meaningless without user stores
      // and they are cheap to re-bootstrap.
      daVinciBackend.retain();
    }
    if(!daVinciBackend.get().compareCacheSettings(cacheBackend)) {
      throw new VeniceClientException("Cannot initialize multiple clients with different cache settings!!");
    }
  }

  // Visible for testing
  public static synchronized DaVinciBackend getBackend() {
    return daVinciBackend.get();
  }

  @Override
  public synchronized void start() {
    if (isReady()) {
      throw new VeniceClientException("Da Vinci client is already started, storeName=" + getStoreName());
    }
    logger.info("Starting client, storeName=" + getStoreName());
    VeniceConfigLoader configLoader = buildVeniceConfig();
    initBackend(clientConfig, configLoader, managedClients, icProvider, isOnHeapCacheEnabled() ? Optional.of(daVinciConfig.getCacheConfig()):Optional.empty());
    if (isOnHeapCacheEnabled()) {
      cacheBackend = daVinciBackend.get().getObjectCache();
    }

    try {
      storeBackend = getBackend().getStoreOrThrow(getStoreName());
      if (managedClients.isPresent()) {
        storeBackend.setManaged(daVinciConfig.isManaged());
      }
      storeBackend.setMemoryLimit(daVinciConfig.getMemoryLimit());

      Schema keySchema = getBackend().getSchemaRepository().getKeySchema(getStoreName()).getSchema();
      keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema, false);

      if (isVeniceQueryAllowed()) {
        veniceClient = (AvroGenericReadComputeStoreClient<K, V>) getAndStartAvroClient(clientConfig);
      }

      ready.set(true);
      logger.info("Client is started successfully, storeName=" + getStoreName());
    } catch (Throwable e) {
      logger.info("Failed to initialize Da Vinci Backend, releasing it now. ", e);
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
      logger.info("Releasing reference of Da Vinci backend, called from: " + Arrays.toString(
          Thread.currentThread().getStackTrace()));
      daVinciBackend.release();
      logger.info("Client is closed successfully, storeName=" + getStoreName());
      if (cacheBackend != null) {
        cacheBackend.close();
      }
    } catch (Throwable e) {
      throw new VeniceClientException("Unable to close Da Vinci client, storeName=" + getStoreName(), e);
    }
  }
}

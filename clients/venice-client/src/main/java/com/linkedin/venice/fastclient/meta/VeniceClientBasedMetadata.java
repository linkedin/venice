package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_PARTITION_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_VERSION_NUMBER;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A store metadata abstraction that uses Venice clients to fetch the metadata and keep the local cache updated. There
 * are currently two types implementations/clients for this. One is using a DaVinci client based metadata (deprecating
 * it due to cyclic dependency when the application is also running DVCs) and a thin client based metadata.
 */
public abstract class VeniceClientBasedMetadata extends AbstractStoreMetadata {
  private static final Logger LOGGER = LogManager.getLogger(VeniceClientBasedMetadata.class);
  private static final long ZSTD_DICT_FETCH_TIMEOUT = 10;
  private static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final long INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS = 30;
  private static final long RETRY_WAIT_TIME_IN_MS = 1000;
  private static final String STORE_PROPERTIES_KEY = "store_properties";
  private static final String STORE_KEY_SCHEMAS_KEY = "store_key_schemas";
  private static final String STORE_VALUE_SCHEMAS_KEY = "store_value_schemas";
  private static final String VERSION_PARTITION_SEPARATOR = "_";

  private final long refreshIntervalInSeconds;
  private final Map<String, StoreMetaKey> storeMetaKeyMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final AtomicInteger currentVersion = new AtomicInteger();
  private final AtomicInteger latestSuperSetValueSchemaId = new AtomicInteger();
  private final AtomicReference<SchemaData> schemas = new AtomicReference<>();
  // A map of version partition string to a list of ready to serve instances.
  private final Map<String, List<String>> readyToServeInstancesMap = new VeniceConcurrentHashMap<>();
  // Map of version number to the corresponding partitioner. Partitioner is immutable per Venice store version.
  private final Map<Integer, VenicePartitioner> versionPartitionerMap = new VeniceConcurrentHashMap<>();
  // Map of version number to the corresponding partition count. Partition count is immutable per Venice store version.
  private final AtomicReference<Int2IntMap> versionPartitionCountMap = new AtomicReference<>();
  // Only used in updateCache for evicting purposes.
  private final Map<Integer, ByteBuffer> versionZstdDictionaryMap = new VeniceConcurrentHashMap<>();
  private final CompressorFactory compressorFactory;
  private final TransportClient transportClient;

  private String clusterName;
  private ClusterStats clusterStats;

  protected VeniceClientBasedMetadata(ClientConfig clientConfig) {
    super(clientConfig);
    this.refreshIntervalInSeconds = clientConfig.getMetadataRefreshIntervalInSeconds() > 0
        ? clientConfig.getMetadataRefreshIntervalInSeconds()
        : DEFAULT_REFRESH_INTERVAL_IN_SECONDS;
    this.storeMetaKeyMap
        .put(STORE_KEY_SCHEMAS_KEY, MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
          }
        }));
    this.storeMetaKeyMap.put(
        STORE_VALUE_SCHEMAS_KEY,
        MetaStoreDataType.STORE_VALUE_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
          }
        }));
    this.transportClient = new R2TransportClient(clientConfig.getR2Client());
    this.compressorFactory = new CompressorFactory();
    this.clusterStats = clientConfig.getClusterStats();
  }

  @Override
  public int getCurrentStoreVersion() {
    return currentVersion.get();
  }

  @Override
  public int getPartitionId(int version, ByteBuffer key) {
    VenicePartitioner partitioner = versionPartitionerMap.get(version);
    if (partitioner == null || !versionPartitionCountMap.get().containsKey(version)) {
      throw new VeniceClientException("Unknown version number: " + version + " for store: " + storeName);
    }
    return partitioner.getPartitionId(key, versionPartitionCountMap.get().get(version));
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    return readyToServeInstancesMap
        .getOrDefault(getVersionPartitionMapKey(version, partitionId), Collections.emptyList());
  }

  @Override
  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
    return getCompressor(compressionStrategy, version, compressorFactory, versionZstdDictionaryMap);
  }

  @Override
  public void start() {
    long initializationTimeout =
        System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS);
    while (true) {
      try {
        updateCache();
        break;
      } catch (MissingKeyInStoreMetadataException e) {
        // Retry until timeout to prevent corner case where the fast client is started right after meta system store is
        // just initialized.
        if (System.currentTimeMillis() > initializationTimeout) {
          throw e;
        }
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    // It might be helpful to add a random delay to offset the schema-refresh. However, this might be a premature
    // optimization since given a large-enough fleet, the requests should get distributed inherently
    scheduler.scheduleAtFixedRate(this::refresh, refreshIntervalInSeconds, refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  @Override
  public Schema getKeySchema() {
    return schemas.get().getKeySchema().getSchema();
  }

  @Override
  public Schema getValueSchema(int id) {
    return schemas.get().getValueSchema(id).getSchema();
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    SchemaEntry schemaEntry = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema);
    return schemas.get().getSchemaID(schemaEntry);
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemas.get().getValueSchema(getLatestValueSchemaId()).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    int latestValueSchemaId = latestSuperSetValueSchemaId.get();
    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = schemas.get().getMaxValueSchemaId();
    }
    return latestValueSchemaId;
  }

  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    // Ideally, we can fetch this information from the SchemaData object, but we're not yet populating these schemas
    throw new VeniceUnsupportedOperationException("getUpdateSchema");
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    // Ideally, we can fetch this information from the SchemaData object, but we're not yet populating these schemas
    throw new VeniceUnsupportedOperationException("getLatestUpdateSchema");
  }

  @Override
  public void close() throws IOException {
    super.close();
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
    readyToServeInstancesMap.clear();
    versionPartitionerMap.clear();
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  private String getVersionPartitionMapKey(int version, int partition) {
    return version + VERSION_PARTITION_SEPARATOR + partition;
  }

  /**
   * Current version should only be updated after readyToServeInstanceMap is updated to avoid race condition between
   * updateCache() and getReplicas. In theory, previous requests with an older version number could get null while we
   * are evicting entries from the readyToServeInstanceMap but that's very unlikely since we always keep at least two
   * versions before evicting old entries.
   */
  private synchronized void updateCache() {
    // Cluster discovery in case the store migrated to another Venice cluster.
    clusterName = getStoreMetaValue(
        MetaStoreDataType.STORE_CLUSTER_CONFIG
            .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName))).storeClusterConfig.cluster
                .toString();
    storeMetaKeyMap
        .put(STORE_PROPERTIES_KEY, MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
          }
        }));
    StoreProperties storeProperties = getStoreMetaValue(storeMetaKeyMap.get(STORE_PROPERTIES_KEY)).storeProperties;
    Int2IntMap newVersionPartitionCountMap = new Int2IntOpenHashMap(storeProperties.versions.size());
    // Update partitioner pair map
    IntList zstdDictionaryFetchVersions = new IntArrayList();
    for (StoreVersion v: storeProperties.versions) {
      newVersionPartitionCountMap.put(v.number, v.partitionCount);
      versionPartitionerMap.computeIfAbsent(v.number, k -> {
        StorePartitionerConfig partitionerConfig = v.partitionerConfig;
        Properties params = new Properties();
        params.putAll(partitionerConfig.partitionerParams);
        return PartitionUtils.getVenicePartitioner(
            partitionerConfig.partitionerClass.toString(),
            partitionerConfig.amplificationFactor,
            new VeniceProperties(params));
      });

      if (CompressionStrategy.valueOf(v.compressionStrategy).equals(CompressionStrategy.ZSTD_WITH_DICT)
          && !versionZstdDictionaryMap.containsKey(v.number)) {
        zstdDictionaryFetchVersions.add(v.number); // versions with no dictionary available
      }
    }
    // Update readyToServeInstanceMap
    for (Int2IntMap.Entry entry: newVersionPartitionCountMap.int2IntEntrySet()) {
      // Assumes partitionId is 0 based
      for (int i = 0; i < entry.getIntValue(); i++) {
        final int partitionId = i;
        String key = getVersionPartitionMapKey(entry.getIntKey(), partitionId);
        try {
          readyToServeInstancesMap.compute(key, (k, v) -> getReadyToServeReplicas(entry.getIntKey(), partitionId));
        } catch (MissingKeyInStoreMetadataException e) {
          // Ignore MissingKeyInStoreMetadataException since a new version may not have replica assignment for all
          // partitions yet. We still want to fetch assignment for all known versions all the time since the refresh is
          // asynchronous to reads and non-blocking. Meaning current version can change in the middle of a refresh.
          LOGGER.info(
              "No replica info available in meta system store yet for version: {} partition: {}. This is normal if this is a new version",
              Version.composeKafkaTopic(storeName, entry.getIntKey()),
              partitionId);
        }
      }
    }
    // Update schemas TODO consider update in place with additional checks to skip existing schemas for better
    // performance if it's thread safe.
    Map.Entry<CharSequence, CharSequence> keySchemaEntry =
        getStoreMetaValue(storeMetaKeyMap.get(STORE_KEY_SCHEMAS_KEY)).storeKeySchemas.keySchemaMap.entrySet()
            .iterator()
            .next();
    SchemaData schemaData = new SchemaData(storeName);
    schemaData.setKeySchema(
        new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString()));
    Map<CharSequence, CharSequence> valueSchemaMap =
        getStoreMetaValue(storeMetaKeyMap.get(STORE_VALUE_SCHEMAS_KEY)).storeValueSchemas.valueSchemaMap;
    for (Map.Entry<CharSequence, CharSequence> entry: valueSchemaMap.entrySet()) {
      Map<String, String> keyMap = new HashMap<>(2);
      keyMap.put(KEY_STRING_STORE_NAME, storeName);
      keyMap.put(KEY_STRING_SCHEMA_ID, entry.getKey().toString());
      StoreMetaKey individualValueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(keyMap);
      String valueSchema = getStoreMetaValue(individualValueSchemaKey).storeValueSchema.valueSchema.toString();
      schemaData.addValueSchema(new SchemaEntry(Integer.parseInt(entry.getKey().toString()), valueSchema));
    }
    schemas.set(schemaData);

    CompletableFuture<TransportClientResponse>[] dictionaryFetchFutures =
        new CompletableFuture[zstdDictionaryFetchVersions.size()];
    for (int i = 0; i < zstdDictionaryFetchVersions.size(); i++) {
      dictionaryFetchFutures[i] = fetchCompressionDictionary(zstdDictionaryFetchVersions.getInt(i));
    }

    // Evict old entries
    if (versionPartitionCountMap.get() != null) {
      for (Int2IntMap.Entry oldEntry: versionPartitionCountMap.get().int2IntEntrySet()) {
        if (!newVersionPartitionCountMap.containsKey(oldEntry.getIntKey())) {
          versionPartitionerMap.remove(oldEntry.getIntKey());
          versionZstdDictionaryMap.remove(oldEntry.getIntKey());
          for (int i = 0; i < oldEntry.getIntValue(); i++) {
            readyToServeInstancesMap.remove(getVersionPartitionMapKey(oldEntry.getIntKey(), i));
          }
        }
      }
    }
    versionPartitionCountMap.set(newVersionPartitionCountMap);

    /** Wait for all dictionary fetches to complete before we finish the refresh. Its possible that the fetch
     jobs return error or we get a timeout. In which case the next refresh will start another job so
     we don't really need to retry again */
    try {
      if (dictionaryFetchFutures.length > 0) {
        CompletableFuture.allOf(dictionaryFetchFutures).get(ZSTD_DICT_FETCH_TIMEOUT, TimeUnit.SECONDS);
      }
      currentVersion.set(storeProperties.currentVersion);
      clusterStats.updateCurrentVersion(currentVersion.get());
      latestSuperSetValueSchemaId.set(storeProperties.latestSuperSetValueSchemaId);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new VeniceClientException("Dictionary fetch operation was interrupted");
    } catch (ExecutionException | TimeoutException e) {
      LOGGER.warn(
          "Dictionary fetch operation could not complete in time for some of the versions. "
              + "Will be retried on next refresh",
          e);
      clusterStats.recordVersionUpdateFailure();
    }
  }

  private void refresh() {
    try {
      updateCache();
    } catch (Exception e) {
      // Catch all errors so periodic refresh doesn't break on transient errors.
      LOGGER.error("Encountered unexpected error during metadata refresh for store {}", storeName, e);
    }
  }

  // Fetch the zstd dictionary for this version
  private CompletableFuture<TransportClientResponse> fetchCompressionDictionary(int version) {
    CompletableFuture<TransportClientResponse> compressionDictionaryFuture = new CompletableFuture<>();
    // Assumption: Every version has partition 0 and available in the map.
    String versionPartitionMapKey = getVersionPartitionMapKey(version, 0);
    if (!readyToServeInstancesMap.containsKey(versionPartitionMapKey)) {
      compressionDictionaryFuture.completeExceptionally(
          new IllegalStateException(
              String.format("Attempt to fetch compression dictionary for unknown version %d", version)));
    }
    List<String> routes = readyToServeInstancesMap.get(versionPartitionMapKey);
    if (routes.size() == 0) {
      compressionDictionaryFuture.completeExceptionally(
          new IllegalStateException(
              String.format("No route found for store:%s version:%d partition:%d", storeName, version, 0)));
    }

    // Fetch from a random route from the available routes to hedge against a route being slow
    String route = routes.get(ThreadLocalRandom.current().nextInt(routes.size()));
    String url = route + "/" + QueryAction.DICTIONARY.toString().toLowerCase() + "/" + storeName + "/" + version;

    LOGGER.info("Fetching compression dictionary for version {} from URL {} ", version, url);
    transportClient.get(url).whenComplete((response, throwable) -> {
      if (throwable != null) {
        String message = String.format(
            "Problem fetching zstd compression dictionary from URL:%s for store:%s , version:%d",
            url,
            storeName,
            version);
        LOGGER.warn(message, throwable);
        compressionDictionaryFuture.completeExceptionally(throwable);
      } else {
        byte[] dictionary = response.getBody();
        versionZstdDictionaryMap.put(version, ByteBuffer.wrap(dictionary));
        compressionDictionaryFuture.complete(response);
      }
    });
    return compressionDictionaryFuture;
  }

  private List<String> getReadyToServeReplicas(int version, int partitionId) {
    StoreMetaKey replicaStatusesKey =
        MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
            put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
            put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
          }
        });
    return PushStatusDecider.getReadyToServeInstances(getStoreMetaValue(replicaStatusesKey).storeReplicaStatuses);
  }

  protected abstract StoreMetaValue getStoreMetaValue(StoreMetaKey key);
}

package com.linkedin.venice.fastclient.meta;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static java.lang.Thread.*;


/**
 * A wrapper with a DaVinci client subscribed to the corresponding Meta store to serve data required by {@link StoreMetadata}.
 * TODO All data are cached locally and refreshed periodically for performance reasons before either object cache becomes
 * available for meta system store or a decorator class of the underlying rocksDB classes is made available for consuming
 * deserialized meta system store data directly.
 */
public class DaVinciClientBasedMetadata extends AbstractStoreMetadata {
  private static final Logger logger = Logger.getLogger(DaVinciClientBasedMetadata.class);
  private static final String STORE_PROPERTIES_KEY = "store_properties";
  private static final String STORE_KEY_SCHEMAS_KEY = "store_key_schemas";
  private static final String STORE_VALUE_SCHEMAS_KEY = "store_value_schemas";
  private static final String VERSION_PARTITION_SEPARATOR = "_";
  private static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final long INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS = 30;
  private static final long RETRY_WAIT_TIME_IN_MS = 1000;

  private final long refreshIntervalInSeconds;
  private final Map<String, StoreMetaKey> storeMetaKeyMap = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final AtomicInteger currentVersion = new AtomicInteger();
  private final AtomicInteger latestSuperSetValueSchemaId = new AtomicInteger();
  private final AtomicReference<SchemaData> schemas = new AtomicReference<>();
  // A map of version partition string to a list of ready to serve instances.
  private final Map<String, List<String>> readyToServeInstancesMap = new VeniceConcurrentHashMap<>();
  // Map of version number to a pair which contains the corresponding partitioner and number of partitions. This is
  // cached since a version's partitioner and partition number are immutable.
  private final Map<Integer, Pair<VenicePartitioner, Integer>> versionPartitionerMap = new VeniceConcurrentHashMap<>();
  private final DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClient;
  // Only used in updateCache for evicting purposes.
  private Map<Integer, Integer> versionPartitionCountMap = new HashMap<>();

  private String clusterName;

  public DaVinciClientBasedMetadata(ClientConfig clientConfig) {
    super(clientConfig);
    if (null == clientConfig.getDaVinciClientForMetaStore()) {
      throw new VeniceClientException(
          "'DaVinciClientForMetaStore' should not be null in 'ClientConfig' when DaVinciClientBasedMetadata is being used.");
    }
    this.daVinciClient = clientConfig.getDaVinciClientForMetaStore();
    this.refreshIntervalInSeconds =
        clientConfig.getMetadataRefreshInvervalInSeconds() > 0 ? clientConfig.getMetadataRefreshInvervalInSeconds()
            : DEFAULT_REFRESH_INTERVAL_IN_SECONDS;
    this.storeMetaKeyMap.put(STORE_KEY_SCHEMAS_KEY,
        MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
        }}));
    this.storeMetaKeyMap.put(STORE_VALUE_SCHEMAS_KEY,
        MetaStoreDataType.STORE_VALUE_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
        }}));
  }

  @Override
  public int getCurrentStoreVersion() {
    return currentVersion.get();
  }

  @Override
  public int getPartitionId(int version, ByteBuffer key) {
    Pair<VenicePartitioner, Integer> partitionerPair = versionPartitionerMap.get(version);
    if (partitionerPair == null) {
      throw new VeniceClientException("Unknown version number: " + version + " for store: " + storeName);
    }
    return partitionerPair.getFirst().getPartitionId(key, partitionerPair.getSecond());
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    String key = getVersionPartitionMapKey(version, partitionId);
    return readyToServeInstancesMap.getOrDefault(key, Collections.emptyList());
  }

  @Override
  public void start() {
    try {
      daVinciClient.subscribeAll().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceClientException(
          "Failed to start the " + DaVinciClientBasedMetadata.class.getSimpleName() + " for store: " + storeName, e);
    }
    long timeoutTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS);
    while (true) {
      try {
        // Ensure the cache can be updated at least once before starting the periodic refresh.
        updateCache();
        break;
      } catch (MissingKeyInStoreMetadataException e) {
        if (System.currentTimeMillis() > timeoutTime) {
          throw e;
        }
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    scheduler.scheduleAtFixedRate(this::refresh, 0, refreshIntervalInSeconds, TimeUnit.SECONDS);
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

  private StoreMetaValue getStoreMetaValue(StoreMetaKey key) {
    try {
      StoreMetaValue value = daVinciClient.get(key).get();
      if (value == null) {
        throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
      }
      return value;
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceClientException(
          "Failed to get data from DaVinci client backed meta store for store: " + storeName);
    }
  }

  /**
   * Current version should only be updated after readyToServeInstanceMap to avoid race condition between updateCache() and
   * getReplicas. In theory, previous requests with an older version number could get null while we are evicting entries
   * from the readyToServeInstanceMap but that's very unlikely since we always keep at least two versions before evicting.
   */
  private synchronized void updateCache() {
    // Re-discover the venice cluster in case it's moved via store migration.
    clusterName = getStoreMetaValue(MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(
        Collections.singletonMap(KEY_STRING_STORE_NAME, storeName))).storeClusterConfig.cluster.toString();
    storeMetaKeyMap.put(STORE_PROPERTIES_KEY,
        MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
        }}));
    StoreProperties storeProperties = getStoreMetaValue(storeMetaKeyMap.get(STORE_PROPERTIES_KEY)).storeProperties;
    Map<Integer, Integer> newVersionPartitionCountMap = new HashMap<>();
    // Update partitioner pair map
    for (StoreVersion v : storeProperties.versions) {
      newVersionPartitionCountMap.put(v.number, v.partitionCount);
      versionPartitionerMap.computeIfAbsent(v.number, k -> {
        StorePartitionerConfig partitionerConfig = v.partitionerConfig;
        Properties params = new Properties();
        params.putAll(partitionerConfig.partitionerParams);
        VenicePartitioner partitioner =
            PartitionUtils.getVenicePartitioner(partitionerConfig.partitionerClass.toString(),
                partitionerConfig.amplificationFactor, new VeniceProperties(params));
        return new Pair<>(partitioner, v.partitionCount);
      });
    }
    // Update readyToServeInstanceMap
    for (Map.Entry<Integer, Integer> entry : newVersionPartitionCountMap.entrySet()) {
      // Assumes partitionId is 0 based
      for (int i = 0; i < entry.getValue(); i++) {
        final int partitionId = i;
        String key = getVersionPartitionMapKey(entry.getKey(), partitionId);
        readyToServeInstancesMap.compute(key, (k, v) -> getReadyToServeReplicas(entry.getKey(), partitionId));
      }
    }
    // Update schemas TODO consider update in place with additional checks to skip existing schemas for better performance if it's thread safe.
    Map.Entry<CharSequence, CharSequence> keySchemaEntry =
        getStoreMetaValue(storeMetaKeyMap.get(STORE_KEY_SCHEMAS_KEY)).storeKeySchemas.keySchemaMap.entrySet()
            .iterator()
            .next();
    SchemaData schemaData = new SchemaData(storeName);
    schemaData.setKeySchema(
        new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString()));
    Map<CharSequence, CharSequence> valueSchemaMap =
        getStoreMetaValue(storeMetaKeyMap.get(STORE_VALUE_SCHEMAS_KEY)).storeValueSchemas.valueSchemaMap;
    for (Map.Entry<CharSequence, CharSequence> entry : valueSchemaMap.entrySet()) {
      if (entry.getValue().toString().isEmpty()) {
        // The value schemas might be too large to be stored in a single K/V.
        StoreMetaKey individualValueSchemaKey =
            MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {{
              put(KEY_STRING_STORE_NAME, storeName);
              put(KEY_STRING_SCHEMA_ID, entry.getKey().toString());
            }});
        String valueSchema =
            getStoreMetaValue(individualValueSchemaKey).storeValueSchema.valueSchema.toString();
        schemaData.addValueSchema(new SchemaEntry(Integer.parseInt(entry.getKey().toString()), valueSchema));
      } else {
        schemaData.addValueSchema(
            new SchemaEntry(Integer.parseInt(entry.getKey().toString()), entry.getValue().toString()));
      }
    }
    schemas.set(schemaData);
    // Update current version
    currentVersion.set(storeProperties.currentVersion);
    latestSuperSetValueSchemaId.set(storeProperties.latestSuperSetValueSchemaId);

    // Evict old entries
    for (Map.Entry<Integer, Integer> oldEntry : versionPartitionCountMap.entrySet()) {
      if (!newVersionPartitionCountMap.containsKey(oldEntry.getKey())) {
        versionPartitionerMap.remove(oldEntry.getKey());
        for (int i = 0; i < oldEntry.getValue(); i++) {
          readyToServeInstancesMap.remove(getVersionPartitionMapKey(oldEntry.getKey(), i));
        }
      }
    }
    versionPartitionCountMap = newVersionPartitionCountMap;
  }

  private String getVersionPartitionMapKey(int version, int partition) {
    return version + VERSION_PARTITION_SEPARATOR + partition;
  }

  private void refresh() {
    try {
      updateCache();
    } catch (Exception e) {
      // Catch all errors so periodic refresh doesn't break on transient errors.
      logger.error("Encountered unexpected error during refresh", e);
    }
  }

  private List<String> getReadyToServeReplicas(int version, int partitionId) {
    StoreMetaKey replicaStatusesKey =
        MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
          put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
          put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
        }});
    return PushStatusDecider.getReadyToServeInstances(getStoreMetaValue(replicaStatusesKey).storeReplicaStatuses);
  }

  private CharSequence getLargestValueSchemaId(Map<CharSequence, CharSequence> valueSchemaMap) {
    List<CharSequence> keyList = new ArrayList<>(valueSchemaMap.keySet());
    keyList.sort((o1, o2) -> {
      Integer i1 = Integer.parseInt(o1.toString());
      Integer i2 = Integer.parseInt(o2.toString());
      return i1.compareTo(i2);
    });
    return keyList.get(keyList.size() - 1);
  }

  @Override
  public void close() throws IOException {
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
    versionPartitionCountMap.clear();
  }
}

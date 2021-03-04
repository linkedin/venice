package com.linkedin.davinci.repository;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;


/**
 * This implementation uses DaVinci client backed meta system store to provide data to the {@link NativeMetadataRepository}.
 * The data is then cached and served from there.
 */
public class DaVinciClientMetaStoreBasedRepository extends NativeMetadataRepository {
  private static final int KEY_SCHEMA_ID = 1;
  private static final long OFFSET_LAG_THRESHOLD_FOR_META_STORE = 1;
  private static final long DEFAULT_DA_VINCI_CLIENT_ROCKS_DB_MEMORY_LIMIT = 1024 * 1024 * 1024; // 1GB
  private static final String MOCKED_PUSH_ID = "mocked_system_store_push_id";
  private static final Logger logger = Logger.getLogger(DaVinciClientMetaStoreBasedRepository.class);

  // Map of user store name to their corresponding meta store which is used for finding the correct current version.
  // TODO Store objects in this map are mocked locally based on client.meta.system.store.version.map config.

  // A map of user store name to their corresponding meta store current version provided via configs.
  private final Map<String, Integer> metaStoreVersions = new VeniceConcurrentHashMap<>();

  // A map of user store name to their corresponding daVinci client of the meta store.
  private final Map<String, DaVinciClient<StoreMetaKey, StoreMetaValue>> daVinciClientMap =
      new VeniceConcurrentHashMap<>();

  // A map of mocked meta Store objects. Keep the meta stores separately from SystemStoreBasedRepository.subscribedStoreMap
  // because these meta stores doesn't require refreshes.
  private final Map<String, Store> metaStoreMap = new VeniceConcurrentHashMap<>();

  private final DaVinciConfig daVinciConfig = new DaVinciConfig();
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final Schema.Parser schemaParser = new Schema.Parser();
  private final SchemaReader metaStoreSchemaReader;

  public DaVinciClientMetaStoreBasedRepository(ClientConfig clientConfig, VeniceProperties backendConfig,
      Map<String, String> metaStoreVersions) {
    super(clientConfig, backendConfig);
    this.metaStoreVersions.putAll(metaStoreVersions.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue()))));
    daVinciClientFactory = new CachingDaVinciClientFactory(clientConfig.getD2Client(),
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository("davinci-client")), backendConfig);
    daVinciConfig.setMemoryLimit(DEFAULT_DA_VINCI_CLIENT_ROCKS_DB_MEMORY_LIMIT);
    ClientConfig clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
        .setStoreName(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName())
        .setSpecificValueClass(StoreMetaValue.class);
    metaStoreSchemaReader = ClientFactory.getSchemaReader(clonedClientConfig);
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
    StoreMetaValue value;
    try {
      value = getDaVinciClientForMetaStore(storeName).get(key).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException(
          "Failed to get data from DaVinci client backed meta store for store: " + storeName + " with key: "
              + key.toString());
    }
    if (value == null) {
      throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
    }
    return value;
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return new SchemaEntry(KEY_SCHEMA_ID, metaStoreSchemaReader.getKeySchema());
    } else {
      return super.getKeySchema(storeName);
    }
  }

  @Override
  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Schema schema = metaStoreSchemaReader.getValueSchema(id);
      return schema == null ? null : new SchemaEntry(id, schema);
    } else {
      return super.getValueSchemaInternally(storeName, id);
    }
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return metaStoreSchemaReader.getValueSchemaId(schemaParser.parse(valueSchemaStr));
    } else {
      return super.getValueSchemaId(storeName, valueSchemaStr);
    }
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      throw new UnsupportedOperationException("getValueSchemas not supported for store: " + storeName);
    } else {
      return super.getValueSchemas(storeName);
    }
  }

  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return new SchemaEntry(metaStoreSchemaReader.getLatestValueSchemaId(),
          metaStoreSchemaReader.getLatestValueSchema());
    } else {
      return super.getLatestValueSchema(storeName);
    }
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      String regularStoreName = VeniceSystemStoreType.META_STORE.extractRegularStoreName(storeName);
      metaStoreMap.computeIfAbsent(storeName, k -> getMetaStore(storeName, regularStoreName));
    } else {
      super.subscribe(storeName);
    }
  }

  @Override
  public Store getStore(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Store store = metaStoreMap.get(storeName);
      return store == null ? null : new ReadOnlyStore(store);
    } else {
      return super.getStore(storeName);
    }
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      Store store = metaStoreMap.get(storeName);
      if (store != null) {
        return new ReadOnlyStore(store);
      }
      throw new VeniceNoStoreException(storeName);
    } else {
      return super.getStoreOrThrow(storeName);
    }
  }

  @Override
  public boolean hasStore(String storeName) {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.META_STORE) {
      return metaStoreMap.containsKey(storeName);
    } else {
      return super.hasStore(storeName);
    }
  }

  @Override
  public void clear() {
    subscribedStoreMap.clear();
    daVinciClientFactory.close();
    daVinciClientMap.clear();
    metaStoreSchemaReader.close();
    subscribedStoreMap.clear();
  }

  @Override
  protected StoreConfig getStoreConfigFromSystemStore(String storeName) {
    return getStoreConfigFromMetaSystemStore(storeName);
  }

  @Override
  protected Store getStoreFromSystemStore(String storeName, String clusterName) {
    StoreProperties storeProperties =
        getStoreMetaValue(storeName, MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
        }})).storeProperties;
    return new ZKStore(storeProperties);
  }

  // TODO Get the meta store from an endpoint instead of mocking it locally based on provided configs.
  private Store getMetaStore(String metaStoreName, String regularStoreName) {
    Store mockedStore = new ZKStore(metaStoreName, "venice-system", 0, PersistenceType.ROCKS_DB, RoutingStrategy.HASH,
        ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1); // TODO: figure out how to get hold of a sensible RF value here
    mockedStore.setPartitionCount(VeniceSystemStoreUtils.DEFAULT_SYSTEM_STORE_PARTITION_COUNT);
    mockedStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(VeniceSystemStoreUtils.DEFAULT_SYSTEM_STORE_REWIND_SECONDS,
            OFFSET_LAG_THRESHOLD_FOR_META_STORE, TimeUnit.MINUTES.toSeconds(1)));
    mockedStore.setLeaderFollowerModelEnabled(true);
    mockedStore.setWriteComputationEnabled(true);
    int currentVersionNumber = DEFAULT_SYSTEM_STORE_CURRENT_VERSION;
    if (metaStoreVersions.containsKey(regularStoreName)) {
      currentVersionNumber = metaStoreVersions.get(regularStoreName);
    } else {
      logger.info("Unable to find corresponding meta store version for store: " + regularStoreName
          + ". Using the default value of: " + DEFAULT_SYSTEM_STORE_CURRENT_VERSION
          + " instead. Please use the config: " + ConfigKeys.CLIENT_META_SYSTEM_STORE_VERSION_MAP
          + " to specify if the default value doesn't work.");
    }
    Version currentVersion = new VersionImpl(metaStoreName, currentVersionNumber, MOCKED_PUSH_ID,
        VeniceSystemStoreUtils.DEFAULT_SYSTEM_STORE_PARTITION_COUNT);
    mockedStore.addVersion(currentVersion);
    mockedStore.setCurrentVersion(currentVersion.getNumber());
    return mockedStore;
  }

  @Override
  protected StoreMetadataValue getStoreMetadata(String storeName, StoreMetadataKey key)
      throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException(
        "getStoreMetadata for store: " + storeName + " and key: " + key.toString() + " is not supported in "
            + this.getClass().getSimpleName());
  }

  @Override
  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    return getSchemaDataFromMetaSystemStore(storeName);
  }

  private DaVinciClient<StoreMetaKey, StoreMetaValue> getDaVinciClientForMetaStore(String storeName) {
    return daVinciClientMap.computeIfAbsent(storeName, k -> {
      DaVinciClient<StoreMetaKey, StoreMetaValue> client = daVinciClientFactory.getAndStartSpecificAvroClient(
          VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), daVinciConfig, StoreMetaValue.class);
      try {
        client.subscribeAll().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException("Failed to construct DaVinci client for the meta store of store: " + storeName, e);
      }
      return client;
    });
  }
}

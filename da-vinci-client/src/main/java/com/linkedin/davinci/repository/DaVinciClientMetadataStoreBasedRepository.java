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
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;


/**
 * Implementation that uses DaVinci client to bootstrap the metadata system stores locally. This implementation will not
 * be used since it's deprecated by the new DaVinciClientMetaStoreBasedRepository implementation. The class should be
 * removed once DaVinciClientMetaStoreBasedRepository is verified in PROD.
 */
public class DaVinciClientMetadataStoreBasedRepository extends NativeMetadataRepository {

  private static final long OFFSET_LAG_THRESHOLD_FOR_METADATA_DA_VINCI_STORE = 1;
  private static final long DEFAULT_DA_VINCI_CLIENT_ROCKS_DB_MEMORY_LIMIT = 1024 * 1024 * 1024; // 1 GB
  private static final Logger logger = Logger.getLogger(DaVinciClientMetadataStoreBasedRepository.class);

  // Dummy stores that mocks the zk shared store and should only be used to bootstrap the corresponding current version.
  // TODO Add a server endpoint to retrieve the zk shared store configs and versions properly instead of relying on the
  // default configs and a predesignated version topic.
  private final Map<String, Store> metadataSystemStores = new VeniceConcurrentHashMap<>();
  private final Map<String, String> metadataSystemStoreVersion = new VeniceConcurrentHashMap<>();

  // SchemaReader used to retrieve schemas for the metadata system stores.
  private SchemaReader metadataStoreSchemaReader;
  // A map of user storeName to their corresponding metadata system store DaVinci client
  private final Map<String, DaVinciClient<StoreMetadataKey, StoreMetadataValue>> daVinciClientMap =
      new VeniceConcurrentHashMap<>();
  private final DaVinciConfig daVinciConfig = new DaVinciConfig();
  private final CachingDaVinciClientFactory daVinciClientFactory;

  public DaVinciClientMetadataStoreBasedRepository(ClientConfig<StoreMetadataValue> clientConfig,
      VeniceProperties backendConfig, Map<String, String> metadataSystemStoreVersion) {
    super(clientConfig, backendConfig);
    this.metadataSystemStoreVersion.putAll(metadataSystemStoreVersion);
    daVinciClientFactory = new CachingDaVinciClientFactory(clientConfig.getD2Client(),
        Optional.ofNullable(clientConfig.getMetricsRepository())
            .orElse(TehutiUtils.getMetricsRepository("davinci-client")), backendConfig);
    daVinciConfig.setMemoryLimit(DEFAULT_DA_VINCI_CLIENT_ROCKS_DB_MEMORY_LIMIT);
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      String veniceStoreName = VeniceSystemStoreUtils.getStoreNameFromSystemStoreName(storeName);
      metadataSystemStores.computeIfAbsent(storeName, k -> {
        Store store = new ZKStore(storeName, "venice-system", 0, PersistenceType.ROCKS_DB, RoutingStrategy.HASH,
            ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
            1); // TODO: figure out how to get hold of a sensible RF value here
        store.setPartitionCount(DEFAULT_SYSTEM_STORE_PARTITION_COUNT);
        // TODO time based lag threshold might be more suitable than offset based for system store use cases.
        store.setHybridStoreConfig(new HybridStoreConfigImpl(DEFAULT_SYSTEM_STORE_REWIND_SECONDS,
            OFFSET_LAG_THRESHOLD_FOR_METADATA_DA_VINCI_STORE, HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD));
        int currentVersionNumber = DEFAULT_SYSTEM_STORE_CURRENT_VERSION;
        if (metadataSystemStoreVersion.containsKey(veniceStoreName)) {
          currentVersionNumber = Integer.parseInt(metadataSystemStoreVersion.get(veniceStoreName));
        } else {
          logger.info("Unable to find corresponding metadata system store version for store: " + storeName
              + ". Using the default value of: " + DEFAULT_SYSTEM_STORE_CURRENT_VERSION
              + " instead. Please use the config: " + ConfigKeys.CLIENT_METADATA_SYSTEM_STORE_VERSION_MAP
              + " to specify if the default value doesn't work.");
        }
        Version currentVersion = new VersionImpl(storeName, currentVersionNumber, "system_store_push_job",
            DEFAULT_SYSTEM_STORE_PARTITION_COUNT);
        store.addVersion(currentVersion);
        store.setCurrentVersion(currentVersion.getNumber());
        return store;
      });
      if (metadataStoreSchemaReader == null) {
        ClientConfig<StoreMetadataValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
            .setStoreName(VeniceSystemStoreType.METADATA_STORE.getPrefix())
            .setSpecificValueClass(StoreMetadataValue.class);
        metadataStoreSchemaReader = ClientFactory.getSchemaReader(clonedClientConfig);
      }
    } else {
      super.subscribe(storeName);
    }
  }

  @Override
  public Store getStore(String storeName) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      Store store = metadataSystemStores.get(storeName);
      if (store != null) {
        store = new ReadOnlyStore(store);
      }
      return store;
    } else {
      return super.getStore(storeName);
    }
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      Store store = metadataSystemStores.get(storeName);
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
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      return metadataSystemStores.containsKey(storeName);
    } else {
      return super.hasStore(storeName);
    }
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      return new SchemaEntry(KEY_SCHEMA_ID, metadataStoreSchemaReader.getKeySchema());
    } else {
      return super.getKeySchema(storeName);
    }
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      return metadataStoreSchemaReader.getValueSchemaId(Schema.parse(valueSchemaStr));
    } else {
      return super.getValueSchemaId(storeName, valueSchemaStr);
    }
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      throw new VeniceException("getValueSchemas is not supported with store: " + storeName);
    } else {
      return super.getValueSchemas(storeName);
    }
  }

  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      return new SchemaEntry(metadataStoreSchemaReader.getLatestValueSchemaId(),
          metadataStoreSchemaReader.getLatestValueSchema());
    } else {
      return super.getLatestValueSchema(storeName);
    }
  }

  @Override
  public void clear() {
    super.clear();
    daVinciClientMap.forEach((k, v) -> v.unsubscribeAll());
    daVinciClientFactory.close();
    daVinciClientMap.clear();
    if (metadataStoreSchemaReader != null) {
      metadataStoreSchemaReader.close();
    }
  }

  @Override
  protected StoreConfig getStoreConfigFromSystemStore(String storeName) {
    return getStoreConfigFromMetadataSystemStore(storeName);
  }

  @Override
  protected Store getStoreFromSystemStore(String storeName, String clusterName) {
    return getStoreFromMetadataSystemStore(storeName, clusterName);
  }

  @Override
  protected StoreMetadataValue getStoreMetadata(String storeName, StoreMetadataKey key)
      throws ExecutionException, InterruptedException {
    return getDaVinciClientForSystemStore(storeName).get(key).get();
  }

  @Override
  protected SchemaEntry getValueSchemaInternally(String storeName, int id) {
    if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
      Schema schema = metadataStoreSchemaReader.getValueSchema(id);
      return schema == null ? null : new SchemaEntry(id, schema);
    } else {
      return super.getValueSchemaInternally(storeName, id);
    }
  }

  @Override
  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    return getSchemaDataFromMetadataSystemStore(storeName);
  }

  private DaVinciClient<StoreMetadataKey, StoreMetadataValue> getDaVinciClientForSystemStore(String storeName) {
    return daVinciClientMap.computeIfAbsent(storeName, k -> {
      DaVinciClient<StoreMetadataKey, StoreMetadataValue> client =
          daVinciClientFactory.getAndStartSpecificAvroClient(VeniceSystemStoreUtils.getMetadataStoreName(storeName),
              daVinciConfig, StoreMetadataValue.class);
      try {
        client.subscribeAll().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException("Failed to construct DaVinci client for the metadata store of store: " + storeName,
            e);
      }
      daVinciClientMap.put(storeName, client);
      return client;
    });
  }
}

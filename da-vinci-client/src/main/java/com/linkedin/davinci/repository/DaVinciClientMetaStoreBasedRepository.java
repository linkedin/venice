package com.linkedin.davinci.repository;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StoreStateReader;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
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
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;


/**
 * This implementation uses DaVinci client backed meta system store to provide data to the {@link NativeMetadataRepository}.
 * The data is then cached and served from there.
 */
public class DaVinciClientMetaStoreBasedRepository extends NativeMetadataRepository {
  private static final int KEY_SCHEMA_ID = 1;
  private static final long DEFAULT_DA_VINCI_CLIENT_ROCKS_DB_MEMORY_LIMIT = 1024 * 1024 * 1024; // 1GB
  private static final Logger logger = Logger.getLogger(DaVinciClientMetaStoreBasedRepository.class);

  // Map of user store name to their corresponding meta store which is used for finding the correct current version.
  // TODO Store objects in this map are mocked locally based on client.meta.system.store.version.map config.

  // A map of user store name to their corresponding daVinci client of the meta store.
  private final Map<String, DaVinciClient<StoreMetaKey, StoreMetaValue>> daVinciClientMap =
      new VeniceConcurrentHashMap<>();

  // A map of mocked meta Store objects. Keep the meta stores separately from SystemStoreBasedRepository.subscribedStoreMap
  // because these meta stores doesn't require refreshes.
  private final Map<String, Store> metaStoreMap = new VeniceConcurrentHashMap<>();

  private final DaVinciConfig daVinciConfig = new DaVinciConfig();
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final SchemaReader metaStoreSchemaReader;

  public DaVinciClientMetaStoreBasedRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);
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
          "Failed to get metadata from meta system store with DaVinci client for store: " + storeName + " with key: "
              + key.toString(), e);
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
      return metaStoreSchemaReader.getValueSchemaId(Schema.parse(valueSchemaStr));
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
      metaStoreMap.computeIfAbsent(storeName, k -> getMetaStore(storeName));
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

  private Store getMetaStore(String metaStoreName) {
    ClientConfig clonedClientConfig = ClientConfig.cloneConfig(clientConfig).setStoreName(metaStoreName);
    try (StoreStateReader storeStateReader = StoreStateReader.getInstance(clonedClientConfig)) {
      return storeStateReader.getStore();
    }
  }

  @Override
  protected StoreMetadataValue getStoreMetadata(String storeName, StoreMetadataKey key) {
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
      long metaStoreDVCStartTime = System.currentTimeMillis();
      DaVinciClient<StoreMetaKey, StoreMetaValue> client = daVinciClientFactory.getAndStartSpecificAvroClient(
          VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), daVinciConfig, StoreMetaValue.class);
      try {
        client.subscribeAll().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException("Failed to construct DaVinci client for the meta store of store: " + storeName, e);
      }
      logger.info(String.format("DaVinci client for the meta store of store: %s constructed, took: %s ms", storeName,
          System.currentTimeMillis() - metaStoreDVCStartTime));
      return client;
    });
  }
}

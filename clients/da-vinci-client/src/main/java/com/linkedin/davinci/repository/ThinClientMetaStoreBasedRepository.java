package com.linkedin.davinci.repository;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class ThinClientMetaStoreBasedRepository extends NativeMetadataRepository {
  private final Map<String, AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue>> storeClientMap =
      new VeniceConcurrentHashMap<>();
  private final ICProvider icProvider;

  public ThinClientMetaStoreBasedRepository(
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      ICProvider icProvider) {
    super(clientConfig, backendConfig);
    this.icProvider = icProvider;
  }

  @Override
  public void clear() {
    super.clear();
    storeClientMap.forEach((k, v) -> v.close());
    storeClientMap.clear();
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    if (VeniceSystemStoreType.getSystemStoreType(storeName) != null) {
      throw new UnsupportedOperationException(
          "The implementation " + getClass().getSimpleName() + " should not be subscribing to system store: "
              + storeName + ". Something is mis-configured");
    } else {
      super.subscribe(storeName);
    }
  }

  @Override
  protected StoreConfig fetchStoreConfigFromRemote(String storeName) {
    StoreClusterConfig clusterConfig = getStoreMetaValue(
        storeName,
        MetaStoreDataType.STORE_CLUSTER_CONFIG
            .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName))).storeClusterConfig;
    return new StoreConfig(clusterConfig);
  }

  @Override
  protected Store fetchStoreFromRemote(String storeName, String clusterName) {
    StoreProperties storeProperties =
        getStoreMetaValue(storeName, MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
          }
        })).storeProperties;
    return new ZKStore(storeProperties);
  }

  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
    final Callable<CompletableFuture<StoreMetaValue>> supplier = () -> getAvroClientForMetaStore(storeName).get(key);
    Callable<CompletableFuture<StoreMetaValue>> wrappedSupplier =
        icProvider == null ? supplier : () -> icProvider.call(getClass().getCanonicalName(), supplier);
    StoreMetaValue value = RetryUtils.executeWithMaxAttempt(() -> {
      try {
        return wrappedSupplier.call().get(5, TimeUnit.SECONDS);
      } catch (ServiceDiscoveryException e) {
        throw e;
      } catch (Exception e) {
        throw new VeniceRetriableException(
            "Failed to get data from meta store using thin client for store: " + storeName + " with key: " + key,
            e);
      }
    }, 10, Duration.ofSeconds(1), Collections.singletonList(VeniceRetriableException.class));

    if (value == null) {
      throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
    }
    return value;
  }

  @Override
  protected SchemaData getSchemaData(String storeName) {
    SchemaData schemaData = schemaMap.get(storeName);
    SchemaEntry keySchema;
    if (schemaData == null) {
      // Retrieve the key schema and initialize SchemaData only if it's not cached yet.
      StoreMetaKey keySchemaKey = MetaStoreDataType.STORE_KEY_SCHEMAS
          .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
      Map<CharSequence, CharSequence> keySchemaMap =
          getStoreMetaValue(storeName, keySchemaKey).storeKeySchemas.keySchemaMap;
      if (keySchemaMap.isEmpty()) {
        throw new VeniceException("No key schema found for store: " + storeName);
      }
      Map.Entry<CharSequence, CharSequence> keySchemaEntry = keySchemaMap.entrySet().iterator().next();
      keySchema =
          new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString());
      schemaData = new SchemaData(storeName, keySchema);
    }
    StoreMetaKey valueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMAS
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    Map<CharSequence, CharSequence> valueSchemaMap =
        getStoreMetaValue(storeName, valueSchemaKey).storeValueSchemas.valueSchemaMap;
    // Check the value schema string, if it's empty then try to query the other key space for individual value schema.
    for (Map.Entry<CharSequence, CharSequence> entry: valueSchemaMap.entrySet()) {
      // Check if we already have the corresponding value schema
      int valueSchemaId = Integer.parseInt(entry.getKey().toString());
      if (schemaData.getValueSchema(valueSchemaId) != null) {
        continue;
      }
      if (entry.getValue().toString().isEmpty()) {
        // The value schemas might be too large to be stored in a single K/V.
        StoreMetaKey individualValueSchemaKey =
            MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
              {
                put(KEY_STRING_STORE_NAME, storeName);
                put(KEY_STRING_SCHEMA_ID, entry.getKey().toString());
              }
            });
        // Empty string is not a valid value schema therefore it's safe to throw exceptions if we also cannot find it in
        // the individual value schema key space.
        String valueSchema =
            getStoreMetaValue(storeName, individualValueSchemaKey).storeValueSchema.valueSchema.toString();
        schemaData.addValueSchema(new SchemaEntry(valueSchemaId, valueSchema));
      } else {
        schemaData.addValueSchema(new SchemaEntry(valueSchemaId, entry.getValue().toString()));
      }
    }
    return schemaData;
  }

  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getAvroClientForMetaStore(String storeName) {
    return storeClientMap.computeIfAbsent(storeName, k -> {
      ClientConfig<StoreMetaValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName))
          .setSpecificValueClass(StoreMetaValue.class)
          .setRetryOnRouterError(false)
          .setRetryOnAllErrors(false)
          .setForceClusterDiscoveryAtStartTime(true);
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
  }
}

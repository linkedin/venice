package com.linkedin.davinci.repository;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;


public class ThinClientMetaStoreBasedRepository extends NativeMetadataRepository {

  private final Map<String, AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue>> storeClientMap =
      new VeniceConcurrentHashMap<>();

  public ThinClientMetaStoreBasedRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);
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
          "The implementation " + this.getClass().getSimpleName() + " should not be subscribing to system store: "
              + storeName + ". Something is mis-configured");
    } else {
      super.subscribe(storeName);
    }
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

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
    StoreMetaValue value;
    try {
      value = getAvroClientForMetaStore(storeName).get(key).get();
      if (value == null) {
        throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
      }
      return value;
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException(
          "Failed to get data from meta store using thin client for store: " + storeName + " with key: "
              + key.toString());
    }
  }

  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getAvroClientForMetaStore(String storeName) {
    return storeClientMap.computeIfAbsent(storeName, k -> {
      ClientConfig<StoreMetaValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName))
          .setSpecificValueClass(StoreMetaValue.class);
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
  }
}

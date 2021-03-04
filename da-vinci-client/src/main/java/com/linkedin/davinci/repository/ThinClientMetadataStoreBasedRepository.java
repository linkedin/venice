package com.linkedin.davinci.repository;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class ThinClientMetadataStoreBasedRepository extends NativeMetadataRepository {

  // Local cache for system store clients.
  private final Map<String, AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue>> storeClientMap =
      new VeniceConcurrentHashMap<>();

  public ThinClientMetadataStoreBasedRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
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
    return getStoreConfigFromMetadataSystemStore(storeName);
  }

  @Override
  protected Store getStoreFromSystemStore(String storeName, String clusterName) {
    return getStoreFromMetadataSystemStore(storeName, clusterName);
  }

  @Override
  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    return getSchemaDataFromMetadataSystemStore(storeName);
  }

  @Override
  protected StoreMetadataValue getStoreMetadata(String storeName, StoreMetadataKey key)
      throws ExecutionException, InterruptedException {
    return getAvroClientForSystemStore(storeName).get(key).get();
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key)
      throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException(
        "getStoreMetaValue for store: " + storeName + " and key: " + key.toString() + " is not supported in "
            + this.getClass().getSimpleName());
  }

  protected AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> getAvroClientForSystemStore(
      String storeName) {
    return storeClientMap.computeIfAbsent(storeName, k -> {
      ClientConfig<StoreMetadataValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreUtils.getMetadataStoreName(storeName))
          .setSpecificValueClass(StoreMetadataValue.class);
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
  }
}

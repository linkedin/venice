package com.linkedin.davinci.repository;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class ThinClientMetadataStoreBasedRepository extends NativeMetadataRepository {

  // Local cache for system store clients.
  private final Map<String, AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue>>
      storeClientMap = new VeniceConcurrentHashMap<>();

  public ThinClientMetadataStoreBasedRepository(ClientConfig<StoreMetadataValue> clientConfig,
      VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);
  }

  @Override
  public void clear() {
    super.clear();
    storeClientMap.forEach((k, v) -> v.close());
    storeClientMap.clear();
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

  protected AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> getAvroClientForSystemStore(String storeName) {
    return storeClientMap.computeIfAbsent(storeName,  k -> {
      ClientConfig<StoreMetadataValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreUtils.getMetadataStoreName(storeName))
          .setSpecificValueClass(StoreMetadataValue.class);
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
  }
}

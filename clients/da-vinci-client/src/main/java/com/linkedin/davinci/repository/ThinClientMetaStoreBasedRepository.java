package com.linkedin.davinci.repository;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.util.Arrays;
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
  protected StoreConfig getStoreConfigFromSystemStore(String storeName) {
    return getStoreConfigFromMetaSystemStore(storeName);
  }

  @Override
  protected Store getStoreFromSystemStore(String storeName, String clusterName) {
    StoreProperties storeProperties =
        getStoreMetaValue(storeName, MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
          }
        })).storeProperties;
    return new ZKStore(storeProperties);
  }

  @Override
  protected SchemaData getSchemaDataFromSystemStore(String storeName) {
    return getSchemaDataFromMetaSystemStore(storeName);
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
    final Callable<CompletableFuture<StoreMetaValue>> supplier = () -> getAvroClientForMetaStore(storeName).get(key);
    Callable<CompletableFuture<StoreMetaValue>> wrappedSupplier =
        icProvider == null ? supplier : () -> icProvider.call(getClass().getCanonicalName(), supplier);
    StoreMetaValue value = RetryUtils.executeWithMaxAttempt(() -> {
      try {
        return wrappedSupplier.call().get(1, TimeUnit.SECONDS);
      } catch (ServiceDiscoveryException e) {
        throw e;
      } catch (Exception e) {
        throw new VeniceRetriableException(
            "Failed to get data from meta store using thin client for store: " + storeName + " with key: " + key,
            e);
      }
    }, 5, Duration.ofSeconds(1), Arrays.asList(VeniceRetriableException.class));

    if (value == null) {
      throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
    }
    return value;
  }

  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getAvroClientForMetaStore(String storeName) {
    return storeClientMap.computeIfAbsent(storeName, k -> {
      ClientConfig<StoreMetaValue> clonedClientConfig = ClientConfig.cloneConfig(clientConfig)
          .setStoreName(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName))
          .setSpecificValueClass(StoreMetaValue.class)
          .setRetryOnAllErrors(true)
          .setRetryCount(THIN_CLIENT_RETRY_COUNT)
          .setForceClusterDiscoveryAtStartTime(true)
          .setRetryBackOffInMs(THIN_CLIENT_RETRY_BACKOFF_MS);
      return ClientFactory.getAndStartSpecificAvroClient(clonedClientConfig);
    });
  }
}

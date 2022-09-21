package com.linkedin.venice.client.factory;

import com.linkedin.venice.client.VeniceStoreClientGlobalConfig;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CachingVeniceStoreClientFactory implements VeniceStoreClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(CachingVeniceStoreClientFactory.class);

  private final Map<String, AvroGenericStoreClient> genericStoreClientMap = new HashMap<>();
  private final Map<String, AvroSpecificStoreClient> specificStoreClientMap = new HashMap<>();

  // Creating 2 clients that have the same store name is not allowed in InGraphReporter.
  // This is a cache between store name and store config that prevents this happening.
  private final Map<String, ClientConfig> storeToConfigMap = new HashMap<>();

  private final ClientConfig clientConfig;

  private final VeniceStoreClientGlobalConfig globalConfig;

  public CachingVeniceStoreClientFactory(ClientConfig clientConfig) {
    this(clientConfig, null);
  }

  public CachingVeniceStoreClientFactory(ClientConfig clientConfig, VeniceStoreClientGlobalConfig globalConfig) {
    this.clientConfig = clientConfig;
    this.globalConfig = globalConfig;
  }

  @Override
  public synchronized <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(String storeName) {
    return getAndStartAvroGenericStoreClient(storeName, (clientConfig) -> {
      if (globalConfig != null && globalConfig.getVsonStoreList().contains(storeName)) {
        clientConfig.setVsonClient(true);
      }

      return clientConfig;
    });
  }

  @Override
  public synchronized <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(
      String storeName,
      ClientConfigUpdater configUpdater) {
    ClientConfig updatedClientConfig =
        configUpdater.update(ClientConfig.cloneConfig(clientConfig).setStoreName(storeName));
    checkDupStoreClient(storeName, updatedClientConfig);

    return genericStoreClientMap.computeIfAbsent(storeName, name -> {
      if (updatedClientConfig.isVsonClient()) {
        if (globalConfig != null && !globalConfig.getVsonStoreList().contains(storeName)) {
          throw new VeniceClientException("Cannot convert Avro store to Vson store");
        }

        LOGGER.info("Will create a VSON store client for store: {}", storeName);
      } else {
        LOGGER.info("Created a new AvroGenericStoreClient for store: {}", storeName);
      }

      return ClientFactory.getAndStartGenericAvroClient(updatedClientConfig);
    });
  }

  @Override
  public synchronized <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
      String storeName,
      Class<V> specificRecordClass) {
    return getAndStartAvroSpecificStoreClient(storeName, specificRecordClass, (clientConfig) -> clientConfig);
  }

  @Override
  public synchronized <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
      String storeName,
      Class<V> specificRecordClass,
      ClientConfigUpdater configUpdater) {
    ClientConfig updatedClientConfig = configUpdater.update(
        ClientConfig.cloneConfig(clientConfig).setStoreName(storeName).setSpecificValueClass(specificRecordClass));
    checkDupStoreClient(storeName, updatedClientConfig);

    return specificStoreClientMap.computeIfAbsent(storeName, name -> {
      LOGGER.info("Created a new AvroSpecificStoreClient for store: {}", storeName);
      return ClientFactory.getAndStartSpecificAvroClient(updatedClientConfig);
    });
  }

  private void checkDupStoreClient(String storeName, ClientConfig clientConfig) {
    if (storeToConfigMap.containsKey(storeName) && !storeToConfigMap.get(storeName).equals(clientConfig)) {
      throw new VeniceClientException(
          "Duplicate store client creation. VeniceStoreClientFactory doesn't support"
              + " to create duplicate clients that have the same store name. If it's intent, try creating multiple client factory");
    } else {
      storeToConfigMap.put(storeName, clientConfig);
    }
  }

  @Override
  public synchronized void close() {
    genericStoreClientMap.forEach((storeName, storeClient) -> storeClient.close());
    genericStoreClientMap.clear();
    specificStoreClientMap.forEach((storeName, storeClient) -> storeClient.close());
    specificStoreClientMap.clear();
    LOGGER.info("Closed store client factory");
  }
}

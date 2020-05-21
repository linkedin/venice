package com.linkedin.davinci.client.factory;

import com.linkedin.davinci.client.AvroGenericDaVinciClientImpl;
import com.linkedin.davinci.client.AvroSpecificDaVinciClientImpl;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;


public class CachingDaVinciClientFactory implements DaVinciClientFactory, AutoCloseable {
  private static final Logger logger = Logger.getLogger(CachingDaVinciClientFactory.class);

  protected final ClientConfig clientConfig;
  protected final VeniceProperties backendConfig;
  protected final Map<String, DaVinciClient> clients = new HashMap<>();
  protected final Map<String, DaVinciConfig> configs = new HashMap<>();

  public CachingDaVinciClientFactory(ClientConfig clientConfig, VeniceProperties backendConfig) {
    this.clientConfig = clientConfig;
    this.backendConfig = backendConfig;
  }

  @Override
  public synchronized void close() {
    for (DaVinciClient client : clients.values()) {
      try {
        client.close();
      } catch (Exception e) {
        logger.error("Unable to close a client, storeName=" + client.getStoreName(), e);
      }
    }
    clients.clear();
    configs.clear();
  }

  @Override
  public  <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(storeName, config, null, AvroGenericDaVinciClientImpl::new, AvroGenericDaVinciClientImpl.class);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(String storeName, DaVinciConfig config, Class<V> valueClass) {
    return getClient(storeName, config, valueClass, AvroSpecificDaVinciClientImpl::new, AvroSpecificDaVinciClientImpl.class);
  }

  protected interface DaVinciClientConstructor {
    DaVinciClient apply(DaVinciConfig config, ClientConfig clientConfig, VeniceProperties backendConfig);
  }

  protected synchronized DaVinciClient getClient(
      String storeName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass) {

    DaVinciConfig originalConfig = configs.computeIfAbsent(storeName, k -> config);
    if (originalConfig.getStorageClass() != config.getStorageClass()) {
      throw new VeniceException("Storage class conflict"
          + ", storeName=" + storeName
          + ", original=" + originalConfig.getStorageClass()
          + ", requested=" + config.getStorageClass());
    }

    if (originalConfig.getRemoteReadPolicy() != config.getRemoteReadPolicy()) {
      throw new VeniceException("Remote read policy conflict"
          + ", storeName=" + storeName
          + ", original=" + originalConfig.getRemoteReadPolicy()
          + ", requested=" + config.getRemoteReadPolicy());
    }

    DaVinciClient client = clients.computeIfAbsent(storeName, k -> {
      ClientConfig clientConfig = ClientConfig.cloneConfig(this.clientConfig);
      clientConfig.setStoreName(storeName);
      clientConfig.setSpecificValueClass(valueClass);
      DaVinciClient newClient = clientConstructor.apply(config, clientConfig, backendConfig);
      newClient.start();
      return newClient;
    });

    if (!clientClass.isInstance(client)) {
      throw new VeniceException("Client type conflict"
          + ", storeName=" + storeName
          + ", originalClientClass=" + client.getClass()
          + ", requestedStorageClass=" + clientClass);
    }

    return client;
  }
}



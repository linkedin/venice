package com.linkedin.davinci.client.factory;

import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.AvroSpecificDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;

import io.tehuti.metrics.MetricsRepository;

import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class CachingDaVinciClientFactory implements DaVinciClientFactory, AutoCloseable {
  private static final Logger logger = Logger.getLogger(CachingDaVinciClientFactory.class);

  protected boolean closed;
  protected final D2Client d2Client;
  protected final MetricsRepository metricsRepository;
  protected final VeniceProperties backendConfig;
  protected final Optional<Set<String>> managedClients;
  protected final Map<String, AvroGenericDaVinciClient> cachedClients = new HashMap<>();
  protected final List<DaVinciClient> isolatedClients = new ArrayList<>();
  protected final Map<String, DaVinciConfig> configs = new HashMap<>();

  public CachingDaVinciClientFactory(D2Client d2Client, MetricsRepository metricsRepository, VeniceProperties backendConfig) {
    this(d2Client, metricsRepository, backendConfig, Optional.empty());
  }

  public CachingDaVinciClientFactory(D2Client d2Client, MetricsRepository metricsRepository, VeniceProperties backendConfig, Optional<Set<String>> managedClients) {
    this.d2Client = d2Client;
    this.metricsRepository = metricsRepository;
    this.backendConfig = backendConfig;
    this.managedClients = managedClients;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      logger.warn("Ignoring second attempt to close Da Vinci client factory");
      return;
    }
    closed = true;

    List<DaVinciClient> clients = new ArrayList<>(cachedClients.values());
    clients.addAll(isolatedClients);
    logger.info("Closing Da Vinci client factory, clientCount=" + clients.size());
    for (DaVinciClient client : clients) {
      try {
        client.close();
      } catch (Throwable e) {
        logger.error("Unable to close a client, storeName=" + client.getStoreName(), e);
      }
    }
    D2ClientUtils.shutdownClient(d2Client);
    cachedClients.clear();
    isolatedClients.clear();
    configs.clear();
    logger.info("Da Vinci client factory is closed successfully, clientCount=" + clients.size());
  }

  @Override
  public <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(storeName, config, null, AvroGenericDaVinciClient::new, AvroGenericDaVinciClient.class, false);
  }

  @Override
  public <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(storeName, config, null, AvroGenericDaVinciClient::new, AvroGenericDaVinciClient.class, true);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(String storeName, DaVinciConfig config, Class<V> valueClass) {
    return getClient(storeName, config, valueClass, AvroSpecificDaVinciClient::new, AvroSpecificDaVinciClient.class, false);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClient(String storeName, DaVinciConfig config, Class<V> valueClass) {
    return getClient(storeName, config, valueClass, AvroSpecificDaVinciClient::new, AvroSpecificDaVinciClient.class, true);
  }

  protected interface DaVinciClientConstructor {
    AvroGenericDaVinciClient apply(DaVinciConfig config, ClientConfig clientConfig, VeniceProperties backendConfig, Optional<Set<String>> managedClients);
  }

  protected synchronized DaVinciClient getClient(
      String storeName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass,
      boolean startClient) {
    if (closed) {
      throw new VeniceException("Cannot obtain a client from a closed factory, storeName=" + storeName);
    }

    DaVinciConfig originalConfig = configs.computeIfAbsent(storeName, k -> config);
    if (originalConfig.isManaged() != config.isManaged()) {
      throw new VeniceException("Managed flag conflict"
                                    + ", storeName=" + storeName
                                    + ", original=" + originalConfig.isManaged()
                                    + ", requested=" + config.isManaged());
    }

    if (originalConfig.getMemoryLimit() != config.getMemoryLimit()) {
      throw new VeniceException("Memory limit conflict"
                                    + ", storeName=" + storeName
                                    + ", original=" + originalConfig.getMemoryLimit()
                                    + ", requested=" + config.getMemoryLimit());
    }

    if (originalConfig.getStorageClass() != config.getStorageClass()) {
      throw new VeniceException("Storage class conflict"
          + ", storeName=" + storeName
          + ", original=" + originalConfig.getStorageClass()
          + ", requested=" + config.getStorageClass());
    }

    ClientConfig clientConfig = new ClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
            .setMetricsRepository(metricsRepository)
            .setSpecificValueClass(valueClass);

    AvroGenericDaVinciClient client;
    if (config.isIsolated()) {
      client = clientConstructor.apply(config, clientConfig, backendConfig, managedClients);
      isolatedClients.add(client);
    } else {
      if (originalConfig.getNonLocalAccessPolicy() != config.getNonLocalAccessPolicy()) {
        throw new VeniceException("Non-local access policy conflict"
                                      + ", storeName=" + storeName
                                      + ", original=" + originalConfig.getNonLocalAccessPolicy()
                                      + ", requested=" + config.getNonLocalAccessPolicy());
      }

      client = cachedClients.computeIfAbsent(storeName,
          k -> clientConstructor.apply(config, clientConfig, backendConfig, managedClients));

      if (!clientClass.isInstance(client)) {
        throw new VeniceException("Client type conflict"
                                      + ", storeName=" + storeName
                                      + ", originalClientClass=" + client.getClass()
                                      + ", requestedClientClass=" + clientClass);
      }
    }

    if (startClient) {
      // synchronize ensures that isReady() returns up-to-date state when start() or close() runs in another thread.
      synchronized (client) {
        if (!client.isReady()) {
          client.start();
        }
      }
    }
    return client;
  }
}

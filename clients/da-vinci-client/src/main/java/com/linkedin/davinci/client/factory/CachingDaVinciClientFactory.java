package com.linkedin.davinci.client.factory;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.AvroSpecificDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CachingDaVinciClientFactory implements DaVinciClientFactory, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(CachingDaVinciClientFactory.class);

  protected boolean closed;
  protected final D2Client d2Client;
  protected final MetricsRepository metricsRepository;
  protected final VeniceProperties backendConfig;
  protected final Optional<Set<String>> managedClients;
  protected final ICProvider icProvider;
  protected final Map<String, AvroGenericDaVinciClient> sharedClients = new HashMap<>();
  protected final List<DaVinciClient> isolatedClients = new ArrayList<>();
  protected final Map<String, DaVinciConfig> configs = new HashMap<>();

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig) {
    this(d2Client, metricsRepository, backendConfig, Optional.empty(), null);
  }

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients) {
    this(d2Client, metricsRepository, backendConfig, managedClients, null);
  }

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider) {
    LOGGER.info(
        "Creating client factory, managedClients={}, existingMetrics={}",
        managedClients,
        metricsRepository.metrics().keySet());
    this.d2Client = d2Client;
    this.metricsRepository = metricsRepository;
    this.backendConfig = backendConfig;
    this.managedClients = managedClients;
    this.icProvider = icProvider;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      LOGGER.warn("Ignoring duplicate attempt to close client factory");
      return;
    }
    closed = true;

    List<DaVinciClient> clients = new ArrayList<>(sharedClients.values());
    clients.addAll(isolatedClients);
    LOGGER.info("Closing client factory, clientCount={}", clients.size());
    for (DaVinciClient client: clients) {
      try {
        client.close();
      } catch (Throwable e) {
        LOGGER.error("Unable to close a client, storeName={}", client.getStoreName(), e);
      }
    }
    sharedClients.clear();
    isolatedClients.clear();
    configs.clear();
    LOGGER.info("Client factory is closed successfully, clientCount={}", clients.size());
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
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        config,
        valueClass,
        AvroSpecificDaVinciClient::new,
        AvroSpecificDaVinciClient.class,
        false);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        config,
        valueClass,
        AvroSpecificDaVinciClient::new,
        AvroSpecificDaVinciClient.class,
        true);
  }

  public VeniceProperties getBackendConfig() {
    return backendConfig;
  }

  protected interface DaVinciClientConstructor {
    AvroGenericDaVinciClient apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider);
  }

  protected synchronized DaVinciClient getClient(
      String storeName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass,
      boolean startClient) {
    if (closed) {
      throw new VeniceException("Unable to get a client from a closed factory, storeName=" + storeName);
    }

    DaVinciConfig originalConfig = configs.computeIfAbsent(storeName, k -> config);
    if (originalConfig.isManaged() != config.isManaged()) {
      throw new VeniceException(
          "Managed flag conflict" + ", storeName=" + storeName + ", original=" + originalConfig.isManaged()
              + ", requested=" + config.isManaged());
    }

    if (originalConfig.getStorageClass() != config.getStorageClass()) {
      throw new VeniceException(
          "Storage class conflict" + ", storeName=" + storeName + ", original=" + originalConfig.getStorageClass()
              + ", requested=" + config.getStorageClass());
    }

    ClientConfig clientConfig = new ClientConfig(storeName).setD2Client(d2Client)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
        .setMetricsRepository(metricsRepository)
        .setSpecificValueClass(valueClass);

    AvroGenericDaVinciClient client;
    if (config.isIsolated()) {
      String statsPrefix = "davinci-client-" + isolatedClients.size();
      clientConfig.setStatsPrefix(statsPrefix);
      client = clientConstructor.apply(config, clientConfig, backendConfig, managedClients, icProvider);
      isolatedClients.add(client);
    } else {
      if (originalConfig.getNonLocalAccessPolicy() != config.getNonLocalAccessPolicy()) {
        throw new VeniceException(
            "Non-local access policy conflict" + ", storeName=" + storeName + ", original="
                + originalConfig.getNonLocalAccessPolicy() + ", requested=" + config.getNonLocalAccessPolicy());
      }

      client = sharedClients.computeIfAbsent(
          storeName,
          k -> clientConstructor.apply(config, clientConfig, backendConfig, managedClients, icProvider));

      if (!clientClass.isInstance(client)) {
        throw new VeniceException(
            "Client type conflict" + ", storeName=" + storeName + ", originalClientClass=" + client.getClass()
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

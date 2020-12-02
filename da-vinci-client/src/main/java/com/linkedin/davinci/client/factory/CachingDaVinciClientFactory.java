package com.linkedin.davinci.client.factory;

import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.AvroSpecificDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;

import io.tehuti.metrics.MetricsRepository;

import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class CachingDaVinciClientFactory implements DaVinciClientFactory, AutoCloseable {
  private static final Logger logger = Logger.getLogger(CachingDaVinciClientFactory.class);

  protected boolean isClosed;
  protected final D2Client d2Client;
  protected final MetricsRepository metricsRepository;
  protected final VeniceProperties backendConfig;
  protected final Map<String, DaVinciClient> clients = new HashMap<>();
  protected final Map<String, DaVinciConfig> configs = new HashMap<>();
  protected final String instanceName;

  public CachingDaVinciClientFactory(D2Client d2Client, MetricsRepository metricsRepository, VeniceProperties backendConfig, String appName) {
    this.d2Client = d2Client;
    this.metricsRepository = metricsRepository;
    this.backendConfig = backendConfig;
    this.instanceName = Utils.getHostName() + "/" + appName;
  }

  public CachingDaVinciClientFactory(D2Client d2Client, MetricsRepository metricsRepository, VeniceProperties backendConfig) {
    this(d2Client, metricsRepository, backendConfig, "test");
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      logger.warn("Ignoring second attempt to close Da Vinci client factory");
      return;
    }
    isClosed = true;

    logger.info("Closing Da Vinci client factory, clientCount=" + clients.size());
    for (DaVinciClient client : clients.values()) {
      try {
        client.close();
      } catch (Throwable e) {
        logger.error("Unable to close a client, storeName=" + client.getStoreName(), e);
      }
    }
    D2ClientUtils.shutdownClient(d2Client);
    clients.clear();
    configs.clear();
    logger.info("Da Vinci client factory is closed successfully, clientCount=" + clients.size());
  }

  @Override
  public  <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(storeName, config, null, AvroGenericDaVinciClient::new, AvroGenericDaVinciClient.class, false);
  }

  @Override
  public  <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(String storeName, DaVinciConfig config) {
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
    DaVinciClient apply(DaVinciConfig config, ClientConfig clientConfig, VeniceProperties backendConfig, String instanceName);
  }

  protected synchronized DaVinciClient getClient(
      String storeName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass,
      boolean startClient) {
    if (isClosed) {
      throw new VeniceException("Cannot obtain a client from a closed factory, storeName=" + storeName);
    }

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
      ClientConfig clientConfig = new ClientConfig(storeName)
              .setD2Client(d2Client)
              .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
              .setMetricsRepository(metricsRepository)
              .setSpecificValueClass(valueClass);
      DaVinciClient newClient = clientConstructor.apply(config, clientConfig, backendConfig, instanceName);
      if (startClient) {
        newClient.start();
      }
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

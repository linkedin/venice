package com.linkedin.venice.integration.utils;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


public class DaVinciTestContext<K, V> {
  private static final Logger logger = LogManager.getLogger(DaVinciTestContext.class);
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final DaVinciClient<K, V> daVinciClient;
  private static final int maxAttempt = 10;

  public DaVinciTestContext(CachingDaVinciClientFactory factory, DaVinciClient<K, V> client) {
    daVinciClientFactory = factory;
    daVinciClient = client;
  }

  public CachingDaVinciClientFactory getDaVinciClientFactory() {
    return daVinciClientFactory;
  }

  public DaVinciClient<K, V> getDaVinciClient() {
    return daVinciClient;
  }

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClientWithRetries(String storeName, String zkAddress,
      DaVinciConfig daVinciConfig, Map<String, Object> extraBackendProperties) {
    ClientConfig clientConfig = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
        .setVeniceURL(zkAddress);
    DaVinciClient<K, V> daVinciClient = null;
    for (int attempt = 1; attempt <= maxAttempt; attempt++) {
      try {
        // Prepare backend config with overrides.
        PropertyBuilder backendConfigBuilder = getDaVinciPropertyBuilder(zkAddress);
        extraBackendProperties.forEach(backendConfigBuilder::put);
        // Get and start Da Vinci client.
        daVinciClient = new AvroGenericDaVinciClient<>(daVinciConfig, clientConfig,
            backendConfigBuilder.build(), Optional.empty());
        daVinciClient.start();
        return daVinciClient;
      } catch (Exception e) {
        String errorMessage = "Got " + e.getClass().getSimpleName() + " while trying to start Da Vinci client. Attempt #"
            + attempt + "/" + maxAttempt + ".";
        logger.warn(errorMessage, e);
        Utils.closeQuietlyWithErrorLogged(daVinciClient);
      }
    }
    throw new VeniceException("Failed to start Da Vinci client in " + maxAttempt + " attempts.");
  }

  public static <K, V> DaVinciTestContext<K, V> getGenericAvroDaVinciFactoryAndClientWithRetries(
      D2Client d2Client, MetricsRepository metricsRepository, Optional<Set<String>> managedClients, String zkAddress,
      String storeName, DaVinciConfig daVinciConfig, Map<String, Object> extraBackendProperties) {
    CachingDaVinciClientFactory factory = null;
    DaVinciClient<K, V> daVinciClient = null;
    for (int attempt = 1; attempt <= maxAttempt; attempt++) {
      try {
        // Prepare backend config with overrides.
        PropertyBuilder backendConfigBuilder = getDaVinciPropertyBuilder(zkAddress);
        extraBackendProperties.forEach(backendConfigBuilder::put);
        VeniceProperties backendConfig = backendConfigBuilder.build();
        logger.info("Creating Da Vinci factory with backend config: " + backendConfig);
        // Create Da Vinci factory
        factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig, managedClients);
        // Get and start Da Vinci client.
        daVinciClient = factory.getGenericAvroClient(storeName, daVinciConfig);
        daVinciClient.start();
        return new DaVinciTestContext<>(factory, daVinciClient);
      } catch (Exception e) {
        String errorMessage = "Got " + e.getClass().getSimpleName() + " while trying to start Da Vinci client. Attempt #"
            + attempt + "/" + maxAttempt + ".";
        logger.warn(errorMessage, e);
        Utils.closeQuietlyWithErrorLogged(daVinciClient);
        Utils.closeQuietlyWithErrorLogged(factory);
      }
    }
    throw new VeniceException("Failed to start Da Vinci client in " + maxAttempt + " attempts.");
  }

  public static PropertyBuilder getDaVinciPropertyBuilder(String zkAddress) {
    return new PropertyBuilder()
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, Utils.getFreePort())
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, Utils.getFreePort())
        .put(SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, zkAddress);
  }
}
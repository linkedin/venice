package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DaVinciTestContext<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciTestContext.class);
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final DaVinciClient<K, V> daVinciClient;
  private static final int MAX_ATTEMPT = 10;

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

  public static <K, V> DaVinciClient<K, V> getGenericAvroDaVinciClientWithRetries(
      String storeName,
      String zkAddress,
      DaVinciConfig daVinciConfig,
      Map<String, Object> extraBackendProperties) {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setVeniceURL(zkAddress);
    DaVinciClient<K, V> daVinciClient = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      try {
        // Prepare backend config with overrides.
        PropertyBuilder backendConfigBuilder = getDaVinciPropertyBuilder(zkAddress);
        extraBackendProperties.forEach(backendConfigBuilder::put);
        // Get and start Da Vinci client.
        daVinciClient =
            new AvroGenericDaVinciClient<>(daVinciConfig, clientConfig, backendConfigBuilder.build(), Optional.empty());
        daVinciClient.start();
        return daVinciClient;
      } catch (Exception e) {
        LOGGER.warn(
            "Got {} while trying to start Da Vinci client. Attempt #{}/{}.",
            e.getClass().getSimpleName(),
            attempt,
            MAX_ATTEMPT,
            e);
        Utils.closeQuietlyWithErrorLogged(daVinciClient);
      }
    }
    throw new VeniceException("Failed to start Da Vinci client in " + MAX_ATTEMPT + " attempts.");
  }

  public static <K, V> DaVinciTestContext<K, V> getGenericAvroDaVinciFactoryAndClientWithRetries(
      D2Client d2Client,
      MetricsRepository metricsRepository,
      Optional<Set<String>> managedClients,
      String zkAddress,
      String storeName,
      DaVinciConfig daVinciConfig,
      Map<String, Object> extraBackendProperties) {
    CachingDaVinciClientFactory factory = null;
    DaVinciClient<K, V> daVinciClient = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
      try {
        // Prepare backend config with overrides.
        PropertyBuilder backendConfigBuilder = getDaVinciPropertyBuilder(zkAddress);
        extraBackendProperties.forEach(backendConfigBuilder::put);
        VeniceProperties backendConfig = backendConfigBuilder.build();
        LOGGER.info("Creating Da Vinci factory with backend config: {}", backendConfig);
        // Create Da Vinci factory
        factory = new CachingDaVinciClientFactory(
            d2Client,
            VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
            metricsRepository,
            backendConfig,
            managedClients);
        // Get and start Da Vinci client.
        daVinciClient = factory.getGenericAvroClient(storeName, daVinciConfig);
        daVinciClient.start();
        return new DaVinciTestContext<>(factory, daVinciClient);
      } catch (Exception e) {
        LOGGER.warn(
            "Got {} while trying to start Da Vinci client. Attempt #{}/{}.",
            e.getClass().getSimpleName(),
            attempt,
            MAX_ATTEMPT,
            e);
        Utils.closeQuietlyWithErrorLogged(daVinciClient);
        Utils.closeQuietlyWithErrorLogged(factory);
      }
    }
    throw new VeniceException("Failed to start Da Vinci client in " + MAX_ATTEMPT + " attempts.");
  }

  public static PropertyBuilder getDaVinciPropertyBuilder(String zkAddress) {
    return new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, TestUtils.getFreePort())
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, TestUtils.getFreePort())
        .put(SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(D2_ZK_HOSTS_ADDRESS, zkAddress)
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME);
  }
}

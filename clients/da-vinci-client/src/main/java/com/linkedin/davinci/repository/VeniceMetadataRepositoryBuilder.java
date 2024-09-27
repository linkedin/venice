package com.linkedin.davinci.repository;

import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.StaticClusterInfoProvider;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * VeniceMetadataRepositoryBuilder is a centralized builder class for constructing a variety of metadata components
 * including store repository, schema repository, ZK-shared schema repository, ZK client and cluster info provider
 * for Da Vinci, Venice Service and Isolated Ingestion Service.
 */
public class VeniceMetadataRepositoryBuilder {
  private static final Logger LOGGER = LogManager.getLogger(VeniceMetadataRepositoryBuilder.class);

  private final VeniceConfigLoader configLoader;
  private final ClientConfig clientConfig;
  private final MetricsRepository metricsRepository;
  private final boolean isIngestionIsolation;
  private final ICProvider icProvider;

  private ReadOnlyStoreRepository storeRepo;
  private HelixReadOnlyStoreConfigRepository storeConfigRepo;
  private ReadOnlySchemaRepository schemaRepo;
  private HelixReadOnlyZKSharedSchemaRepository readOnlyZKSharedSchemaRepository;
  private ReadOnlyLiveClusterConfigRepository liveClusterConfigRepo;
  private ZkClient zkClient;
  private ClusterInfoProvider clusterInfoProvider;

  public VeniceMetadataRepositoryBuilder(
      VeniceConfigLoader configLoader,
      ClientConfig clientConfig,
      MetricsRepository metricsRepository,
      ICProvider icProvider,
      boolean isIngestionIsolation) {
    this.configLoader = configLoader;
    this.clientConfig = clientConfig;
    this.metricsRepository = metricsRepository;
    this.isIngestionIsolation = isIngestionIsolation;
    this.icProvider = icProvider;
    if (isDaVinciClient()) {
      initDaVinciStoreAndSchemaRepository();
    } else {
      initServerStoreAndSchemaRepository();
    }
  }

  public ZkClient getZkClient() {
    return zkClient;
  }

  public ClusterInfoProvider getClusterInfoProvider() {
    return clusterInfoProvider;
  }

  public ReadOnlyStoreRepository getStoreRepo() {
    return storeRepo;
  }

  public HelixReadOnlyStoreConfigRepository getStoreConfigRepo() {
    return storeConfigRepo;
  }

  public ReadOnlySchemaRepository getSchemaRepo() {
    return schemaRepo;
  }

  public ReadOnlyLiveClusterConfigRepository getLiveClusterConfigRepo() {
    return liveClusterConfigRepo;
  }

  public Optional<HelixReadOnlyZKSharedSchemaRepository> getReadOnlyZKSharedSchemaRepository() {
    return Optional.ofNullable(readOnlyZKSharedSchemaRepository);
  }

  public final boolean isDaVinciClient() {
    return configLoader.getVeniceServerConfig().isDaVinciClient();
  }

  private void initDaVinciStoreAndSchemaRepository() {
    VeniceProperties veniceProperties = configLoader.getCombinedProperties();
    LOGGER.info(
        "Initializing meta system store based store repository with {}",
        NativeMetadataRepository.class.getSimpleName());
    NativeMetadataRepository systemStoreBasedRepository =
        NativeMetadataRepository.getInstance(clientConfig, veniceProperties, icProvider);
    systemStoreBasedRepository.start();
    systemStoreBasedRepository.refresh();
    clusterInfoProvider = systemStoreBasedRepository;
    storeRepo = systemStoreBasedRepository;
    schemaRepo = systemStoreBasedRepository;
    liveClusterConfigRepo = null;
  }

  private void initServerStoreAndSchemaRepository() {
    VeniceClusterConfig clusterConfig = configLoader.getVeniceClusterConfig();
    zkClient = ZkClientFactory.newZkClient(clusterConfig.getZookeeperAddress());
    String zkClientNamePrefix = isIngestionIsolation ? "ingestion-isolation-" : "";
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, zkClientNamePrefix + "server-zk-client"));
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    String clusterName = clusterConfig.getClusterName();

    String systemSchemaClusterName = configLoader.getVeniceServerConfig().getSystemSchemaClusterName();
    HelixReadOnlyZKSharedSystemStoreRepository readOnlyZKSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, systemSchemaClusterName);

    HelixReadOnlyStoreRepository readOnlyStoreRepository = new HelixReadOnlyStoreRepository(
        zkClient,
        adapter,
        clusterName,
        clusterConfig.getRefreshAttemptsForZkReconnect(),
        clusterConfig.getRefreshIntervalForZkReconnectInMs());

    storeRepo = new HelixReadOnlyStoreRepositoryAdapter(
        readOnlyZKSharedSystemStoreRepository,
        readOnlyStoreRepository,
        clusterName);
    // Load existing store config and setup watches
    storeRepo.refresh();

    storeConfigRepo = new HelixReadOnlyStoreConfigRepository(zkClient, adapter);
    storeConfigRepo.refresh();

    readOnlyZKSharedSchemaRepository = new HelixReadOnlyZKSharedSchemaRepository(
        readOnlyZKSharedSystemStoreRepository,
        zkClient,
        adapter,
        systemSchemaClusterName,
        clusterConfig.getRefreshAttemptsForZkReconnect(),
        clusterConfig.getRefreshIntervalForZkReconnectInMs());
    schemaRepo = new HelixReadOnlySchemaRepositoryAdapter(
        readOnlyZKSharedSchemaRepository,
        new HelixReadOnlySchemaRepository(
            readOnlyStoreRepository,
            zkClient,
            adapter,
            clusterName,
            clusterConfig.getRefreshAttemptsForZkReconnect(),
            clusterConfig.getRefreshIntervalForZkReconnectInMs()));
    schemaRepo.refresh();

    liveClusterConfigRepo = new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapter, clusterName);
    liveClusterConfigRepo.refresh();

    clusterInfoProvider = new StaticClusterInfoProvider(Collections.singleton(clusterName));
  }
}

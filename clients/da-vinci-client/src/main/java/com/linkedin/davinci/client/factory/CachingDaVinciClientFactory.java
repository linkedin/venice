package com.linkedin.davinci.client.factory;

import static com.linkedin.venice.stats.ClientType.DAVINCI_CLIENT;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.AvroGenericSeekableDaVinciClient;
import com.linkedin.davinci.client.AvroSpecificDaVinciClient;
import com.linkedin.davinci.client.AvroSpecificSeekableDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.client.StatsAvroGenericDaVinciClient;
import com.linkedin.davinci.client.StatsAvroSpecificDaVinciClient;
import com.linkedin.davinci.client.VersionSpecificAvroGenericDaVinciClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.VeniceView;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CachingDaVinciClientFactory
    implements DaVinciClientFactory, VersionSpecificDaVinciClientFactory, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(CachingDaVinciClientFactory.class);

  protected boolean closed;
  protected final D2Client d2Client;
  private final String clusterDiscoveryD2ServiceName;
  protected final MetricsRepository metricsRepository;
  protected final VeniceProperties backendConfig;
  protected final Optional<Set<String>> managedClients;
  protected final ICProvider icProvider;
  protected final Map<String, DaVinciClient> sharedClients = new HashMap<>();
  protected final Map<String, DaVinciClient> versionSpecificClients = new HashMap<>();
  protected final List<DaVinciClient> isolatedClients = new ArrayList<>();
  protected final Map<String, DaVinciConfig> configs = new HashMap<>();
  private final Executor readChunkExecutorForLargeRequest;

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig) {
    this(d2Client, clusterDiscoveryD2ServiceName, metricsRepository, backendConfig, Optional.empty());
  }

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients) {
    this(d2Client, clusterDiscoveryD2ServiceName, metricsRepository, backendConfig, managedClients, null, null);
  }

  public CachingDaVinciClientFactory(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      MetricsRepository metricsRepository,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Executor readChunkExecutorForLargeRequest) {
    LOGGER.info(
        "Creating client factory, managedClients={}, existingMetrics={}",
        managedClients,
        metricsRepository.metrics().keySet());
    this.d2Client = d2Client;
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
    this.metricsRepository = metricsRepository;
    this.backendConfig = backendConfig;
    this.managedClients = managedClients;
    this.icProvider = icProvider;
    this.readChunkExecutorForLargeRequest = readChunkExecutorForLargeRequest;
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

  private Class getClientClass(DaVinciConfig daVinciConfig, boolean isSpecific) {
    boolean readMetricsEnabled = daVinciConfig.isReadMetricsEnabled();
    if (isSpecific) {
      return readMetricsEnabled ? StatsAvroSpecificDaVinciClient.class : AvroSpecificDaVinciClient.class;
    }
    return readMetricsEnabled ? StatsAvroGenericDaVinciClient.class : AvroGenericDaVinciClient.class;
  }

  // Generic Avro client creation methods below
  @Override
  public <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(
        storeName,
        null,
        config,
        null,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        false);
  }

  public <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config, Class<V> valueClass) {
    return getClient(
        storeName,
        null,
        config,
        valueClass,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        false);
  }

  @Override
  public <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(String storeName, DaVinciConfig config) {
    return getClient(
        storeName,
        null,
        config,
        null,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        true);
  }

  public <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        null,
        config,
        valueClass,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        true);
  }

  @Override
  public <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, String viewName, DaVinciConfig config) {
    return getClient(
        storeName,
        viewName,
        config,
        null,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        false);
  }

  @Override
  public <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(
      String storeName,
      String viewName,
      DaVinciConfig config) {
    return getClient(
        storeName,
        viewName,
        config,
        null,
        new GenericDaVinciClientConstructor<>(),
        getClientClass(config, false),
        true);
  }

  // Specific Avro client creation methods below
  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        null,
        config,
        valueClass,
        new SpecificDaVinciClientConstructor<>(),
        getClientClass(config, true),
        false);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        null,
        config,
        valueClass,
        new SpecificDaVinciClientConstructor<>(),
        getClientClass(config, true),
        true);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(
      String storeName,
      String viewName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        viewName,
        config,
        valueClass,
        new SpecificDaVinciClientConstructor<>(),
        getClientClass(config, true),
        false);
  }

  @Override
  public <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClient(
      String storeName,
      String viewName,
      DaVinciConfig config,
      Class<V> valueClass) {
    return getClient(
        storeName,
        viewName,
        config,
        valueClass,
        new SpecificDaVinciClientConstructor<>(),
        getClientClass(config, true),
        true);
  }

  // Version specific client creation methods below
  public <K, V> SeekableDaVinciClient<K, V> getVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        storeVersion,
        null,
        config,
        null,
        new VersionSpecificGenericDaVinciClientConstructor<>(storeVersion),
        VersionSpecificAvroGenericDaVinciClient.class,
        false);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getAndStartVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        storeVersion,
        null,
        config,
        null,
        new VersionSpecificGenericDaVinciClientConstructor<>(storeVersion),
        VersionSpecificAvroGenericDaVinciClient.class,
        true);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      String viewName,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        storeVersion,
        viewName,
        config,
        null,
        new VersionSpecificGenericDaVinciClientConstructor<>(storeVersion),
        VersionSpecificAvroGenericDaVinciClient.class,
        false);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getAndStartVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      String viewName,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        storeVersion,
        viewName,
        config,
        null,
        new VersionSpecificGenericDaVinciClientConstructor<>(storeVersion),
        VersionSpecificAvroGenericDaVinciClient.class,
        true);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public VeniceProperties getBackendConfig() {
    return backendConfig;
  }

  protected interface DaVinciClientConstructor {
    DaVinciClient apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider);
  }

  class GenericDaVinciClientConstructor<K, V> implements DaVinciClientConstructor {
    @Override
    public DaVinciClient<K, V> apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider) {
      AvroGenericDaVinciClient<K, V> client = new AvroGenericDaVinciClient<>(
          config,
          clientConfig,
          backendConfig,
          managedClients,
          icProvider,
          readChunkExecutorForLargeRequest);
      if (config.isReadMetricsEnabled()) {
        return new StatsAvroGenericDaVinciClient<>(client, clientConfig);
      }
      return client;
    }
  }

  class GenericSeekableDaVinciClientConstructor<K, V> implements DaVinciClientConstructor {
    @Override
    public DaVinciClient<K, V> apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider) {
      // For seekable client, we currently do not provide a stats-wrapping implementation.
      return new AvroGenericSeekableDaVinciClient<>(
          config,
          clientConfig,
          backendConfig,
          managedClients,
          icProvider,
          readChunkExecutorForLargeRequest,
          null);
    }
  }

  class SpecificSeekableDaVinciClientConstructor<K, V extends SpecificRecord> implements DaVinciClientConstructor {
    @Override
    public DaVinciClient<K, V> apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider) {
      return new AvroSpecificSeekableDaVinciClient<>(
          config,
          clientConfig,
          backendConfig,
          managedClients,
          icProvider,
          readChunkExecutorForLargeRequest);
    }
  }

  class SpecificDaVinciClientConstructor<K, V extends SpecificRecord> implements DaVinciClientConstructor {
    @Override
    public DaVinciClient<K, V> apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider) {
      AvroSpecificDaVinciClient<K, V> client = new AvroSpecificDaVinciClient<>(
          config,
          clientConfig,
          backendConfig,
          managedClients,
          icProvider,
          readChunkExecutorForLargeRequest);
      if (config.isReadMetricsEnabled()) {
        return new StatsAvroSpecificDaVinciClient<>(client, clientConfig);
      }
      return client;
    }
  }

  class VersionSpecificGenericDaVinciClientConstructor<K, V> implements DaVinciClientConstructor {
    private final int storeVersion;

    public VersionSpecificGenericDaVinciClientConstructor(int storeVersion) {
      this.storeVersion = storeVersion;
    }

    @Override
    public DaVinciClient<K, V> apply(
        DaVinciConfig config,
        ClientConfig clientConfig,
        VeniceProperties backendConfig,
        Optional<Set<String>> managedClients,
        ICProvider icProvider) {
      // For seekable client, we currently do not provide a stats-wrapping implementation.
      return new VersionSpecificAvroGenericDaVinciClient<>(
          config,
          clientConfig,
          backendConfig,
          managedClients,
          icProvider,
          readChunkExecutorForLargeRequest,
          storeVersion);
    }
  }

  protected DaVinciClient getClient(
      String storeName,
      String viewName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass,
      boolean startClient) {
    return getClient(storeName, null, viewName, config, valueClass, clientConstructor, clientClass, startClient);
  }

  /**
   * @param storeVersion Pass in a non-null value if you want a version specific {@link DaVinciClient}
   */
  protected synchronized DaVinciClient getClient(
      String storeName,
      Integer storeVersion,
      String viewName,
      DaVinciConfig config,
      Class valueClass,
      DaVinciClientConstructor clientConstructor,
      Class clientClass,
      boolean startClient) {
    String internalStoreName =
        (viewName == null || viewName.isEmpty()) ? storeName : VeniceView.getViewStoreName(storeName, viewName);
    if (closed) {
      throw new VeniceException("Unable to get a client from a closed factory, storeName=" + internalStoreName);
    }

    DaVinciConfig originalConfig = configs.computeIfAbsent(internalStoreName, k -> config);
    if (originalConfig.isManaged() != config.isManaged()) {
      throw new VeniceException(
          "Managed flag conflict" + ", storeName=" + internalStoreName + ", original=" + originalConfig.isManaged()
              + ", requested=" + config.isManaged());
    }

    if (originalConfig.getStorageClass() != config.getStorageClass()) {
      throw new VeniceException(
          "Storage class conflict" + ", storeName=" + internalStoreName + ", original="
              + originalConfig.getStorageClass() + ", requested=" + config.getStorageClass());
    }

    ClientConfig clientConfig = new ClientConfig(internalStoreName).setD2Client(d2Client)
        .setD2ServiceName(clusterDiscoveryD2ServiceName)
        .setMetricsRepository(metricsRepository)
        .setSpecificValueClass(valueClass)
        .setUseRequestBasedMetaRepository(config.isUseRequestBasedMetaRepository());

    DaVinciClient client;
    if (config.isIsolated()) {
      String statsPrefix = DAVINCI_CLIENT.getName() + "-" + isolatedClients.size();
      clientConfig.setStatsPrefix(statsPrefix);
      client = clientConstructor.apply(config, clientConfig, backendConfig, managedClients, icProvider);
      isolatedClients.add(client);
    } else {
      if (storeVersion != null) {
        String versionSpecificStoreName = internalStoreName + "v" + storeVersion;
        client = versionSpecificClients.computeIfAbsent(
            versionSpecificStoreName,
            k -> clientConstructor.apply(config, clientConfig, backendConfig, managedClients, icProvider));

      } else {
        client = sharedClients.computeIfAbsent(
            internalStoreName,
            k -> clientConstructor.apply(config, clientConfig, backendConfig, managedClients, icProvider));
      }

      if (!clientClass.isInstance(client)) {
        throw new VeniceException(
            "Client type conflict" + ", storeName=" + internalStoreName + ", originalClientClass=" + client.getClass()
                + ", requestedClientClass=" + clientClass);
      }
    }

    if (startClient) {
      client.start();
    }
    return client;
  }

  // Seekable Avro client creation methods
  public <K, V> SeekableDaVinciClient<K, V> getGenericSeekableAvroClient(String storeName, DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        null,
        config,
        null,
        new GenericSeekableDaVinciClientConstructor<>(),
        AvroGenericSeekableDaVinciClient.class,
        false);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getGenericSeekableAvroClient(
      String storeName,
      String viewName,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        viewName,
        config,
        null,
        new GenericSeekableDaVinciClientConstructor<>(),
        AvroGenericSeekableDaVinciClient.class,
        false);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getAndStartGenericSeekableAvroClient(
      String storeName,
      DaVinciConfig config) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        null,
        config,
        null,
        new GenericSeekableDaVinciClientConstructor<>(),
        AvroGenericSeekableDaVinciClient.class,
        true);
    return (SeekableDaVinciClient<K, V>) client;
  }

  public <K, V> SeekableDaVinciClient<K, V> getSpecificSeekableAvroClient(
      String storeName,
      String viewName,
      DaVinciConfig config,
      Class<V> valueClass) {
    DaVinciClient<K, V> client = getClient(
        storeName,
        viewName,
        config,
        valueClass,
        new SpecificSeekableDaVinciClientConstructor<>(),
        AvroSpecificSeekableDaVinciClient.class,
        false);
    return (SeekableDaVinciClient<K, V>) client;
  }
}

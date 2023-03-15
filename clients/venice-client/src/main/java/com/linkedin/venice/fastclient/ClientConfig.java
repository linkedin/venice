package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.ClientRoutingStrategy;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;


public class ClientConfig<K, V, T extends SpecificRecord> {
  private final Client r2Client;
  private final String statsPrefix;
  private final boolean speculativeQueryEnabled;
  private final Class<T> specificValueClass;
  private final String storeName;
  private final Map<RequestType, FastClientStats> clientStatsMap = new VeniceConcurrentHashMap<>();
  private final Executor deserializationExecutor;
  private final ClientRoutingStrategy clientRoutingStrategy;
  /**
   * For dual-read support.
   */
  private final boolean dualReadEnabled;
  private final AvroGenericStoreClient<K, V> genericThinClient;
  private final AvroSpecificStoreClient<K, T> specificThinClient;

  /**
   * For Client Routing.
   * Please check {@link com.linkedin.venice.fastclient.meta.InstanceHealthMonitor} to find more details.
   */
  private final long routingLeakedRequestCleanupThresholdMS;
  private final long routingQuotaExceededRequestCounterResetDelayMS;
  private final long routingErrorRequestCounterResetDelayMS;
  private final long routingUnavailableRequestCounterResetDelayMS;
  private final int routingPendingRequestCounterInstanceBlockThreshold;

  /**
   * The max allowed key count in batch-get request.
   * Right now, the batch-get implementation will leverage single-get, which is inefficient, when there
   * are many keys, since the requests to the same storage node won't be reused.
   * But to temporarily unblock the first customer, we will only allow at most two keys in a batch-get request.
   */
  private final int maxAllowedKeyCntInBatchGetReq;
  private final AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore;
  private final long metadataRefreshIntervalInSeconds;
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final int longTailRetryThresholdForSingleGetInMicroSeconds;
  private final int longTailRetryThresholdForBatchGetInMicroSeconds;
  private final ClusterStats clusterStats;
  private final boolean isVsonStore;

  private ClientConfig(
      String storeName,
      Client r2Client,
      MetricsRepository metricsRepository,
      String statsPrefix,
      boolean speculativeQueryEnabled,
      Class<T> specificValueClass,
      Executor deserializationExecutor,
      ClientRoutingStrategy clientRoutingStrategy,
      boolean dualReadEnabled,
      AvroGenericStoreClient<K, V> genericThinClient,
      AvroSpecificStoreClient<K, T> specificThinClient,
      long routingLeakedRequestCleanupThresholdMS,
      long routingQuotaExceededRequestCounterResetDelayMS,
      long routingErrorRequestCounterResetDelayMS,
      long routingUnavailableRequestCounterResetDelayMS,
      int routingPendingRequestCounterInstanceBlockThreshold,
      int maxAllowedKeyCntInBatchGetReq,
      AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore,
      long metadataRefreshIntervalInSeconds,
      boolean longTailRetryEnabledForSingleGet,
      int longTailRetryThresholdForSingleGetInMicroSeconds,
      boolean longTailRetryEnabledForBatchGet,
      int longTailRetryThresholdForBatchGetInMicroSeconds,
      boolean isVsonStore) {
    if (storeName == null || storeName.isEmpty()) {
      throw new VeniceClientException("storeName param shouldn't be empty");
    }
    if (r2Client == null) {
      throw new VeniceClientException("r2Client param shouldn't be null");
    }
    this.r2Client = r2Client;
    this.storeName = storeName;
    this.statsPrefix = (statsPrefix == null ? "" : statsPrefix);
    if (metricsRepository == null) {
      metricsRepository = new MetricsRepository();
    }
    // TODO consider changing the implementation or make it explicit that the config builder can only build once with
    // the same metricsRepository
    for (RequestType requestType: RequestType.values()) {
      clientStatsMap.put(
          requestType,
          FastClientStats.getClientStats(metricsRepository, this.statsPrefix, storeName, requestType));
    }
    this.clusterStats = new ClusterStats(metricsRepository, storeName);
    this.speculativeQueryEnabled = speculativeQueryEnabled;
    this.specificValueClass = specificValueClass;
    this.deserializationExecutor = deserializationExecutor;
    this.clientRoutingStrategy = clientRoutingStrategy;
    this.dualReadEnabled = dualReadEnabled;
    this.genericThinClient = genericThinClient;
    this.specificThinClient = specificThinClient;
    if (this.dualReadEnabled) {
      if (this.specificThinClient == null && this.genericThinClient == null) {
        throw new VeniceClientException(
            "Either param: specificThinClient or param: genericThinClient"
                + " should be specified when dual read is enabled");
      }
    } else {
      if (this.specificThinClient != null || this.genericThinClient != null) {
        throw new VeniceClientException(
            "Both param: specificThinClient and param: genericThinClient"
                + " should not be specified when dual read is not enabled");
      }
    }

    this.routingLeakedRequestCleanupThresholdMS = routingLeakedRequestCleanupThresholdMS > 0
        ? routingLeakedRequestCleanupThresholdMS
        : TimeUnit.SECONDS.toMillis(30); // 30 seconds by default
    this.routingQuotaExceededRequestCounterResetDelayMS =
        routingQuotaExceededRequestCounterResetDelayMS > 0 ? routingQuotaExceededRequestCounterResetDelayMS : 50; // 50
                                                                                                                  // ms
                                                                                                                  // by
                                                                                                                  // default
    this.routingErrorRequestCounterResetDelayMS = routingErrorRequestCounterResetDelayMS > 0
        ? routingErrorRequestCounterResetDelayMS
        : TimeUnit.SECONDS.toMillis(10); // 10 seconds
    this.routingUnavailableRequestCounterResetDelayMS = routingUnavailableRequestCounterResetDelayMS > 0
        ? routingUnavailableRequestCounterResetDelayMS
        : TimeUnit.SECONDS.toMillis(10); // 10 seconds
    this.routingPendingRequestCounterInstanceBlockThreshold = routingPendingRequestCounterInstanceBlockThreshold > 0
        ? routingPendingRequestCounterInstanceBlockThreshold
        : 50;

    this.maxAllowedKeyCntInBatchGetReq = maxAllowedKeyCntInBatchGetReq;

    this.thinClientForMetaStore = thinClientForMetaStore;
    this.metadataRefreshIntervalInSeconds = metadataRefreshIntervalInSeconds;

    this.longTailRetryEnabledForSingleGet = longTailRetryEnabledForSingleGet;
    this.longTailRetryThresholdForSingleGetInMicroSeconds = longTailRetryThresholdForSingleGetInMicroSeconds;

    this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
    this.longTailRetryThresholdForBatchGetInMicroSeconds = longTailRetryThresholdForBatchGetInMicroSeconds;

    if (this.longTailRetryEnabledForSingleGet) {
      if (this.longTailRetryThresholdForSingleGetInMicroSeconds <= 0) {
        throw new VeniceClientException(
            "longTailRetryThresholdForSingleGetInMicroSeconds must be positive, but got: "
                + this.longTailRetryThresholdForSingleGetInMicroSeconds);
      }
    }

    if (this.longTailRetryEnabledForBatchGet) {
      if (this.longTailRetryThresholdForBatchGetInMicroSeconds <= 0) {
        throw new VeniceClientException(
            "longTailRetryThresholdForBatchGetInMicroSeconds must be positive, but got: "
                + this.longTailRetryThresholdForBatchGetInMicroSeconds);
      }
    }

    // TODO: Need to check whether this case applies for BatchGet
    if (this.speculativeQueryEnabled && this.longTailRetryEnabledForSingleGet) {
      throw new VeniceClientException(
          "Speculative query feature can't be enabled together with long-tail retry for single-get");
    }

    this.isVsonStore = isVsonStore;
  }

  public String getStoreName() {
    return storeName;
  }

  public Client getR2Client() {
    return r2Client;
  }

  public FastClientStats getStats(RequestType requestType) {
    return clientStatsMap.get(requestType);
  }

  public boolean isSpeculativeQueryEnabled() {
    return speculativeQueryEnabled;
  }

  public Class<T> getSpecificValueClass() {
    return specificValueClass;
  }

  public Executor getDeserializationExecutor() {
    return deserializationExecutor;
  }

  public boolean isDualReadEnabled() {
    return dualReadEnabled;
  }

  public AvroGenericStoreClient<K, V> getGenericThinClient() {
    return genericThinClient;
  }

  public AvroSpecificStoreClient<K, T> getSpecificThinClient() {
    return specificThinClient;
  }

  public long getRoutingLeakedRequestCleanupThresholdMS() {
    return routingLeakedRequestCleanupThresholdMS;
  }

  public long getRoutingQuotaExceededRequestCounterResetDelayMS() {
    return routingQuotaExceededRequestCounterResetDelayMS;
  }

  public long getRoutingErrorRequestCounterResetDelayMS() {
    return routingErrorRequestCounterResetDelayMS;
  }

  public long getRoutingUnavailableRequestCounterResetDelayMS() {
    return routingUnavailableRequestCounterResetDelayMS;
  }

  public int getRoutingPendingRequestCounterInstanceBlockThreshold() {
    return routingPendingRequestCounterInstanceBlockThreshold;
  }

  public int getMaxAllowedKeyCntInBatchGetReq() {
    return maxAllowedKeyCntInBatchGetReq;
  }

  public AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getThinClientForMetaStore() {
    return thinClientForMetaStore;
  }

  public long getMetadataRefreshIntervalInSeconds() {
    return metadataRefreshIntervalInSeconds;
  }

  public boolean isLongTailRetryEnabledForSingleGet() {
    return longTailRetryEnabledForSingleGet;
  }

  public int getLongTailRetryThresholdForSingleGetInMicroSeconds() {
    return longTailRetryThresholdForSingleGetInMicroSeconds;
  }

  public boolean isLongTailRetryEnabledForBatchGet() {
    return longTailRetryEnabledForBatchGet;
  }

  public int getLongTailRetryThresholdForBatchGetInMicroSeconds() {
    return longTailRetryThresholdForBatchGetInMicroSeconds;
  }

  @Deprecated
  public boolean isVsonStore() {
    return isVsonStore;
  }

  public ClientRoutingStrategy getClientRoutingStrategy() {
    return clientRoutingStrategy;
  }

  public ClusterStats getClusterStats() {
    return this.clusterStats;
  }

  public static class ClientConfigBuilder<K, V, T extends SpecificRecord> {
    private MetricsRepository metricsRepository;
    private String statsPrefix = "";
    private boolean speculativeQueryEnabled = false;
    private Class<T> specificValueClass;
    private String storeName;
    private Executor deserializationExecutor;
    private ClientRoutingStrategy clientRoutingStrategy;
    private Client r2Client;
    private boolean dualReadEnabled = false;
    private AvroGenericStoreClient<K, V> genericThinClient;
    private AvroSpecificStoreClient<K, T> specificThinClient;

    private long routingLeakedRequestCleanupThresholdMS = -1;
    private long routingQuotaExceededRequestCounterResetDelayMS = -1;
    private long routingErrorRequestCounterResetDelayMS = -1;
    private long routingUnavailableRequestCounterResetDelayMS = -1;
    private int routingPendingRequestCounterInstanceBlockThreshold = -1;
    /**
     * TODO:
     * maxAllowedKeyCntInBatchGetReq was set to 2 initially for singleGet based multiGet
     * for a specific customer ask. This needs to be reevaluated for streamingBatchGet().
     * Today, the batch-get size for thinclient is enforced in Venice Router via
     * {@link VenicePathParser#getBatchGetLimit} and it is configurable in store-level.
     * In Fast-Client, it is still an open question about how to setup the batch-get limit
     * or whether we need any limit at all. To start with, this can be set similar to routers
     * global config and evaluate from there.
     */
    private int maxAllowedKeyCntInBatchGetReq = 2;

    private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore;

    private long metadataRefreshIntervalInSeconds = -1;

    private boolean longTailRetryEnabledForSingleGet = false;
    private int longTailRetryThresholdForSingleGetInMicroSeconds = 1000; // 1ms.

    private boolean longTailRetryEnabledForBatchGet = false;
    private int longTailRetryThresholdForBatchGetInMicroSeconds = 10000; // 10ms.

    private boolean isVsonStore = false;

    public ClientConfigBuilder<K, V, T> setStoreName(String storeName) {
      this.storeName = storeName;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStatsPrefix(String statsPrefix) {
      this.statsPrefix = statsPrefix;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setSpeculativeQueryEnabled(boolean speculativeQueryEnabled) {
      this.speculativeQueryEnabled = speculativeQueryEnabled;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setSpecificValueClass(Class<T> specificValueClass) {
      this.specificValueClass = specificValueClass;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setDeserializationExecutor(Executor deserializationExecutor) {
      this.deserializationExecutor = deserializationExecutor;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setClientRoutingStrategy(ClientRoutingStrategy clientRoutingStrategy) {
      this.clientRoutingStrategy = clientRoutingStrategy;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setR2Client(Client r2Client) {
      this.r2Client = r2Client;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setDualReadEnabled(boolean dualReadEnabled) {
      this.dualReadEnabled = dualReadEnabled;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setGenericThinClient(AvroGenericStoreClient<K, V> genericThinClient) {
      this.genericThinClient = genericThinClient;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setSpecificThinClient(AvroSpecificStoreClient<K, T> specificThinClient) {
      this.specificThinClient = specificThinClient;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRoutingLeakedRequestCleanupThresholdMS(
        long routingLeakedRequestCleanupThresholdMS) {
      this.routingLeakedRequestCleanupThresholdMS = routingLeakedRequestCleanupThresholdMS;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRoutingQuotaExceededRequestCounterResetDelayMS(
        long routingQuotaExceededRequestCounterResetDelayMS) {
      this.routingQuotaExceededRequestCounterResetDelayMS = routingQuotaExceededRequestCounterResetDelayMS;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRoutingErrorRequestCounterResetDelayMS(
        long routingErrorRequestCounterResetDelayMS) {
      this.routingErrorRequestCounterResetDelayMS = routingErrorRequestCounterResetDelayMS;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRoutingUnavailableRequestCounterResetDelayMS(
        long routingUnavailableRequestCounterResetDelayMS) {
      this.routingUnavailableRequestCounterResetDelayMS = routingUnavailableRequestCounterResetDelayMS;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRoutingPendingRequestCounterInstanceBlockThreshold(
        int routingPendingRequestCounterInstanceBlockThreshold) {
      this.routingPendingRequestCounterInstanceBlockThreshold = routingPendingRequestCounterInstanceBlockThreshold;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setThinClientForMetaStore(
        AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore) {
      this.thinClientForMetaStore = thinClientForMetaStore;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMetadataRefreshIntervalInSeconds(long metadataRefreshIntervalInSeconds) {
      this.metadataRefreshIntervalInSeconds = metadataRefreshIntervalInSeconds;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMaxAllowedKeyCntInBatchGetReq(int maxAllowedKeyCntInBatchGetReq) {
      this.maxAllowedKeyCntInBatchGetReq = maxAllowedKeyCntInBatchGetReq;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryEnabledForSingleGet(boolean longTailRetryEnabledForSingleGet) {
      this.longTailRetryEnabledForSingleGet = longTailRetryEnabledForSingleGet;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryThresholdForSingleGetInMicroSeconds(
        int longTailRetryThresholdForSingleGetInMicroSeconds) {
      this.longTailRetryThresholdForSingleGetInMicroSeconds = longTailRetryThresholdForSingleGetInMicroSeconds;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryEnabledForBatchGet(boolean longTailRetryEnabledForBatchGet) {
      this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryThresholdForBatchGetInMicroSeconds(
        int longTailRetryThresholdForBatchGetInMicroSeconds) {
      this.longTailRetryThresholdForBatchGetInMicroSeconds = longTailRetryThresholdForBatchGetInMicroSeconds;
      return this;
    }

    @Deprecated
    public ClientConfigBuilder<K, V, T> setVsonStore(boolean vsonStore) {
      isVsonStore = vsonStore;
      return this;
    }

    public ClientConfigBuilder<K, V, T> clone() {
      return new ClientConfigBuilder().setStoreName(storeName)
          .setR2Client(r2Client)
          .setMetricsRepository(metricsRepository)
          .setStatsPrefix(statsPrefix)
          .setSpeculativeQueryEnabled(speculativeQueryEnabled)
          .setSpecificValueClass(specificValueClass)
          .setDeserializationExecutor(deserializationExecutor)
          .setClientRoutingStrategy(clientRoutingStrategy)
          .setDualReadEnabled(dualReadEnabled)
          .setGenericThinClient(genericThinClient)
          .setSpecificThinClient(specificThinClient)
          .setRoutingLeakedRequestCleanupThresholdMS(routingLeakedRequestCleanupThresholdMS)
          .setRoutingQuotaExceededRequestCounterResetDelayMS(routingQuotaExceededRequestCounterResetDelayMS)
          .setRoutingErrorRequestCounterResetDelayMS(routingErrorRequestCounterResetDelayMS)
          .setRoutingUnavailableRequestCounterResetDelayMS(routingUnavailableRequestCounterResetDelayMS)
          .setRoutingPendingRequestCounterInstanceBlockThreshold(routingPendingRequestCounterInstanceBlockThreshold)
          .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeyCntInBatchGetReq)
          .setThinClientForMetaStore(thinClientForMetaStore)
          .setMetadataRefreshIntervalInSeconds(metadataRefreshIntervalInSeconds)
          .setLongTailRetryEnabledForSingleGet(longTailRetryEnabledForSingleGet)
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(longTailRetryThresholdForSingleGetInMicroSeconds)
          .setLongTailRetryEnabledForBatchGet(longTailRetryEnabledForBatchGet)
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(longTailRetryThresholdForBatchGetInMicroSeconds)
          .setVsonStore(isVsonStore);
    }

    public ClientConfig<K, V, T> build() {
      return new ClientConfig<>(
          storeName,
          r2Client,
          metricsRepository,
          statsPrefix,
          speculativeQueryEnabled,
          specificValueClass,
          deserializationExecutor,
          clientRoutingStrategy,
          dualReadEnabled,
          genericThinClient,
          specificThinClient,
          routingLeakedRequestCleanupThresholdMS,
          routingQuotaExceededRequestCounterResetDelayMS,
          routingErrorRequestCounterResetDelayMS,
          routingUnavailableRequestCounterResetDelayMS,
          routingPendingRequestCounterInstanceBlockThreshold,
          maxAllowedKeyCntInBatchGetReq,
          thinClientForMetaStore,
          metadataRefreshIntervalInSeconds,
          longTailRetryEnabledForSingleGet,
          longTailRetryThresholdForSingleGetInMicroSeconds,
          longTailRetryEnabledForBatchGet,
          longTailRetryThresholdForBatchGetInMicroSeconds,
          isVsonStore);
    }
  }
}

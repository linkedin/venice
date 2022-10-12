package com.linkedin.venice.fastclient;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.ClientRoutingStrategy;
import com.linkedin.venice.fastclient.stats.ClientStats;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.read.RequestType;
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
  private final Map<RequestType, ClientStats> clientStatsMap = new VeniceConcurrentHashMap<>();
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
  private final DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;
  private final long metadataRefreshInvervalInSeconds;
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final int longTailRetryThresholdForSingletGetInMicroSeconds;
  private final int longTailRetryThresholdForBatchGetInMicroSeconds;
  private final ClusterStats clusterStats;

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
      DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore,
      long metadataRefreshInvervalInSeconds,
      boolean longTailRetryEnabledForSingleGet,
      int longTailRetryThresholdForSingletGetInMicroSeconds,
      boolean longTailRetryEnabledForBatchGet,
      int longTailRetryThresholdForBatchGetInMicroSeconds) {
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
      clientStatsMap
          .put(requestType, ClientStats.getClientStats(metricsRepository, this.statsPrefix, storeName, requestType));
    }
    this.clusterStats = new ClusterStats(metricsRepository, storeName);
    this.speculativeQueryEnabled = speculativeQueryEnabled;
    this.specificValueClass = specificValueClass;
    this.deserializationExecutor = deserializationExecutor;
    this.clientRoutingStrategy = clientRoutingStrategy;
    this.dualReadEnabled = dualReadEnabled;
    this.genericThinClient = genericThinClient;
    this.specificThinClient = specificThinClient;
    if (this.dualReadEnabled && this.specificThinClient == null && this.genericThinClient == null) {
      throw new VeniceClientException(
          "Either param: specificThinClient or param: genericThinClient"
              + " should be specified when dual read is enabled");
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

    this.daVinciClientForMetaStore = daVinciClientForMetaStore;
    this.metadataRefreshInvervalInSeconds = metadataRefreshInvervalInSeconds;

    this.longTailRetryEnabledForSingleGet = longTailRetryEnabledForSingleGet;
    this.longTailRetryThresholdForSingletGetInMicroSeconds = longTailRetryThresholdForSingletGetInMicroSeconds;

    this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
    this.longTailRetryThresholdForBatchGetInMicroSeconds = longTailRetryThresholdForBatchGetInMicroSeconds;

    if (this.longTailRetryThresholdForSingletGetInMicroSeconds <= 0) {
      throw new VeniceClientException(
          "longTailRetryThresholdForSingletGetInMicroSeconds must be positive, but got: "
              + this.longTailRetryThresholdForSingletGetInMicroSeconds);
    }

    if (this.longTailRetryThresholdForBatchGetInMicroSeconds <= 0) {
      throw new VeniceClientException(
          "longTailRetryThresholdForBatchGetInMicroSeconds must be positive, but got: "
              + this.longTailRetryThresholdForBatchGetInMicroSeconds);
    }

    if (this.speculativeQueryEnabled && this.longTailRetryEnabledForSingleGet) {
      throw new VeniceClientException(
          "Speculative query feature can't be enabled together with long-tail retry for single-get");
    }
  }

  public String getStoreName() {
    return storeName;
  }

  public Client getR2Client() {
    return r2Client;
  }

  public ClientStats getStats(RequestType requestType) {
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

  public DaVinciClient<StoreMetaKey, StoreMetaValue> getDaVinciClientForMetaStore() {
    return daVinciClientForMetaStore;
  }

  public long getMetadataRefreshInvervalInSeconds() {
    return metadataRefreshInvervalInSeconds;
  }

  public boolean isLongTailRetryEnabledForSingleGet() {
    return longTailRetryEnabledForSingleGet;
  }

  public int getLongTailRetryThresholdForSingletGetInMicroSeconds() {
    return longTailRetryThresholdForSingletGetInMicroSeconds;
  }

  public boolean isLongTailRetryEnabledForBatchGet() {
    return longTailRetryEnabledForBatchGet;
  }

  public int getLongTailRetryThresholdForBatchGetInMicroSeconds() {
    return longTailRetryThresholdForBatchGetInMicroSeconds;
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

    private int maxAllowedKeyCntInBatchGetReq = 2;

    private DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;

    private long metadataRefreshIntervalInSeconds = -1;

    private boolean longTailRetryEnabledForSingleGet = false;
    private int longTailRetryThresholdForSingletGetInMicroSeconds = 1000; // 1ms.

    private boolean longTailRetryEnabledForBatchGet = false;
    private int longTailRetryThresholdForBatchtGetInMicroSeconds = 10000; // 10ms.

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

    public ClientConfigBuilder<K, V, T> setDaVinciClientForMetaStore(
        DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore) {
      this.daVinciClientForMetaStore = daVinciClientForMetaStore;
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

    public ClientConfigBuilder<K, V, T> setLongTailRetryThresholdForSingletGetInMicroSeconds(
        int longTailRetryThresholdForSingletGetInMicroSeconds) {
      this.longTailRetryThresholdForSingletGetInMicroSeconds = longTailRetryThresholdForSingletGetInMicroSeconds;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryEnabledForBatchGet(boolean longTailRetryEnabledForBatchGet) {
      this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryThresholdForBatchGetInMicroSeconds(
        int longTailRetryThresholdForBatchGetInMicroSeconds) {
      this.longTailRetryThresholdForBatchtGetInMicroSeconds = longTailRetryThresholdForBatchGetInMicroSeconds;
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
          .setDaVinciClientForMetaStore(daVinciClientForMetaStore)
          .setMetadataRefreshIntervalInSeconds(metadataRefreshIntervalInSeconds)
          .setLongTailRetryEnabledForSingleGet(longTailRetryEnabledForSingleGet)
          .setLongTailRetryThresholdForSingletGetInMicroSeconds(longTailRetryThresholdForSingletGetInMicroSeconds)
          .setLongTailRetryEnabledForBatchGet(longTailRetryEnabledForBatchGet)
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(longTailRetryThresholdForBatchtGetInMicroSeconds);
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
          daVinciClientForMetaStore,
          metadataRefreshIntervalInSeconds,
          longTailRetryEnabledForSingleGet,
          longTailRetryThresholdForSingletGetInMicroSeconds,
          longTailRetryEnabledForBatchGet,
          longTailRetryThresholdForBatchtGetInMicroSeconds);
    }
  }
}

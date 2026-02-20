package com.linkedin.venice.fastclient;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.ClientRoutingStrategyType;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitorConfig;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClientConfig<K, V, T extends SpecificRecord> {
  private static final Logger LOGGER = LogManager.getLogger(ClientConfig.class);
  public static final String LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_BATCH_GET_IN_MILLI_SECONDS =
      "1-12:8,13-20:30,21-150:50,151-500:100,501-:500";

  public static final String LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_COMPUTE_IN_MILLI_SECONDS =
      "1-12:8,13-20:30,21-150:50,151-500:100,501-:500";

  private final Client r2Client;
  private final String statsPrefix;
  private final Class<T> specificValueClass;
  private final String storeName;
  private final Map<RequestType, FastClientStats> clientStatsMap = new VeniceConcurrentHashMap<>();
  private final Executor deserializationExecutor;
  private final ScheduledExecutorService metadataRefreshExecutor;
  private final ClientRoutingStrategyType clientRoutingStrategyType;
  /**
   * For dual-read support.
   */
  private final boolean dualReadEnabled;
  private final AvroGenericStoreClient<K, V> genericThinClient;
  private final AvroSpecificStoreClient<K, T> specificThinClient;

  private final DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;
  /**
   * Config to enable/disable warm up connection to instances from fetched metadata.
   */
  private final boolean isMetadataConnWarmupEnabled;
  /**
   * The interval in seconds to refresh metadata. If not configured, it will be set to
   * {@link com.linkedin.venice.fastclient.meta.RequestBasedMetadata#DEFAULT_REFRESH_INTERVAL_IN_SECONDS} by default.
   */
  private final long metadataRefreshIntervalInSeconds;
  /**
   * The timeout in seconds to wait to warm up connection to metadata instances. If not configured, it will be set to
   * {@link com.linkedin.venice.fastclient.meta.RequestBasedMetadata#DEFAULT_CONN_WARMUP_TIMEOUT_IN_SECONDS_DEFAULT} by default.
   */
  private final long metadataConnWarmupTimeoutInSeconds;
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final boolean longTailRetryEnabledForCompute;
  private final int longTailRetryThresholdForSingleGetInMicroSeconds;
  private final int longTailRetryThresholdForBatchGetInMicroSeconds;
  private final String longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds;
  private final String longTailRangeBasedRetryThresholdForComputeInMilliSeconds;
  private final ClusterStats clusterStats;
  private final boolean isVsonStore;
  private final StoreMetadataFetchMode storeMetadataFetchMode;
  private final D2Client d2Client;
  private final String clusterDiscoveryD2Service;
  private final boolean useGrpc;
  /**
   * This is a temporary solution to support gRPC with Venice, we will replace this with retrieving information about
   * gRPC servers when we make a request to receive Metadata from a server to obtain information in order to successfully
   * route requests to the correct server/partition
   */
  private final GrpcClientConfig grpcClientConfig;
  /**
   * The time window used to calculate user traffic and corresponding long tail retry budget. The default value is one
   * minute. Meaning it will use the average request occurrence rate over the one minute to calculate the corresponding
   * retry budget for the next minute and so on.
   */
  private final long longTailRetryBudgetEnforcementWindowInMs;

  private boolean projectionFieldValidation;
  private Set<String> harClusters;
  private final InstanceHealthMonitor instanceHealthMonitor;
  private final boolean retryBudgetEnabled;
  private final double retryBudgetPercentage;
  private final boolean enableLeastLoadedRoutingStrategyForHelixGroupRouting;

  private final MetricsRepository metricsRepository;

  private final boolean storeLoadControllerEnabled;
  private final int storeLoadControllerWindowSizeInSec;
  private final int storeLoadControllerRejectionRatioUpdateIntervalInSec;
  private final double storeLoadControllerMaxRejectionRatio;
  private final double storeLoadControllerAcceptMultiplier;

  /**
   * Optional factory for creating custom key serializers (e.g., for Protocol Buffers).
   * If not provided, the default Avro serializer will be used.
   */
  private final Optional<SerializerFactory<K>> keySerializerFactory;

  /**
   * Optional factory for creating custom value deserializers (e.g., for Protocol Buffers).
   * If not provided, the default Avro deserializer will be used.
   */
  private final Optional<DeserializerFactory<V>> valueDeserializerFactory;

  private ClientConfig(ClientConfigBuilder builder) {
    if (builder.storeName == null || builder.storeName.isEmpty()) {
      throw new VeniceClientException("storeName param shouldn't be empty");
    }
    if (builder.r2Client == null && !builder.useGrpc) {
      throw new VeniceClientException("r2Client param shouldn't be null");
    }
    if (builder.useGrpc && builder.grpcClientConfig == null) {
      throw new UnsupportedOperationException(
          "we require additional gRPC related configs when we create a gRPC enabled client");
    }

    this.r2Client = builder.r2Client;
    this.storeName = builder.storeName;
    this.statsPrefix = (builder.statsPrefix == null ? "" : builder.statsPrefix);
    this.metricsRepository = builder.metricsRepository != null
        ? builder.metricsRepository
        : MetricsRepositoryUtils.createMultiThreadedMetricsRepository();
    // TODO consider changing the implementation or make it explicit that the config builder can only build once with
    // the same metricsRepository
    for (RequestType requestType: RequestType.values()) {
      clientStatsMap.put(
          requestType,
          FastClientStats.getClientStats(this.metricsRepository, this.statsPrefix, storeName, requestType));
    }
    this.clusterStats = new ClusterStats(this.metricsRepository, storeName);
    this.specificValueClass = builder.specificValueClass;
    this.deserializationExecutor = builder.deserializationExecutor;
    this.metadataRefreshExecutor = builder.metadataRefreshExecutor;
    this.clientRoutingStrategyType = builder.clientRoutingStrategyType == null
        ? ClientRoutingStrategyType.LEAST_LOADED
        : builder.clientRoutingStrategyType;
    this.dualReadEnabled = builder.dualReadEnabled;
    this.genericThinClient = builder.genericThinClient;
    this.specificThinClient = builder.specificThinClient;
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
    this.daVinciClientForMetaStore = builder.daVinciClientForMetaStore;
    this.isMetadataConnWarmupEnabled = builder.isMetadataConnWarmupEnabled;
    this.metadataRefreshIntervalInSeconds = builder.metadataRefreshIntervalInSeconds;
    this.metadataConnWarmupTimeoutInSeconds = builder.metadataConnWarmupTimeoutInSeconds;

    this.longTailRetryEnabledForSingleGet = builder.longTailRetryEnabledForSingleGet;
    this.longTailRetryThresholdForSingleGetInMicroSeconds = builder.longTailRetryThresholdForSingleGetInMicroSeconds;

    this.longTailRetryEnabledForBatchGet = builder.longTailRetryEnabledForBatchGet;
    this.longTailRetryThresholdForBatchGetInMicroSeconds = builder.longTailRetryThresholdForBatchGetInMicroSeconds;

    this.longTailRetryEnabledForCompute = builder.longTailRetryEnabledForCompute;

    if (this.longTailRetryEnabledForSingleGet) {
      if (this.longTailRetryThresholdForSingleGetInMicroSeconds <= 0) {
        throw new VeniceClientException(
            "longTailRetryThresholdForSingleGetInMicroSeconds must be positive, but got: "
                + this.longTailRetryThresholdForSingleGetInMicroSeconds);
      }
    }

    this.isVsonStore = builder.isVsonStore;
    this.storeMetadataFetchMode = builder.storeMetadataFetchMode;
    this.d2Client = builder.d2Client;
    this.clusterDiscoveryD2Service = builder.clusterDiscoveryD2Service;
    if (this.storeMetadataFetchMode == StoreMetadataFetchMode.SERVER_BASED_METADATA) {
      if (this.d2Client == null || this.clusterDiscoveryD2Service == null) {
        throw new VeniceClientException(
            "Both param: d2Client and param: clusterDiscoveryD2Service must be set for request based metadata");
      }
    }
    if (clientRoutingStrategyType == ClientRoutingStrategyType.HELIX_ASSISTED
        && this.storeMetadataFetchMode != StoreMetadataFetchMode.SERVER_BASED_METADATA) {
      throw new VeniceClientException("Helix assisted routing is only available with server based metadata enabled");
    }

    this.useGrpc = builder.useGrpc;
    this.grpcClientConfig = builder.grpcClientConfig;

    this.projectionFieldValidation = builder.projectionFieldValidation;
    this.longTailRetryBudgetEnforcementWindowInMs = builder.longTailRetryBudgetEnforcementWindowInMs;
    this.harClusters = builder.harClusters;
    if (builder.instanceHealthMonitor == null) {
      this.instanceHealthMonitor =
          new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().setClient(this.r2Client).build());
    } else {
      this.instanceHealthMonitor = builder.instanceHealthMonitor;
    }
    this.enableLeastLoadedRoutingStrategyForHelixGroupRouting =
        builder.enableLeastLoadedRoutingStrategyForHelixGroupRouting;
    this.retryBudgetEnabled = builder.retryBudgetEnabled;
    this.retryBudgetPercentage = builder.retryBudgetPercentage;
    if (retryBudgetPercentage > 1.0 || retryBudgetPercentage < 0.0) {
      throw new VeniceClientException(
          "Invalid retryBudgetPercentage value: " + retryBudgetPercentage + ", should be in [0.0, 1.0]");
    }
    this.storeLoadControllerEnabled = builder.storeLoadControllerEnabled;
    this.storeLoadControllerWindowSizeInSec = builder.storeLoadControllerWindowSizeInSec;
    this.storeLoadControllerRejectionRatioUpdateIntervalInSec =
        builder.storeLoadControllerRejectionRatioUpdateIntervalInSec;
    this.storeLoadControllerMaxRejectionRatio = builder.storeLoadControllerMaxRejectionRatio;
    this.storeLoadControllerAcceptMultiplier = builder.storeLoadControllerAcceptMultiplier;
    this.longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds =
        builder.longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds;
    this.longTailRangeBasedRetryThresholdForComputeInMilliSeconds =
        builder.longTailRangeBasedRetryThresholdForComputeInMilliSeconds;
    this.keySerializerFactory = Optional.ofNullable(builder.keySerializerFactory);
    this.valueDeserializerFactory = Optional.ofNullable(builder.valueDeserializerFactory);
  }

  public String getStoreName() {
    return storeName;
  }

  public Client getR2Client() {
    return r2Client;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public FastClientStats getStats(RequestType requestType) {
    return clientStatsMap.get(requestType);
  }

  public Class<T> getSpecificValueClass() {
    return specificValueClass;
  }

  public Executor getDeserializationExecutor() {
    return deserializationExecutor;
  }

  public ScheduledExecutorService getMetadataRefreshExecutor() {
    return metadataRefreshExecutor;
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

  public DaVinciClient<StoreMetaKey, StoreMetaValue> getDaVinciClientForMetaStore() {
    return daVinciClientForMetaStore;
  }

  public boolean isMetadataConnWarmupEnabled() {
    return isMetadataConnWarmupEnabled;
  }

  public long getMetadataRefreshIntervalInSeconds() {
    return metadataRefreshIntervalInSeconds;
  }

  public long getMetadataConnWarmupTimeoutInSeconds() {
    return metadataConnWarmupTimeoutInSeconds;
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

  public boolean isLongTailRetryEnabledForCompute() {
    return longTailRetryEnabledForCompute;
  }

  @Deprecated
  public boolean isVsonStore() {
    return isVsonStore;
  }

  public ClientRoutingStrategyType getClientRoutingStrategyType() {
    return clientRoutingStrategyType;
  }

  public ClusterStats getClusterStats() {
    return this.clusterStats;
  }

  public StoreMetadataFetchMode getStoreMetadataFetchMode() {
    return this.storeMetadataFetchMode;
  }

  public D2Client getD2Client() {
    return this.d2Client;
  }

  public String getClusterDiscoveryD2Service() {
    return this.clusterDiscoveryD2Service;
  }

  public boolean useGrpc() {
    return useGrpc;
  }

  public GrpcClientConfig getGrpcClientConfig() {
    return grpcClientConfig;
  }

  public boolean isProjectionFieldValidationEnabled() {
    return projectionFieldValidation;
  }

  public long getLongTailRetryBudgetEnforcementWindowInMs() {
    return longTailRetryBudgetEnforcementWindowInMs;
  }

  public Set<String> getHarClusters() {
    return Collections.unmodifiableSet(harClusters);
  }

  public ClientConfig setProjectionFieldValidationEnabled(boolean projectionFieldValidation) {
    this.projectionFieldValidation = projectionFieldValidation;
    return this;
  }

  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  public boolean isRetryBudgetEnabled() {
    return retryBudgetEnabled;
  }

  public double getRetryBudgetPercentage() {
    return retryBudgetPercentage;
  }

  public boolean isEnableLeastLoadedRoutingStrategyForHelixGroupRouting() {
    return enableLeastLoadedRoutingStrategyForHelixGroupRouting;
  }

  public boolean isStoreLoadControllerEnabled() {
    return storeLoadControllerEnabled;
  }

  public int getStoreLoadControllerWindowSizeInSec() {
    return storeLoadControllerWindowSizeInSec;
  }

  public int getStoreLoadControllerRejectionRatioUpdateIntervalInSec() {
    return storeLoadControllerRejectionRatioUpdateIntervalInSec;
  }

  public double getStoreLoadControllerMaxRejectionRatio() {
    return storeLoadControllerMaxRejectionRatio;
  }

  public double getStoreLoadControllerAcceptMultiplier() {
    return storeLoadControllerAcceptMultiplier;
  }

  public String getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds() {
    return longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds;
  }

  public String getLongTailRangeBasedRetryThresholdForComputeInMilliSeconds() {
    return longTailRangeBasedRetryThresholdForComputeInMilliSeconds;
  }

  public Optional<SerializerFactory<K>> getKeySerializerFactory() {
    return keySerializerFactory;
  }

  public Optional<DeserializerFactory<V>> getValueDeserializerFactory() {
    return valueDeserializerFactory;
  }

  public static class ClientConfigBuilder<K, V, T extends SpecificRecord> {
    private MetricsRepository metricsRepository;
    private String statsPrefix = "";
    private Class<T> specificValueClass;
    private String storeName;
    private Executor deserializationExecutor;
    private ScheduledExecutorService metadataRefreshExecutor;
    private ClientRoutingStrategyType clientRoutingStrategyType;
    private Client r2Client;
    private boolean dualReadEnabled = false;
    private AvroGenericStoreClient<K, V> genericThinClient;
    private AvroSpecificStoreClient<K, T> specificThinClient;
    private DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;

    private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore;

    private boolean isMetadataConnWarmupEnabled = true;
    private long metadataRefreshIntervalInSeconds = -1;
    private long metadataConnWarmupTimeoutInSeconds = -1;

    private boolean longTailRetryEnabledForSingleGet = false;
    private int longTailRetryThresholdForSingleGetInMicroSeconds = 1000; // 1ms.

    private boolean longTailRetryEnabledForBatchGet = false;
    private int longTailRetryThresholdForBatchGetInMicroSeconds = 0;

    private String longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds =
        LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_BATCH_GET_IN_MILLI_SECONDS;

    private boolean longTailRetryEnabledForCompute = false;
    private String longTailRangeBasedRetryThresholdForComputeInMilliSeconds =
        LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_COMPUTE_IN_MILLI_SECONDS;

    private boolean isVsonStore = false;
    private StoreMetadataFetchMode storeMetadataFetchMode = StoreMetadataFetchMode.SERVER_BASED_METADATA;
    private D2Client d2Client;
    private String clusterDiscoveryD2Service;
    private boolean useGrpc = false;
    private GrpcClientConfig grpcClientConfig = null;

    private boolean projectionFieldValidation = true;

    private long longTailRetryBudgetEnforcementWindowInMs = 60000; // 1 minute

    private Set<String> harClusters = Collections.EMPTY_SET;

    private InstanceHealthMonitor instanceHealthMonitor;

    private boolean retryBudgetEnabled = true;

    // Default value of 0.1 meaning only 10 percent of the user requests are allowed to trigger long tail retry
    private double retryBudgetPercentage = 0.1d;

    private boolean enableLeastLoadedRoutingStrategyForHelixGroupRouting = true;
    private boolean storeLoadControllerEnabled = false;
    private int storeLoadControllerWindowSizeInSec = 30;
    private int storeLoadControllerRejectionRatioUpdateIntervalInSec = 3;
    private double storeLoadControllerMaxRejectionRatio = 0.9;
    private double storeLoadControllerAcceptMultiplier = 2.0;

    private SerializerFactory<K> keySerializerFactory = null;
    private DeserializerFactory<V> valueDeserializerFactory = null;

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

    public ClientConfigBuilder<K, V, T> setSpecificValueClass(Class<T> specificValueClass) {
      this.specificValueClass = specificValueClass;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setDeserializationExecutor(Executor deserializationExecutor) {
      this.deserializationExecutor = deserializationExecutor;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMetadataRefreshExecutor(ScheduledExecutorService metadataRefreshExecutor) {
      this.metadataRefreshExecutor = metadataRefreshExecutor;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setClientRoutingStrategyType(
        ClientRoutingStrategyType clientRoutingStrategyType) {
      this.clientRoutingStrategyType = clientRoutingStrategyType;
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

    public ClientConfigBuilder<K, V, T> setDaVinciClientForMetaStore(
        DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore) {
      this.daVinciClientForMetaStore = daVinciClientForMetaStore;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setThinClientForMetaStore(
        AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore) {
      this.thinClientForMetaStore = thinClientForMetaStore;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setIsMetadataConnWarmupEnabled(boolean isMetadataConnWarmupEnabled) {
      this.isMetadataConnWarmupEnabled = isMetadataConnWarmupEnabled;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMetadataRefreshIntervalInSeconds(long metadataRefreshIntervalInSeconds) {
      this.metadataRefreshIntervalInSeconds = metadataRefreshIntervalInSeconds;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setMetadataConnWarmupTimeoutInSeconds(long metadataConnWarmupTimeoutInSeconds) {
      this.metadataConnWarmupTimeoutInSeconds = metadataConnWarmupTimeoutInSeconds;
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

    public ClientConfigBuilder<K, V, T> setLongTailRetryEnabledForCompute(boolean longTailRetryEnabledForCompute) {
      this.longTailRetryEnabledForCompute = longTailRetryEnabledForCompute;
      return this;
    }

    @Deprecated
    public ClientConfigBuilder<K, V, T> setVsonStore(boolean vsonStore) {
      isVsonStore = vsonStore;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreMetadataFetchMode(StoreMetadataFetchMode storeMetadataFetchMode) {
      this.storeMetadataFetchMode = storeMetadataFetchMode;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setD2Client(D2Client d2Client) {
      this.d2Client = d2Client;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setClusterDiscoveryD2Service(String clusterDiscoveryD2Service) {
      this.clusterDiscoveryD2Service = clusterDiscoveryD2Service;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setUseGrpc(boolean useGrpc) {
      this.useGrpc = useGrpc;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setGrpcClientConfig(GrpcClientConfig grpcClientConfig) {
      this.grpcClientConfig = grpcClientConfig;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setProjectionFieldValidationEnabled(boolean projectionFieldValidation) {
      this.projectionFieldValidation = projectionFieldValidation;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRetryBudgetEnforcementWindowInMs(
        long longTailRetryBudgetEnforcementWindowInMs) {
      this.longTailRetryBudgetEnforcementWindowInMs = longTailRetryBudgetEnforcementWindowInMs;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setHARClusters(Set<String> clusters) {
      this.harClusters = clusters;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setInstanceHealthMonitor(InstanceHealthMonitor instanceHealthMonitor) {
      this.instanceHealthMonitor = instanceHealthMonitor;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRetryBudgetEnabled(boolean retryBudgetEnabled) {
      this.retryBudgetEnabled = retryBudgetEnabled;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setRetryBudgetPercentage(double retryBudgetPercentage) {
      this.retryBudgetPercentage = retryBudgetPercentage;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setEnableLeastLoadedRoutingStrategyForHelixGroupRouting(
        boolean enableLeastLoadedRoutingStrategyForHelixGroupRouting) {
      this.enableLeastLoadedRoutingStrategyForHelixGroupRouting = enableLeastLoadedRoutingStrategyForHelixGroupRouting;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreLoadControllerEnabled(boolean storeLoadControllerEnabled) {
      this.storeLoadControllerEnabled = storeLoadControllerEnabled;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreLoadControllerWindowSizeInSec(int storeLoadControllerWindowSizeInSec) {
      this.storeLoadControllerWindowSizeInSec = storeLoadControllerWindowSizeInSec;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreLoadControllerRejectionRatioUpdateIntervalInSec(
        int storeLoadControllerRejectionRatioUpdateIntervalInSec) {
      this.storeLoadControllerRejectionRatioUpdateIntervalInSec = storeLoadControllerRejectionRatioUpdateIntervalInSec;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreLoadControllerMaxRejectionRatio(
        double storeLoadControllerMaxRejectionRatio) {
      this.storeLoadControllerMaxRejectionRatio = storeLoadControllerMaxRejectionRatio;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setStoreLoadControllerAcceptMultiplier(
        double storeLoadControllerAcceptMultiplier) {
      this.storeLoadControllerAcceptMultiplier = storeLoadControllerAcceptMultiplier;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(
        String longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds) {
      this.longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds =
          longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds;
      return this;
    }

    public ClientConfigBuilder<K, V, T> setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(
        String longTailRangeBasedRetryThresholdForComputeInMilliSeconds) {
      this.longTailRangeBasedRetryThresholdForComputeInMilliSeconds =
          longTailRangeBasedRetryThresholdForComputeInMilliSeconds;
      return this;
    }

    /**
     * Set a custom key serializer factory.
     * This allows using custom serialization formats (e.g., Protocol Buffers) instead of Avro.
     *
     * @param keySerializerFactory the factory to create key serializers, or null to use default Avro serializers
     * @return this builder
     */
    public ClientConfigBuilder<K, V, T> setKeySerializerFactory(SerializerFactory<K> keySerializerFactory) {
      this.keySerializerFactory = keySerializerFactory;
      return this;
    }

    /**
     * Set a custom value deserializer factory.
     * This allows using custom deserialization formats (e.g., Protocol Buffers) instead of Avro.
     *
     * @param valueDeserializerFactory the factory to create value deserializers, or null to use default Avro deserializers
     * @return this builder
     */
    public ClientConfigBuilder<K, V, T> setValueDeserializerFactory(DeserializerFactory<V> valueDeserializerFactory) {
      this.valueDeserializerFactory = valueDeserializerFactory;
      return this;
    }

    public ClientConfigBuilder<K, V, T> clone() {
      return new ClientConfigBuilder().setStoreName(storeName)
          .setR2Client(r2Client)
          .setMetricsRepository(metricsRepository)
          .setStatsPrefix(statsPrefix)
          .setSpecificValueClass(specificValueClass)
          .setDeserializationExecutor(deserializationExecutor)
          .setMetadataRefreshExecutor(metadataRefreshExecutor)
          .setClientRoutingStrategyType(clientRoutingStrategyType)
          .setDualReadEnabled(dualReadEnabled)
          .setGenericThinClient(genericThinClient)
          .setSpecificThinClient(specificThinClient)
          .setDaVinciClientForMetaStore(daVinciClientForMetaStore)
          .setThinClientForMetaStore(thinClientForMetaStore)
          .setIsMetadataConnWarmupEnabled(isMetadataConnWarmupEnabled)
          .setMetadataRefreshIntervalInSeconds(metadataRefreshIntervalInSeconds)
          .setMetadataConnWarmupTimeoutInSeconds(metadataConnWarmupTimeoutInSeconds)
          .setLongTailRetryEnabledForSingleGet(longTailRetryEnabledForSingleGet)
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(longTailRetryThresholdForSingleGetInMicroSeconds)
          .setLongTailRetryEnabledForBatchGet(longTailRetryEnabledForBatchGet)
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(longTailRetryThresholdForBatchGetInMicroSeconds)
          .setLongTailRetryEnabledForCompute(longTailRetryEnabledForCompute)
          .setVsonStore(isVsonStore)
          .setStoreMetadataFetchMode(storeMetadataFetchMode)
          .setD2Client(d2Client)
          .setClusterDiscoveryD2Service(clusterDiscoveryD2Service)
          .setUseGrpc(useGrpc)
          .setGrpcClientConfig(grpcClientConfig)
          .setProjectionFieldValidationEnabled(projectionFieldValidation)
          .setLongTailRetryBudgetEnforcementWindowInMs(longTailRetryBudgetEnforcementWindowInMs)
          .setHARClusters(harClusters)
          .setInstanceHealthMonitor(instanceHealthMonitor)
          .setRetryBudgetEnabled(retryBudgetEnabled)
          .setRetryBudgetPercentage(retryBudgetPercentage)
          .setEnableLeastLoadedRoutingStrategyForHelixGroupRouting(enableLeastLoadedRoutingStrategyForHelixGroupRouting)
          .setStoreLoadControllerEnabled(storeLoadControllerEnabled)
          .setStoreLoadControllerWindowSizeInSec(storeLoadControllerWindowSizeInSec)
          .setStoreLoadControllerRejectionRatioUpdateIntervalInSec(storeLoadControllerRejectionRatioUpdateIntervalInSec)
          .setStoreLoadControllerMaxRejectionRatio(storeLoadControllerMaxRejectionRatio)
          .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(
              longTailRangeBasedRetryThresholdForBatchGetInMilliSeconds)
          .setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(
              longTailRangeBasedRetryThresholdForComputeInMilliSeconds)
          .setStoreLoadControllerAcceptMultiplier(storeLoadControllerAcceptMultiplier)
          .setKeySerializerFactory(keySerializerFactory)
          .setValueDeserializerFactory(valueDeserializerFactory);
    }

    public ClientConfig<K, V, T> build() {
      return new ClientConfig<>(this);
    }
  }
}

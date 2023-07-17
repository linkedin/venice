package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.deserialization.BatchDeserializer;
import com.linkedin.venice.client.store.deserialization.BatchDeserializerType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClientConfig<T extends SpecificRecord> {
  private static final Logger LOGGER = LogManager.getLogger(ClientConfig.class);
  private static final String HTTPS = "https";

  public static final int DEFAULT_ZK_TIMEOUT_MS = 5000;
  public static final String DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME = "venice-discovery";
  public static final String DEFAULT_D2_ZK_BASE_PATH = "/d2";
  public static final Duration DEFAULT_SCHEMA_REFRESH_PERIOD = Duration.ofMillis(0);

  // Basic settings
  private String storeName;
  private String veniceURL;
  private String statsPrefix;
  private Class<T> specificValueClass = null;
  private boolean isVsonClient = false;

  // D2 specific settings
  private boolean isD2Routing = false;
  private String d2ServiceName = DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
  private String d2BasePath = DEFAULT_D2_ZK_BASE_PATH;
  private int d2ZkTimeout = DEFAULT_ZK_TIMEOUT_MS;
  private D2Client d2Client = null;

  // Performance-related settings
  private MetricsRepository metricsRepository = null;
  private Executor deserializationExecutor = null;
  private BatchDeserializerType batchDeserializerType = BatchDeserializerType.BLOCKING;
  private boolean useFastAvro = true;
  private boolean retryOnRouterError = false;
  private boolean retryOnAllErrors = false;
  private int retryCount = 1;
  private long retryBackOffInMs = 0;
  private boolean useBlackHoleDeserializer = false;
  private boolean forceClusterDiscoveryAtStartTime = false;
  private boolean projectionFieldValidation = true;
  private boolean remoteComputationOnly = false;
  private Duration schemaRefreshPeriod = DEFAULT_SCHEMA_REFRESH_PERIOD;
  private Optional<Predicate<Schema>> preferredSchemaFilter = Optional.empty();

  // Security settings
  private boolean isHttps = false;
  private SSLFactory sslFactory = null;

  // HttpTransport settings
  private int maxConnectionsPerRoute; // only for HTTP1

  private int maxConnectionsTotal; // only for HTTP1

  private boolean httpClient5Http2Enabled;

  // Test settings
  private Time time = new SystemTime();

  public static ClientConfig defaultGenericClientConfig(String storeName) {
    return new ClientConfig(storeName);
  }

  @Deprecated
  public static ClientConfig defaultVsonGenericClientConfig(String storeName) {
    return new ClientConfig(storeName).setVsonClient(true);
  }

  public static <V extends SpecificRecord> ClientConfig<V> defaultSpecificClientConfig(
      String storeName,
      Class<V> specificValueClass) {
    return new ClientConfig<V>(storeName).setSpecificValueClass(specificValueClass);
  }

  public static <V extends SpecificRecord> ClientConfig<V> cloneConfig(ClientConfig<V> config) {
    ClientConfig<V> newConfig = new ClientConfig<V>()

        // Basic settings
        .setStoreName(config.getStoreName())
        .setVeniceURL(config.getVeniceURL())
        .setSpecificValueClass(config.getSpecificValueClass())
        .setVsonClient(config.isVsonClient())

        // D2 specific settings
        .setD2ServiceName(config.getD2ServiceName())
        .setD2BasePath(config.getD2BasePath())
        .setD2ZkTimeout(config.getD2ZkTimeout())
        .setD2Client(config.getD2Client())
        .setD2Routing(config.isD2Routing()) // This should be the last of the D2 configs since it is an inferred config
                                            // and we want the cloned config to match the source config

        // Performance-related settings
        .setMetricsRepository(config.getMetricsRepository())
        .setDeserializationExecutor(config.getDeserializationExecutor())
        .setUseFastAvro(config.isUseFastAvro())
        .setRetryOnRouterError(config.isRetryOnRouterErrorEnabled())
        .setRetryOnAllErrors(config.isRetryOnAllErrorsEnabled())
        .setRetryCount(config.getRetryCount())
        .setRetryBackOffInMs(config.getRetryBackOffInMs())
        .setUseBlackHoleDeserializer(config.isUseBlackHoleDeserializer())
        // Security settings
        .setHttps(config.isHttps())
        .setSslFactory(config.getSslFactory())
        .setForceClusterDiscoveryAtStartTime(config.isForceClusterDiscoveryAtStartTime())
        .setProjectionFieldValidationEnabled(config.isProjectionFieldValidationEnabled())
        .setPreferredSchemaFilter(config.getPreferredSchemaFilter().orElse(null))
        .setSchemaRefreshPeriod(config.getSchemaRefreshPeriod())

        // HttpTransport settings
        .setMaxConnectionsPerRoute(config.getMaxConnectionsPerRoute())
        .setMaxConnectionsTotal(config.getMaxConnectionsTotal())
        .setHttpClient5Http2Enabled(config.isHttpClient5Http2Enabled())

        // Test settings
        .setTime(config.getTime());

    return newConfig;
  }

  public ClientConfig() {
  }

  public ClientConfig(String storeName) {
    this.storeName = storeName;
  }

  public String getStoreName() {
    return storeName;
  }

  public ClientConfig<T> setStoreName(String storeName) {
    this.storeName = storeName;
    return this;
  }

  public boolean isForceClusterDiscoveryAtStartTime() {
    return forceClusterDiscoveryAtStartTime;
  }

  public ClientConfig<T> setForceClusterDiscoveryAtStartTime(boolean forceClusterDiscoveryAtStartTime) {
    this.forceClusterDiscoveryAtStartTime = forceClusterDiscoveryAtStartTime;
    return this;
  }

  public String getVeniceURL() {
    return veniceURL;
  }

  /**
   * @param veniceURL If using D2, this should be D2 ZK address.
   *                     Otherwise, it should be router address.
   */
  public ClientConfig<T> setVeniceURL(String veniceURL) {
    if (veniceURL != null && veniceURL.startsWith(HTTPS)) {
      setHttps(true);
    } else {
      setHttps(false);
    }

    this.veniceURL = veniceURL;
    return this;
  }

  public String getStatsPrefix() {
    return statsPrefix;
  }

  public ClientConfig<T> setStatsPrefix(String statsPrefix) {
    this.statsPrefix = statsPrefix;
    return this;
  }

  public Class<T> getSpecificValueClass() {
    return specificValueClass;
  }

  public ClientConfig<T> setSpecificValueClass(Class<T> specificValueClass) {
    this.specificValueClass = specificValueClass;
    return this;
  }

  public boolean isSpecificClient() {
    return specificValueClass != null;
  }

  public boolean isD2Routing() {
    return isD2Routing;
  }

  // This is identified automatically when a D2 service name is passed in
  private ClientConfig<T> setD2Routing(boolean isD2Routing) {
    this.isD2Routing = isD2Routing;
    return this;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public ClientConfig<T> setD2ServiceName(String d2ServiceName) {
    if (d2ServiceName != null) {
      setD2Routing(true);
      this.d2ServiceName = d2ServiceName;
    } else {
      setD2Routing(false);
    }

    return this;
  }

  public String getD2BasePath() {
    return d2BasePath;
  }

  public ClientConfig<T> setD2BasePath(String d2BasePath) {
    this.d2BasePath = d2BasePath;
    return this;
  }

  public int getD2ZkTimeout() {
    return d2ZkTimeout;
  }

  public ClientConfig<T> setD2ZkTimeout(int d2ZkTimeout) {
    this.d2ZkTimeout = d2ZkTimeout;
    return this;
  }

  public D2Client getD2Client() {
    return d2Client;
  }

  public ClientConfig<T> setD2Client(D2Client d2Client) {
    this.d2Client = d2Client;
    return this;
  }

  public boolean isHttps() {
    return isHttps;
  }

  // this is identified automatically when a URL is passed in
  private ClientConfig<T> setHttps(boolean isHttps) {
    this.isHttps = isHttps;
    return this;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  public ClientConfig<T> setSslFactory(SSLFactory sslEngineComponentFactory) {
    this.sslFactory = sslEngineComponentFactory;
    return this;
  }

  public int getMaxConnectionsPerRoute() {
    return maxConnectionsPerRoute;
  }

  public ClientConfig<T> setMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
    this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    return this;
  }

  public int getMaxConnectionsTotal() {
    return maxConnectionsTotal;
  }

  public ClientConfig<T> setMaxConnectionsTotal(int maxConnectionsTotal) {
    this.maxConnectionsTotal = maxConnectionsTotal;
    return this;
  }

  public boolean isHttpClient5Http2Enabled() {
    return httpClient5Http2Enabled;
  }

  public ClientConfig<T> setHttpClient5Http2Enabled(boolean httpClient5Http2Enabled) {
    this.httpClient5Http2Enabled = httpClient5Http2Enabled;
    return this;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public ClientConfig<T> setMetricsRepository(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;
    return this;
  }

  public Executor getDeserializationExecutor() {
    return deserializationExecutor;
  }

  /**
   * Provide an arbitrary executor to execute client requests in,
   * rather than letting the client use its own internally-generated executor.
   * If null, or unset, the client will use {@link java.util.concurrent.Executors#newFixedThreadPool(int)}
   * with a thread limit equal to half the CPU cores.
   */
  public ClientConfig<T> setDeserializationExecutor(Executor deserializationExecutor) {
    this.deserializationExecutor = deserializationExecutor;
    return this;
  }

  @Deprecated
  public boolean isVsonClient() {
    return isVsonClient;
  }

  @Deprecated
  public ClientConfig<T> setVsonClient(boolean isVonClient) {
    this.isVsonClient = isVonClient;
    return this;
  }

  public BatchDeserializer getBatchGetDeserializer(Executor executor) {
    return batchDeserializerType.get(executor, this);
  }

  public ClientConfig<T> setBatchDeserializerType(BatchDeserializerType batchDeserializerType) {
    if (batchDeserializerType.equals(BatchDeserializerType.ONE_FUTURE_PER_RECORD)
        || batchDeserializerType.equals(BatchDeserializerType.ALWAYS_ON_MULTI_THREADED_PIPELINE)) {
      LOGGER.info(
          "The {} BatchDeserializerType is deprecated. Will instead use: {}",
          batchDeserializerType,
          BatchDeserializerType.BLOCKING);
    }
    this.batchDeserializerType = batchDeserializerType;
    return this;
  }

  @Deprecated
  public ClientConfig<T> setMultiGetEnvelopeIterableImpl(
      AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    LOGGER.warn("multiGetEnvelopeIterableImpl is deprecated and will be ignored.");
    return this;
  }

  @Deprecated
  public ClientConfig<T> setOnDemandDeserializerNumberOfRecordsPerThread(
      int onDemandDeserializerNumberOfRecordsPerThread) {
    LOGGER.warn("onDemandDeserializerNumberOfRecordsPerThread is deprecated and will be ignored.");
    return this;
  }

  @Deprecated
  public ClientConfig<T> setAlwaysOnDeserializerNumberOfThreads(int alwaysOnDeserializerNumberOfThreads) {
    LOGGER.warn("alwaysOnDeserializerNumberOfThreads is deprecated and will be ignored.");
    return this;
  }

  @Deprecated
  public ClientConfig<T> setAlwaysOnDeserializerQueueCapacity(int alwaysOnDeserializerQueueCapacity) {
    LOGGER.warn("alwaysOnDeserializerQueueCapacity is deprecated and will be ignored.");
    return this;
  }

  public boolean isUseFastAvro() {
    return useFastAvro;
  }

  public ClientConfig<T> setUseFastAvro(boolean useFastAvro) {
    this.useFastAvro = useFastAvro;
    return this;
  }

  public ClientConfig<T> setRetryOnRouterError(boolean value) {
    this.retryOnRouterError = value;
    return this;
  }

  public boolean isRetryOnRouterErrorEnabled() {
    return retryOnRouterError;
  }

  public ClientConfig<T> setRetryOnAllErrors(boolean value) {
    this.retryOnAllErrors = value;
    return this;
  }

  public boolean isRetryOnAllErrorsEnabled() {
    return retryOnAllErrors;
  }

  public ClientConfig<T> setRetryCount(int retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public ClientConfig<T> setRetryBackOffInMs(long retryBackOffInMs) {
    this.retryBackOffInMs = retryBackOffInMs;
    return this;
  }

  public long getRetryBackOffInMs() {
    return retryBackOffInMs;
  }

  public boolean isUseBlackHoleDeserializer() {
    return useBlackHoleDeserializer;
  }

  public ClientConfig<T> setUseBlackHoleDeserializer(boolean useBlackHoleDeserializer) {
    this.useBlackHoleDeserializer = useBlackHoleDeserializer;
    return this;
  }

  public boolean isProjectionFieldValidationEnabled() {
    return projectionFieldValidation;
  }

  public ClientConfig<T> setProjectionFieldValidationEnabled(boolean projectionFieldValidation) {
    this.projectionFieldValidation = projectionFieldValidation;
    return this;
  }

  public boolean isRemoteComputationOnly() {
    return remoteComputationOnly;
  }

  public ClientConfig<T> setRemoteComputationOnly(boolean remoteComputationOnly) {
    this.remoteComputationOnly = remoteComputationOnly;
    return this;
  }

  public Optional<Predicate<Schema>> getPreferredSchemaFilter() {
    return preferredSchemaFilter;
  }

  public ClientConfig<T> setPreferredSchemaFilter(Predicate<Schema> preferredSchemaFilter) {
    this.preferredSchemaFilter = Optional.ofNullable(preferredSchemaFilter);
    return this;
  }

  public Duration getSchemaRefreshPeriod() {
    return schemaRefreshPeriod;
  }

  public ClientConfig<T> setSchemaRefreshPeriod(Duration schemaRefreshPeriod) {
    this.schemaRefreshPeriod = schemaRefreshPeriod;
    return this;
  }

  public Time getTime() {
    return time;
  }

  public ClientConfig<T> setTime(Time time) {
    this.time = time;
    return this;
  }

  @Override
  public int hashCode() {
    int result = getStoreName().hashCode();
    result = 31 * result + (isVsonClient() ? 1 : 0);
    result = 31 * result + (getSpecificValueClass() != null ? getSpecificValueClass().hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClientConfig anotherClientConfig = (ClientConfig) o;

    if (!this.getStoreName().equals(anotherClientConfig.getStoreName())) {
      return false;
    }

    if (this.isVsonClient() != anotherClientConfig.isVsonClient()) {
      return false;
    }

    if (this.isD2Routing() != anotherClientConfig.isD2Routing()) {
      return false;
    }

    return this.getSpecificValueClass() != null
        ? this.getSpecificValueClass().equals(anotherClientConfig.getSpecificValueClass())
        : anotherClientConfig.getSpecificValueClass() == null;
  }
}

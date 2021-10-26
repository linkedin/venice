package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.client.store.deserialization.BatchDeserializer;
import com.linkedin.venice.client.store.deserialization.BatchDeserializerType;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.concurrent.Executor;
import org.apache.avro.specific.SpecificRecord;


public class ClientConfig<T extends SpecificRecord> {
  private static final String HTTPS = "https";
  public static final int DEFAULT_ZK_TIMEOUT_MS = 5000;
  public static final String DEFAULT_D2_SERVICE_NAME = "venice-discovery";
  public static final String DEFAULT_D2_ZK_BASE_PATH = "/d2";


  // Basic settings
  private String storeName;
  private String veniceURL;
  private String statsPrefix;
  private Class<T> specificValueClass = null;
  private boolean isVsonClient = false;

  // D2 specific settings
  private boolean isD2Routing = false;
  private String d2ServiceName = DEFAULT_D2_SERVICE_NAME;
  private String d2BasePath = DEFAULT_D2_ZK_BASE_PATH;
  private int d2ZkTimeout = DEFAULT_ZK_TIMEOUT_MS;
  private D2Client d2Client = null;

  // Performance-related settings
  private MetricsRepository metricsRepository = null;
  private Executor deserializationExecutor = null;
  private BatchDeserializerType batchDeserializerType = BatchDeserializerType.BLOCKING;
  private AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl = AvroGenericDeserializer.IterableImpl.BLOCKING;
  private int onDemandDeserializerNumberOfRecordsPerThread = 250;
  private int alwaysOnDeserializerNumberOfThreads = Math.max(Runtime.getRuntime().availableProcessors() / 4, 1);
  private int alwaysOnDeserializerQueueCapacity = 10000;
  private boolean useFastAvro = true;
  private boolean retryOnRouterError = false;
  private boolean retryOnAllErrors = false;
  private int retryCount = 1;
  private long retryBackOffInMs = 0;
  private boolean useBlackHoleDeserializer = false;
  private boolean reuseObjectsForSerialization = false;

  // Security settings
  private boolean isHttps = false;
  private SSLEngineComponentFactory sslEngineComponentFactory = null;

  // Test settings
  private Time time = new SystemTime();

  public static ClientConfig defaultGenericClientConfig(String storeName) {
    return new ClientConfig(storeName);
  }

  public static ClientConfig defaultVsonGenericClientConfig(String storeName) {
    return new ClientConfig(storeName).setVsonClient(true);
  }

  public static <V extends SpecificRecord> ClientConfig<V> defaultSpecificClientConfig(String storeName,
      Class<V> specificValueClass) {
    return new ClientConfig<V>(storeName)
        .setSpecificValueClass(specificValueClass);
  }

  public static <V extends SpecificRecord> ClientConfig<V> cloneConfig(ClientConfig<V> config) {
    ClientConfig<V> newConfig = new ClientConfig<V>()

        // Basic settings
        .setStoreName(config.getStoreName())
        .setVeniceURL(config.getVeniceURL())
        .setSpecificValueClass(config.getSpecificValueClass())
        .setVsonClient(config.isVsonClient())

        // D2 specific settings
        .setD2Routing(config.isD2Routing())
        .setD2ServiceName(config.getD2ServiceName())
        .setD2BasePath(config.getD2BasePath())
        .setD2ZkTimeout(config.getD2ZkTimeout())
        .setD2Client(config.getD2Client())

        // Performance-related settings
        .setMetricsRepository(config.getMetricsRepository())
        .setDeserializationExecutor(config.getDeserializationExecutor())
        .setBatchDeserializerType(config.getBatchDeserializerType())
        .setMultiGetEnvelopeIterableImpl(config.getMultiGetEnvelopeIterableImpl())
        .setOnDemandDeserializerNumberOfRecordsPerThread(config.getOnDemandDeserializerNumberOfRecordsPerThread())
        .setAlwaysOnDeserializerNumberOfThreads(config.getAlwaysOnDeserializerNumberOfThreads())
        .setAlwaysOnDeserializerQueueCapacity(config.getAlwaysOnDeserializerQueueCapacity())
        .setUseFastAvro(config.isUseFastAvro())
        .setRetryOnRouterError(config.isRetryOnRouterErrorEnabled())
        .setRetryOnAllErrors(config.isRetryOnAllErrorsEnabled())
        .setRetryCount(config.getRetryCount())
        .setRetryBackOffInMs(config.getRetryBackOffInMs())
        .setUseBlackHoleDeserializer(config.isUseBlackHoleDeserializer())
        .setReuseObjectsForSerialization(config.isReuseObjectsForSerialization())
        // Security settings
        .setHttps(config.isHttps())
        .setSslEngineComponentFactory(config.getSslEngineComponentFactory())

        // Test settings
        .setTime(config.getTime());

    return newConfig;
  }

  public ClientConfig() {}

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

  //This is identified automatically when a D2 service name is passed in
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

  //this is identified automatically when a URL is passed in
  private ClientConfig<T> setHttps(boolean isHttps) {
    this.isHttps = isHttps;
    return this;
  }

  public SSLEngineComponentFactory getSslEngineComponentFactory() {
    return sslEngineComponentFactory;
  }

  public ClientConfig<T> setSslEngineComponentFactory(SSLEngineComponentFactory sslEngineComponentFactory) {
    this.sslEngineComponentFactory = sslEngineComponentFactory;
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

  public boolean isVsonClient() {
    return isVsonClient;
  }

  public ClientConfig<T> setVsonClient(boolean isVonClient) {
    this.isVsonClient = isVonClient;
    return this;
  }

  public BatchDeserializerType getBatchDeserializerType() {
    return batchDeserializerType;
  }

  public BatchDeserializer getBatchGetDeserializer(Executor executor) {
    return batchDeserializerType.get(executor, this);
  }

  public ClientConfig<T> setBatchDeserializerType(BatchDeserializerType batchDeserializerType) {
    this.batchDeserializerType = batchDeserializerType;
    return this;
  }

  public AvroGenericDeserializer.IterableImpl getMultiGetEnvelopeIterableImpl() {
    return multiGetEnvelopeIterableImpl;
  }

  public ClientConfig<T> setMultiGetEnvelopeIterableImpl(AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    this.multiGetEnvelopeIterableImpl = multiGetEnvelopeIterableImpl;
    return this;
  }

  public int getOnDemandDeserializerNumberOfRecordsPerThread() {
    return onDemandDeserializerNumberOfRecordsPerThread;
  }

  public ClientConfig<T> setOnDemandDeserializerNumberOfRecordsPerThread(int onDemandDeserializerNumberOfRecordsPerThread) {
    this.onDemandDeserializerNumberOfRecordsPerThread = onDemandDeserializerNumberOfRecordsPerThread;
    return this;
  }

  public int getAlwaysOnDeserializerNumberOfThreads() {
    return alwaysOnDeserializerNumberOfThreads;
  }

  public ClientConfig<T> setAlwaysOnDeserializerNumberOfThreads(int alwaysOnDeserializerNumberOfThreads) {
    this.alwaysOnDeserializerNumberOfThreads = alwaysOnDeserializerNumberOfThreads;
    return this;
  }

  public int getAlwaysOnDeserializerQueueCapacity() {
    return alwaysOnDeserializerQueueCapacity;
  }

  public ClientConfig<T> setAlwaysOnDeserializerQueueCapacity(int alwaysOnDeserializerQueueCapacity) {
    this.alwaysOnDeserializerQueueCapacity = alwaysOnDeserializerQueueCapacity;
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

  public boolean isReuseObjectsForSerialization() {
    return reuseObjectsForSerialization;
  }

  public ClientConfig<T> setReuseObjectsForSerialization(boolean reuseObjectsForSerialization) {
    this.reuseObjectsForSerialization = reuseObjectsForSerialization;
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

    return this.getSpecificValueClass() != null
        ? this.getSpecificValueClass().equals(anotherClientConfig.getSpecificValueClass())
        : anotherClientConfig.getSpecificValueClass() == null;
  }
}

package com.linkedin.davinci.consumer;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class ChangelogClientConfig<T extends SpecificRecord> {
  private @Nonnull Properties consumerProperties = new Properties();

  private SchemaReader schemaReader;
  private String viewName;
  private Boolean isBeforeImageView = false;

  private String consumerName = "";

  private boolean compactMessages = false;
  private ClientConfig<T> innerClientConfig;
  private D2ControllerClient d2ControllerClient;

  private String controllerD2ServiceName;
  private int controllerRequestRetryCount;

  private String bootstrapFileSystemPath;
  private long versionSwapDetectionIntervalTimeInSeconds = 60L;
  private int seekThreadPoolSize = 10;

  /**
   * This will be used in BootstrappingVeniceChangelogConsumer to determine when to sync updates with the underlying
   * storage engine, e.g. flushes entity and offset data to disk. Default is 32 MB.
   */
  private long databaseSyncBytesInterval = 32 * 1024 * 1024L;

  /**
   * RocksDB block cache size per BootstrappingVeniceChangelogConsumer. Default is 64 MB. This config is used for both
   * the internal bootstrapping change log consumer and chunk assembler's RocksDB usage.
   */
  private long rocksDBBlockCacheSizeInBytes = 64 * 1024 * 1024L;

  /**
   * Whether to skip failed to assemble records or fail the consumption by throwing errors. Default is set to true.
   * This is acceptable for now because we allow user to provide a {@link VeniceChangeCoordinate} to seek to. The seek
   * could land in-between chunks. Partially consumed chunked records cannot be assembled and will be skipped.
   */
  private boolean skipFailedToAssembleRecords = true;

  private Boolean isExperimentalClientEnabled = false;
  private int maxBufferSize = 1000;

  /**
   * Internal fields derived from the consumer properties.
   * These are refreshed each time a new set of consumer properties is applied.
   */
  private PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
  private PubSubPositionDeserializer pubSubPositionDeserializer;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  public ChangelogClientConfig(String storeName) {
    this.innerClientConfig = new ClientConfig<>(storeName);
  }

  public ChangelogClientConfig() {
    this.innerClientConfig = new ClientConfig<>();
  }

  public ChangelogClientConfig<T> setStoreName(String storeName) {
    this.innerClientConfig.setStoreName(storeName);
    return this;
  }

  public String getStoreName() {
    return innerClientConfig.getStoreName();
  }

  public ChangelogClientConfig<T> setConsumerProperties(@Nonnull Properties consumerProperties) {
    this.consumerProperties = Objects.requireNonNull(consumerProperties);
    // Initialize all internal PubSub-related components using the current consumer properties.
    initializePubSubInternals();
    return this;
  }

  @Nonnull
  public Properties getConsumerProperties() {
    return consumerProperties;
  }

  public ChangelogClientConfig<T> setSchemaReader(SchemaReader schemaReader) {
    this.schemaReader = schemaReader;
    return this;
  }

  public SchemaReader getSchemaReader() {
    return schemaReader;
  }

  public ChangelogClientConfig<T> setViewName(String viewName) {
    this.viewName = viewName;
    return this;
  }

  public ChangelogClientConfig<T> setConsumerName(String consumerName) {
    this.consumerName = consumerName;
    return this;
  }

  public ChangelogClientConfig<T> setShouldCompactMessages(boolean compactMessages) {
    this.compactMessages = compactMessages;
    return this;
  }

  public String getViewName() {
    return viewName;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public boolean shouldCompactMessages() {
    return compactMessages;
  }

  public ChangelogClientConfig<T> setControllerD2ServiceName(String controllerD2ServiceName) {
    this.controllerD2ServiceName = controllerD2ServiceName;
    return this;
  }

  public String getControllerD2ServiceName() {
    return this.controllerD2ServiceName;
  }

  public ChangelogClientConfig<T> setD2ServiceName(String d2ServiceName) {
    this.innerClientConfig.setD2ServiceName(d2ServiceName);
    return this;
  }

  public String getD2ServiceName() {
    return this.innerClientConfig.getD2ServiceName();
  }

  public ChangelogClientConfig<T> setD2ControllerClient(D2ControllerClient d2ControllerClient) {
    this.d2ControllerClient = d2ControllerClient;
    return this;
  }

  public D2ControllerClient getD2ControllerClient() {
    return this.d2ControllerClient;
  }

  public ChangelogClientConfig<T> setD2Client(D2Client d2Client) {
    this.innerClientConfig.setD2Client(d2Client);
    return this;
  }

  public D2Client getD2Client() {
    return this.innerClientConfig.getD2Client();
  }

  public ChangelogClientConfig<T> setLocalD2ZkHosts(String localD2ZkHosts) {
    this.innerClientConfig.setVeniceURL(localD2ZkHosts);
    return this;
  }

  public String getLocalD2ZkHosts() {
    return this.innerClientConfig.getVeniceURL();
  }

  public ChangelogClientConfig<T> setControllerRequestRetryCount(int controllerRequestRetryCount) {
    this.controllerRequestRetryCount = controllerRequestRetryCount;
    return this;
  }

  public int getControllerRequestRetryCount() {
    return this.controllerRequestRetryCount;
  }

  public ClientConfig<T> getInnerClientConfig() {
    return this.innerClientConfig;
  }

  public ChangelogClientConfig<T> setBootstrapFileSystemPath(String bootstrapFileSystemPath) {
    this.bootstrapFileSystemPath = bootstrapFileSystemPath;
    return this;
  }

  public String getBootstrapFileSystemPath() {
    return this.bootstrapFileSystemPath;
  }

  public long getVersionSwapDetectionIntervalTimeInSeconds() {
    return versionSwapDetectionIntervalTimeInSeconds;
  }

  public ChangelogClientConfig setVersionSwapDetectionIntervalTimeInSeconds(long intervalTimeInSeconds) {
    this.versionSwapDetectionIntervalTimeInSeconds = intervalTimeInSeconds;
    return this;
  }

  public int getSeekThreadPoolSize() {
    return seekThreadPoolSize;
  }

  public ChangelogClientConfig setSeekThreadPoolSize(int seekThreadPoolSize) {
    this.seekThreadPoolSize = seekThreadPoolSize;
    return this;
  }

  /**
   * Gets the databaseSyncBytesInterval.
   */
  public long getDatabaseSyncBytesInterval() {
    return databaseSyncBytesInterval;
  }

  /**
   * Sets the value for databaseSyncBytesInterval.
   */
  public ChangelogClientConfig setDatabaseSyncBytesInterval(long databaseSyncBytesInterval) {
    this.databaseSyncBytesInterval = databaseSyncBytesInterval;
    return this;
  }

  public long getRocksDBBlockCacheSizeInBytes() {
    return rocksDBBlockCacheSizeInBytes;
  }

  public ChangelogClientConfig setRocksDBBlockCacheSizeInBytes(long rocksDBBlockCacheSizeInBytes) {
    this.rocksDBBlockCacheSizeInBytes = rocksDBBlockCacheSizeInBytes;
    return this;
  }

  /**
   * If you're using the experimental client, and you want to deserialize your keys into
   * {@link org.apache.avro.specific.SpecificRecord} thenr set this configuration.
   */
  public ChangelogClientConfig setSpecificKey(Class specificKey) {
    this.innerClientConfig.setSpecificKeyClass(specificKey);
    return this;
  }

  public ChangelogClientConfig setSpecificValue(Class<T> specificValue) {
    this.innerClientConfig.setSpecificValueClass(specificValue);
    return this;
  }

  /**
   * If you're using the experimental client, and you want to deserialize your values into
   * {@link org.apache.avro.specific.SpecificRecord} then set this configuration.
   */
  public ChangelogClientConfig setSpecificValueSchema(Schema specificValueSchema) {
    this.innerClientConfig.setSpecificValueSchema(specificValueSchema);
    return this;
  }

  public ChangelogClientConfig setShouldSkipFailedToAssembleRecords(boolean skipFailedToAssembleRecords) {
    this.skipFailedToAssembleRecords = skipFailedToAssembleRecords;
    return this;
  }

  public boolean shouldSkipFailedToAssembleRecords() {
    return skipFailedToAssembleRecords;
  }

  public static <V extends SpecificRecord> ChangelogClientConfig<V> cloneConfig(ChangelogClientConfig<V> config) {
    ChangelogClientConfig<V> newConfig = new ChangelogClientConfig<V>().setStoreName(config.getStoreName())
        .setLocalD2ZkHosts(config.getLocalD2ZkHosts())
        .setD2ServiceName(config.getD2ServiceName())
        .setConsumerProperties(config.getConsumerProperties())
        .setSchemaReader(config.getSchemaReader())
        .setViewName(config.getViewName())
        .setD2ControllerClient(config.getD2ControllerClient())
        .setControllerD2ServiceName(config.controllerD2ServiceName)
        .setD2Client(config.getD2Client())
        .setControllerRequestRetryCount(config.getControllerRequestRetryCount())
        .setBootstrapFileSystemPath(config.getBootstrapFileSystemPath())
        .setVersionSwapDetectionIntervalTimeInSeconds(config.getVersionSwapDetectionIntervalTimeInSeconds())
        .setRocksDBBlockCacheSizeInBytes(config.getRocksDBBlockCacheSizeInBytes())
        .setConsumerName(config.consumerName)
        .setDatabaseSyncBytesInterval(config.getDatabaseSyncBytesInterval())
        .setShouldCompactMessages(config.shouldCompactMessages())
        .setIsBeforeImageView(config.isBeforeImageView())
        .setIsExperimentalClientEnabled(config.isExperimentalClientEnabled())
        .setMaxBufferSize(config.getMaxBufferSize())
        .setSeekThreadPoolSize(config.getSeekThreadPoolSize())
        .setShouldSkipFailedToAssembleRecords(config.shouldSkipFailedToAssembleRecords())
        .setInnerClientConfig(config.getInnerClientConfig());
    return newConfig;
  }

  protected Boolean isBeforeImageView() {
    return isBeforeImageView;
  }

  public ChangelogClientConfig setIsBeforeImageView(Boolean beforeImageView) {
    isBeforeImageView = beforeImageView;
    return this;
  }

  protected Boolean isExperimentalClientEnabled() {
    return isExperimentalClientEnabled;
  }

  /**
   * This uses a highly experimental client.
   * It is currently only supported for {@link BootstrappingVeniceChangelogConsumer}.
   */
  public ChangelogClientConfig setIsExperimentalClientEnabled(Boolean experimentalClientEnabled) {
    isExperimentalClientEnabled = experimentalClientEnabled;
    return this;
  }

  protected int getMaxBufferSize() {
    return maxBufferSize;
  }

  /**
   * Sets the maximum number of records that can be buffered and returned to the user when calling poll.
   * When the maximum number of records is reached, ingestion will be paused until the buffer is drained.
   * Please note that this is separate from {@link com.linkedin.venice.ConfigKeys#SERVER_KAFKA_MAX_POLL_RECORDS}.
   * In order for this feature to be used, {@link #setIsExperimentalClientEnabled(Boolean)} must be set to true.
   * It is currently only supported for {@link BootstrappingVeniceChangelogConsumer}.
   */
  public ChangelogClientConfig setMaxBufferSize(int maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
    return this;
  }

  /**
   * Initializes all internal PubSub-related components using the current {@link #consumerProperties}.
   *
   * <p>This method sets up:
   * <ul>
   *   <li>{@link #pubSubPositionTypeRegistry} – derived from consumer properties</li>
   *   <li>{@link #pubSubPositionDeserializer} – uses the initialized position type registry</li>
   *   <li>{@link #pubSubConsumerAdapterFactory} – created based on resolved consumer configuration</li>
   *   <li>{@link #pubSubMessageDeserializer} – stateless shared instance</li>
   * </ul>
   *
   * <p><strong>Note:</strong> These fields are derived from the {@link #consumerProperties} and should
   * not be externally modified or re-initialized independently. Always ensure
   * {@code consumerProperties} is set first before calling this method. This method
   * should only be invoked internally when the properties are updated.
   */
  private void initializePubSubInternals() {
    VeniceProperties pubSubProperties = new VeniceProperties(this.consumerProperties);
    this.pubSubPositionTypeRegistry = PubSubPositionTypeRegistry.fromPropertiesOrDefault(pubSubProperties);
    this.pubSubPositionDeserializer = new PubSubPositionDeserializer(pubSubPositionTypeRegistry);
    this.pubSubConsumerAdapterFactory = PubSubClientsFactory.createConsumerFactory(pubSubProperties);
    this.pubSubMessageDeserializer = PubSubMessageDeserializer.createOptimizedDeserializer();
  }

  protected PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> getPubSubConsumerAdapterFactory() {
    return pubSubConsumerAdapterFactory;
  }

  protected PubSubPositionDeserializer getPubSubPositionDeserializer() {
    return pubSubPositionDeserializer;
  }

  protected PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  protected PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return pubSubPositionTypeRegistry;
  }

  protected PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  private ChangelogClientConfig setInnerClientConfig(ClientConfig<T> innerClientConfig) {
    this.innerClientConfig = innerClientConfig;
    return this;
  }
}

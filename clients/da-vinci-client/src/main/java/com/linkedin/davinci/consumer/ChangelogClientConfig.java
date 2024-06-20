package com.linkedin.davinci.consumer;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;


public class ChangelogClientConfig<T extends SpecificRecord> {
  private Properties consumerProperties;
  private SchemaReader schemaReader;
  private String viewName;

  private String consumerName = "";
  private ClientConfig<T> innerClientConfig;
  private D2ControllerClient d2ControllerClient;

  private String controllerD2ServiceName;
  private int controllerRequestRetryCount;

  private String bootstrapFileSystemPath;
  private long versionSwapDetectionIntervalTimeInMs = 600000L;

  /**
   * This will be used in BootstrappingVeniceChangelogConsumer to determine when to sync updates with the underlying
   * storage engine, e.g. flushes entity and offset data to disk. Default is 32 MB.
   */
  private long databaseSyncBytesInterval = 32 * 1024 * 1024L;

  /**
   * RocksDB block cache size per BootstrappingVeniceChangelogConsumer. Default is 1 MB.
   */
  private long rocksDBBlockCacheSizeInBytes = 1024 * 1024L;

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

  public ChangelogClientConfig<T> setConsumerProperties(Properties consumerProperties) {
    this.consumerProperties = consumerProperties;
    return this;
  }

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

  public String getViewName() {
    return viewName;
  }

  public String getConsumerName() {
    return consumerName;
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

  public long getVersionSwapDetectionIntervalTimeInMs() {
    return versionSwapDetectionIntervalTimeInMs;
  }

  public ChangelogClientConfig setVersionSwapDetectionIntervalTimeInMs(long intervalTimeInMs) {
    this.versionSwapDetectionIntervalTimeInMs = intervalTimeInMs;
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

  public ChangelogClientConfig setSpecificValue(Class<T> specificValue) {
    this.innerClientConfig.setSpecificValueClass(specificValue);
    return this;
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
        .setVersionSwapDetectionIntervalTimeInMs(config.getVersionSwapDetectionIntervalTimeInMs())
        .setRocksDBBlockCacheSizeInBytes(config.getRocksDBBlockCacheSizeInBytes())
        .setConsumerName(config.consumerName)
        .setDatabaseSyncBytesInterval(config.getDatabaseSyncBytesInterval());
    return newConfig;
  }
}

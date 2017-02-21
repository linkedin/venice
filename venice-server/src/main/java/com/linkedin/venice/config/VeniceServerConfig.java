package com.linkedin.venice.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.store.bdb.BdbServerConfig;
import com.linkedin.venice.utils.VeniceProperties;

import static com.linkedin.venice.ConfigKeys.*;

/**
 * class that maintains config very specific to a Venice server
 */
public class VeniceServerConfig extends VeniceClusterConfig {

  private final int listenerPort;
  private final BdbServerConfig bdbServerConfig;
  private final boolean enableServerWhiteList;
  private final boolean autoCreateDataPath; // default true
  /**
   * Queue capacity for consumer thread allocated by every {@link com.linkedin.venice.kafka.consumer.StoreConsumptionTask}
   */
  private final int consumerRecordsQueueCapacity;

  public VeniceServerConfig(VeniceProperties serverProperties) throws ConfigurationException {
    super(serverProperties);
    listenerPort = serverProperties.getInt(LISTENER_PORT);
    dataBasePath = serverProperties.getString(DATA_BASE_PATH);
    autoCreateDataPath = Boolean.valueOf(serverProperties.getString(AUTOCREATE_DATA_PATH, "true"));
    bdbServerConfig = new BdbServerConfig(serverProperties);
    enableServerWhiteList = serverProperties.getBoolean(ENABLE_SERVER_WHITE_LIST, false);
    /**
     * Here, we choose the default queue capacity is 10 since the only worker could process the queued
     * message very fast, and even bigger queue size won't give better performance, but bigger memory footprint.
     * If the default value is not appropriate, we can adjust it by this config.
     */
    consumerRecordsQueueCapacity = serverProperties.getInt(CONSUMER_RECORDS_QUEUE_CAPACITY, 10);
  }

  public int getListenerPort() {
    return listenerPort;
  }


  /**
   * Get base path of Venice storage data.
   *
   * @return Base path of persisted Venice database files.
   */
  public String getDataBasePath() {
    return this.dataBasePath;
  }

  public boolean isAutoCreateDataPath(){
    return autoCreateDataPath;
  }

  public BdbServerConfig getBdbServerConfig() {
    return this.bdbServerConfig;
  }

  public boolean isServerWhiteLIstEnabled() {
    return enableServerWhiteList;
  }

  public int getConsumerRecordsQueueCapacity() {
    return consumerRecordsQueueCapacity;
  }
}

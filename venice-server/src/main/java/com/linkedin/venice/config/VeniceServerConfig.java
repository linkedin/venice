package com.linkedin.venice.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.store.bdb.BdbServerConfig;
import com.linkedin.venice.utils.VeniceProperties;

import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.ENABLE_SERVER_WHITE_LIST;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;


/**
 * class that maintains config very specific to a Venice server
 */
public class VeniceServerConfig extends VeniceClusterConfig {

  private final int listenerPort;
  private final  BdbServerConfig bdbServerConfig;
  private final boolean enableServerWhiteList;

  public VeniceServerConfig(VeniceProperties serverProperties) throws ConfigurationException {
    super(serverProperties);
    listenerPort = serverProperties.getInt(LISTENER_PORT);
    dataBasePath = serverProperties.getString(DATA_BASE_PATH);
    bdbServerConfig = new BdbServerConfig(serverProperties);
    enableServerWhiteList = serverProperties.getBoolean(ENABLE_SERVER_WHITE_LIST, false);
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

  public BdbServerConfig getBdbServerConfig() {
    return this.bdbServerConfig;
  }

  public boolean isServerWhiteLIstEnabled() {
    return enableServerWhiteList;
  }
}

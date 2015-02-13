package com.linkedin.venice.config;

import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.utils.Props;
import com.linkedin.venice.exceptions.UndefinedPropertyException;


/**
 * class that maintains config very specific to a Venice server
 */
public class VeniceServerConfig extends VeniceClusterConfig {

  private int nodeId;
  private static final String VENICE_NODE_ID_VAR_NAME = "VENICE_NODE_ID";

  public VeniceServerConfig(Props serverProperties)
      throws ConfigurationException {
    super(serverProperties);
    verifyProperties(serverProperties);
  }

  private void verifyProperties(Props serverProps) {
    try {
      nodeId = serverProps.getInt(VeniceConfigService.NODE_ID);
    } catch (UndefinedPropertyException e) {
      this.nodeId = getIntEnvVariable(VENICE_NODE_ID_VAR_NAME);
    }
  }

  public int getNodeId() {
    return nodeId;
  }

  /**
   * Get config from Environment
   * @param name
   * @return
   */
  private int getIntEnvVariable(String name) {
    String var = System.getenv(name);
    if (var == null) {
      throw new ConfigurationException("The environment variable " + name + " is not defined.");
    }
    try {
      return Integer.parseInt(var);
    } catch (NumberFormatException e) {
      throw new ConfigurationException("Invalid format for environment variable " + name + ", expecting an integer.",
          e);
    }
  }
}

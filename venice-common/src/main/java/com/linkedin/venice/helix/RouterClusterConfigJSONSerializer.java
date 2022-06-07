package com.linkedin.venice.helix;

import com.linkedin.venice.meta.RoutersClusterConfig;


/**
 * Serializer used to convert data between RouterClusterConfig Object and JSON string
 */
public class RouterClusterConfigJSONSerializer extends VeniceJsonSerializer<RoutersClusterConfig> {
  public RouterClusterConfigJSONSerializer() {
    super(RoutersClusterConfig.class);
  }
}

package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;


public interface ReadOnlyLiveClusterConfigRepository extends VeniceResource {
  /**
   * Get live cluster configs
   * @return
   */
  LiveClusterConfig getConfigs();
}

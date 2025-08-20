package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;


public interface ReadOnlyDarkClusterConfigRepository extends VeniceResource {
  /**
   * Get dark cluster configs
   * @return
   */
  DarkClusterConfig getConfigs();

  void refresh();

  void clear();
}

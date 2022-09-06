package com.linkedin.venice.meta;

public interface ReadWriteLiveClusterConfigRepository extends ReadOnlyLiveClusterConfigRepository {
  /**
   * Set live cluster configs
   * @param clusterConfig The updated {@link LiveClusterConfig}
   */
  void updateConfigs(LiveClusterConfig clusterConfig);

  /**
   * Delete all live cluster configs
   */
  void deleteConfigs();
}

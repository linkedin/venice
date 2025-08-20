package com.linkedin.venice.meta;

public interface ReadWriteDarkClusterConfigRepository extends ReadOnlyDarkClusterConfigRepository {
  void updateConfigs(DarkClusterConfig config);

  void deleteConfigs();
}

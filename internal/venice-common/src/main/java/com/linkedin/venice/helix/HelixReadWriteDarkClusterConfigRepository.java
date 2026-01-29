package com.linkedin.venice.helix;

import com.linkedin.venice.meta.DarkClusterConfig;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HelixReadWriteDarkClusterConfigRepository extends HelixReadOnlyDarkClusterConfigRepository {
  private static final Logger logger = LogManager.getLogger(HelixReadWriteDarkClusterConfigRepository.class);

  public HelixReadWriteDarkClusterConfigRepository(
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName) {
    super(zkClient, adapter, clusterName);
  }

  public void updateConfigs(DarkClusterConfig darkClusterConfig) {
    logger.info("Updating dark cluster configs to:\n{}.", darkClusterConfig);
    HelixUtils.update(getZkDataAccessor(), getClusterConfigZkPath(), darkClusterConfig);
  }

  public void deleteConfigs() {
    logger.info("Deleting dark cluster configs.");
    HelixUtils.remove(getZkDataAccessor(), getClusterConfigZkPath());
  }
}

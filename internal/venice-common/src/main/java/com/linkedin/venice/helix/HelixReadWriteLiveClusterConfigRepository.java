package com.linkedin.venice.helix;

import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.meta.ReadWriteLiveClusterConfigRepository;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to modify live cluster configs. The expected user is Venice Controller.
 */
public class HelixReadWriteLiveClusterConfigRepository extends HelixReadOnlyLiveClusterConfigRepository
    implements ReadWriteLiveClusterConfigRepository {
  private static final Logger logger = LogManager.getLogger(HelixReadWriteLiveClusterConfigRepository.class);

  public HelixReadWriteLiveClusterConfigRepository(
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName) {
    super(zkClient, adapter, clusterName);
  }

  @Override
  public void updateConfigs(LiveClusterConfig clusterConfig) {
    logger.info("Updating cluster configs to:\n{}.", clusterConfig);
    HelixUtils.update(zkDataAccessor, clusterConfigZkPath, clusterConfig);
  }

  @Override
  public void deleteConfigs() {
    logger.info("Deleting dynamic cluster configs.");
    HelixUtils.remove(zkDataAccessor, clusterConfigZkPath);
  }
}

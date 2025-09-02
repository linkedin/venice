package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.DARK_CLUSTER_CONFIG;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.DarkClusterConfig;
import com.linkedin.venice.meta.ReadOnlyDarkClusterConfigRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.nio.file.Paths;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a read-only implementation of {@link ReadOnlyDarkClusterConfigRepository}.
 * It listens to changes in the dark cluster configuration stored in ZooKeeper and updates its
 * in-memory representation accordingly.
 * ZK Config Path: {@see com.linkedin.venice.zk.VeniceZkPaths.DARK_CLUSTER_CONFIG}
 */
public class HelixReadOnlyDarkClusterConfigRepository implements ReadOnlyDarkClusterConfigRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyDarkClusterConfigRepository.class);

  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final DarkClusterConfig DEFAULT_DARK_CLUSTER_CONFIG = new DarkClusterConfig();

  protected ZkBaseDataAccessor<DarkClusterConfig> zkDataAccessor;
  protected final String clusterConfigZkPath;
  protected DarkClusterConfig darkClusterConfig = DEFAULT_DARK_CLUSTER_CONFIG;

  // Listener to handle modifications to cluster config
  private final IZkDataListener clusterConfigListener = new ClusterConfigZkListener();

  private static final String DARK_CLUSTER_CONFIG_PATH = "/" + DARK_CLUSTER_CONFIG;

  public HelixReadOnlyDarkClusterConfigRepository(
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName) {
    this.zkDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.clusterConfigZkPath =
        Paths.get(HelixUtils.getHelixClusterZkPath(clusterName), DARK_CLUSTER_CONFIG_PATH).toString();
    adapter.registerSerializer(clusterConfigZkPath, new VeniceJsonSerializer<>(DarkClusterConfig.class));
    zkClient.setZkSerializer(adapter);
  }

  @Override
  public DarkClusterConfig getConfigs() {
    return darkClusterConfig;
  }

  @Override
  public void refresh() {
    getZkDataAccessor().subscribeDataChanges(clusterConfigZkPath, clusterConfigListener);
    DarkClusterConfig config = getZkDataAccessor().get(clusterConfigZkPath, null, AccessOption.PERSISTENT);
    darkClusterConfig = config == null ? DEFAULT_DARK_CLUSTER_CONFIG : config;
  }

  @Override
  public void clear() {
    getZkDataAccessor().unsubscribeDataChanges(clusterConfigZkPath, clusterConfigListener);
  }

  protected class ClusterConfigZkListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) {
      if (!(data instanceof DarkClusterConfig)) {
        throw new VeniceException("Invalid config data, changed data is not:" + DarkClusterConfig.class.getName());
      }
      darkClusterConfig = (DarkClusterConfig) data;
      String logMessage = "Received updated DarkClusterConfig:\n" + darkClusterConfig;
      if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logMessage)) {
        LOGGER.info(logMessage);
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      LOGGER.info("{} ZNode deleted. Resetting configs to default.", dataPath);
      darkClusterConfig = DEFAULT_DARK_CLUSTER_CONFIG;
    }
  }

  // For test purposes
  ClusterConfigZkListener getClusterConfigZkListener() {
    return new ClusterConfigZkListener();
  }

  void setZkDataAccessor(ZkBaseDataAccessor<DarkClusterConfig> accessor) {
    this.zkDataAccessor = accessor;
  }

  String getClusterConfigZkPath() {
    return clusterConfigZkPath;
  }

  ZkBaseDataAccessor<DarkClusterConfig> getZkDataAccessor() {
    return zkDataAccessor;
  }
}

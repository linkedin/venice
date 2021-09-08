package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.log4j.Logger;


/**
 * This class is used to cache live cluster configs.
 * The expected users are all venice-backend components.
 */
public class HelixReadOnlyLiveClusterConfigRepository implements ReadOnlyLiveClusterConfigRepository {
  private final Logger logger = Logger.getLogger(HelixReadOnlyLiveClusterConfigRepository.class);
  protected final ZkBaseDataAccessor<LiveClusterConfig> zkDataAccessor;
  protected final String clusterConfigZkPath;
  protected LiveClusterConfig liveClusterConfig;

  private static final LiveClusterConfig DEFAULT_LIVE_CLUSTER_CONFIG = new LiveClusterConfig();

  // Listener to handle modifications to cluster config
  private IZkDataListener clusterConfigListener = new ClusterConfigZkListener();

  private static final String CLUSTER_CONFIG_PATH = "/ClusterConfig";

  public HelixReadOnlyLiveClusterConfigRepository(ZkClient zkClient, HelixAdapterSerializer adapter, String clusterName) {
    this.zkDataAccessor = new ZkBaseDataAccessor(zkClient);
    this.clusterConfigZkPath = Paths.get(HelixUtils.getHelixClusterZkPath(clusterName), CLUSTER_CONFIG_PATH).toString();
    adapter.registerSerializer(clusterConfigZkPath, new VeniceJsonSerializer(LiveClusterConfig.class));
  }

  @Override
  public LiveClusterConfig getConfigs() {
    return liveClusterConfig;
  }

  @Override
  public void refresh() {
    zkDataAccessor.subscribeDataChanges(clusterConfigZkPath, clusterConfigListener);
  }

  @Override
  public void clear() {
    zkDataAccessor.unsubscribeDataChanges(clusterConfigZkPath, clusterConfigListener);
  }

  /**
   * Listener that get partition status ZNode data change notification then transfer it to a Venice partition status
   * change event and broadcast this event to Venice subscriber.
   */
  private class ClusterConfigZkListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) {
      if (!(data instanceof LiveClusterConfig)) {
        throw new VeniceException("Invalid notification, changed data is not:" + LiveClusterConfig.class.getName());
      }
      liveClusterConfig = (LiveClusterConfig) data;
      logger.info("Received updated LiveClusterConfig:\n" + liveClusterConfig);
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      logger.info(dataPath + " ZNode deleted. Resetting configs to default.");
      liveClusterConfig = DEFAULT_LIVE_CLUSTER_CONFIG;
    }
  }
}

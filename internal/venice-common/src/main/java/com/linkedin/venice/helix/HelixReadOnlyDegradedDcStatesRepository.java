package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.DEGRADED_DC_STATES;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.DegradedDcStates;
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
 * Read-only ZK-backed repository for degraded DC states.
 * Caches the current state in memory and watches for ZK changes.
 */
public class HelixReadOnlyDegradedDcStatesRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyDegradedDcStatesRepository.class);

  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final DegradedDcStates DEFAULT_STATES = new DegradedDcStates();

  protected final ZkBaseDataAccessor<DegradedDcStates> zkDataAccessor;
  protected final String zkPath;
  protected volatile DegradedDcStates states = DEFAULT_STATES;

  private final IZkDataListener zkListener = new DegradedDcStatesZkListener();

  private static final String ZK_PATH_SUFFIX = "/" + DEGRADED_DC_STATES;

  public HelixReadOnlyDegradedDcStatesRepository(
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName) {
    this.zkDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.zkPath = Paths.get(HelixUtils.getHelixClusterZkPath(clusterName), ZK_PATH_SUFFIX).toString();
    adapter.registerSerializer(zkPath, new VeniceJsonSerializer<>(DegradedDcStates.class));
    zkClient.setZkSerializer(adapter);
  }

  public DegradedDcStates getStates() {
    return states;
  }

  public void refresh() {
    zkDataAccessor.subscribeDataChanges(zkPath, zkListener);
    DegradedDcStates loaded = zkDataAccessor.get(zkPath, null, AccessOption.PERSISTENT);
    states = loaded == null ? DEFAULT_STATES : loaded;
  }

  public void clear() {
    zkDataAccessor.unsubscribeDataChanges(zkPath, zkListener);
  }

  private class DegradedDcStatesZkListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) {
      if (!(data instanceof DegradedDcStates)) {
        throw new VeniceException("Invalid notification, changed data is not: " + DegradedDcStates.class.getName());
      }
      states = (DegradedDcStates) data;
      String logMessage = "Received updated DegradedDcStates: " + states;
      if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logMessage)) {
        LOGGER.info(logMessage);
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      LOGGER.info("{} ZNode deleted. Resetting degraded DC states to default.", dataPath);
      states = DEFAULT_STATES;
    }
  }
}

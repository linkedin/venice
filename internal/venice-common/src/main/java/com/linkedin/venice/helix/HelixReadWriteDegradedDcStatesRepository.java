package com.linkedin.venice.helix;

import com.linkedin.venice.meta.DegradedDcStates;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Read-write ZK-backed repository for degraded DC states.
 * Used by Venice Controller to mark/unmark datacenters as degraded.
 */
public class HelixReadWriteDegradedDcStatesRepository extends HelixReadOnlyDegradedDcStatesRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadWriteDegradedDcStatesRepository.class);

  public HelixReadWriteDegradedDcStatesRepository(
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String clusterName) {
    super(zkClient, adapter, clusterName);
  }

  public void updateStates(DegradedDcStates newStates) {
    LOGGER.info("Updating degraded DC states to: {}", newStates);
    HelixUtils.update(zkDataAccessor, zkPath, newStates);
  }

  public void deleteStates() {
    LOGGER.info("Deleting degraded DC states.");
    HelixUtils.remove(zkDataAccessor, zkPath);
  }
}

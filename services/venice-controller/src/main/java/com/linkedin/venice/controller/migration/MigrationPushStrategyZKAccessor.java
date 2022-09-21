package com.linkedin.venice.controller.migration;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.migration.MigrationPushStrategy;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MigrationPushStrategyZKAccessor {
  private static final Logger LOGGER = LogManager.getLogger(MigrationPushStrategy.class);

  public static final String MIGRATION_PUSH_STRATEGY_PATH = "/migration-push-strategy";

  private final ZkBaseDataAccessor<Map<String, String>> zkAccessor;

  public MigrationPushStrategyZKAccessor(ZkClient zkClient, HelixAdapterSerializer adapter) {
    this.zkAccessor = new ZkBaseDataAccessor<>(zkClient);

    adapter.registerSerializer(MIGRATION_PUSH_STRATEGY_PATH, new MigrationPushStrategyJSONSerializer());
    zkClient.setZkSerializer(adapter);
  }

  /**
   * The reason to return the push strategy for all the stores:
   * 1. It is necessary for admin tool to retrieve push strategy for all the stores;
   * 2. Right now, {@link com.linkedin.venice.controllerapi.ControllerClient} doesn't provide
   * a good way to recognize "not-exist" status since the backend always throws an exception;
   * 3. Combo plugin needs to know whether the push strategy exists or not to decide whether to use the
   * default push strategy;
   *
   * It is definitely possible to support 'not-exist' case: such as updating ControllerClient to be aware of '404',
   * but right now I choose to return the push strategy for all the stores, and let client decide.
   */
  public Map<String, String> getAllPushStrategies() {
    if (!zkAccessor.exists(MIGRATION_PUSH_STRATEGY_PATH, AccessOption.PERSISTENT)) {
      return Collections.emptyMap();
    }
    return zkAccessor.get(MIGRATION_PUSH_STRATEGY_PATH, null, AccessOption.PERSISTENT);
  }

  public void setPushStrategy(String voldemortStoreName, String pushStrategyStr) {
    // Check whether push strategy is valid or not.
    try {
      MigrationPushStrategy.valueOf(pushStrategyStr);
    } catch (IllegalArgumentException iae) {
      throw new VeniceException(
          "Invalid push strategy: " + pushStrategyStr + ", should be one of the following: ["
              + MigrationPushStrategy.getAllEnumString() + "]");
    }
    LOGGER.info("Setup push strategy: {} for Voldemort store: {}", pushStrategyStr, voldemortStoreName);

    HelixUtils.compareAndUpdate(zkAccessor, MIGRATION_PUSH_STRATEGY_PATH, oldData -> {
      if (oldData == null) {
        // Doesn't exist
        oldData = new HashMap<>();
      }
      oldData.put(voldemortStoreName, pushStrategyStr);

      return oldData;
    });
  }
}

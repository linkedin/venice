package com.linkedin.venice.zk;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * This class contains constants that represent Venice-managed ZooKeeper paths.
 */
public class VeniceZkPaths {
  public static final String ADMIN_TOPIC_METADATA = "adminTopicMetadata";
  // new admin topic metadata structure is incompatible with the old one, so creating a new "v2" path
  public static final String ADMIN_TOPIC_METADATA_V2 = "adminTopicMetadataV2";
  public static final String CLUSTER_CONFIG = "ClusterConfig";
  public static final String EXECUTION_IDS = "executionids";
  public static final String OFFLINE_PUSHES = "OfflinePushes";
  public static final String PARENT_OFFLINE_PUSHES = "ParentOfflinePushes";
  public static final String ROUTERS = "routers";
  public static final String STORES = "Stores";
  public static final String STORE_CONFIGS = "storeConfigs";
  public static final String STORE_GRAVEYARD = "StoreGraveyard";

  /** Set of all Venice-managed ZooKeeper cluster paths */
  private static final Set<String> CLUSTER_ZK_PATHS_MODIFIABLE = new HashSet<>(
      Arrays.asList(ADMIN_TOPIC_METADATA, EXECUTION_IDS, PARENT_OFFLINE_PUSHES, ROUTERS, STORE_GRAVEYARD, STORES));
  /** @see #CLUSTER_ZK_PATHS_MODIFIABLE */
  public static final Set<String> CLUSTER_ZK_PATHS = Collections.unmodifiableSet(CLUSTER_ZK_PATHS_MODIFIABLE);
}

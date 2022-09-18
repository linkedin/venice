package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.VeniceProperties;


public class VeniceClusterWrapperConstants {
  protected static final int DEFAULT_MAX_ATTEMPT = 10;
  protected static final int DEFAULT_REPLICATION_FACTOR = 1;
  protected static final int DEFAULT_PARTITION_SIZE_BYTES = 100;
  // By default, disable the delayed rebalance for testing.
  protected static final long DEFAULT_DELAYED_TO_REBALANCE_MS = 0;
  protected static final boolean DEFAULT_SSL_TO_STORAGE_NODES = false;
  protected static final boolean DEFAULT_SSL_TO_KAFKA = false;
  protected static final int DEFAULT_NUMBER_OF_SERVERS = 1;
  protected static final int DEFAULT_NUMBER_OF_ROUTERS = 1;
  protected static final int DEFAULT_NUMBER_OF_CONTROLLERS = 1;
  protected static final VeniceProperties EMPTY_VENICE_PROPS = new VeniceProperties();
  // Wait time to make sure all the cluster services have been started.
  // If this value is not large enough, i.e. some services have not been
  // started before clients start to interact, please increase it.
  protected static final int DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S = 90;
}

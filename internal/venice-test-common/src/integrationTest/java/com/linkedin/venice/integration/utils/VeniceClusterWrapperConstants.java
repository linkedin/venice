package com.linkedin.venice.integration.utils;

public class VeniceClusterWrapperConstants {
  public static final int DEFAULT_MAX_ATTEMPT = 10;
  public static final int DEFAULT_REPLICATION_FACTOR = 1;
  public static final int DEFAULT_PARTITION_SIZE_BYTES = 100;
  /**
   * Running with just one partition may not fully exercise the distributed nature of the system,
   * but we do want to minimize the number as each partition results in files, connections, threads, etc.
   * in the whole system. 3 seems like a reasonable tradeoff between these concerns.
   */
  public static final int DEFAULT_NUMBER_OF_PARTITIONS = 1;
  public static final int DEFAULT_MAX_NUMBER_OF_PARTITIONS = 3;
  // By default, disable the delayed rebalance for testing.
  public static final long DEFAULT_DELAYED_TO_REBALANCE_MS = 0;
  public static final boolean DEFAULT_SSL_TO_STORAGE_NODES = false;
  public static final boolean DEFAULT_SSL_TO_KAFKA = false;
  public static final int DEFAULT_NUMBER_OF_SERVERS = 1;
  public static final int DEFAULT_NUMBER_OF_ROUTERS = 1;
  public static final int DEFAULT_NUMBER_OF_CONTROLLERS = 1;
  // Wait time to make sure all the cluster services have been started.
  // If this value is not large enough, i.e. some services have not been
  // started before clients start to interact, please increase it.
  public static final int DEFAULT_WAIT_TIME_FOR_CLUSTER_START_S = 90;
}

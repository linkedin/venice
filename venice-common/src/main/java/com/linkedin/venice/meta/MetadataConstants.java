package com.linkedin.venice.meta;

/**
 * Constants of metadata.
 */
public class MetadataConstants {
    public static final int DEFAULT_REPLICATION_FACTOR = 2;

    public static final String IN_MEMORY = "IN_MEMORY";
    public static final String BDB = "BDB";
    public static final String ROCKS_DB = "ROCKS_DB";

    public static final String CONSISTENCY_HASH = "CONSISTENCY_HASH";

    /**
     * Read from any one online replication.
     */
    public static final String ANY_OF_ONLINE = "ANY_OF_ONLINE";
    /**
     * Read from two of replication and use the result returned by the faster one.
     */
    public static final String FASTER_OF_TWO_ONLINE = "FASTER_OF_TWO_ONLINE";

    /**
     * Wait all replica is ready, the version is ready to serve.
     */
    public static final String WAIT_ALL_REPLICAS = "WAIT_ALL_REPLICAS";
    /**
     * Wait at least one replica in each parition is ready, the version is ready to server.
     */
    public static final String WAIT_ONE_REPLICA_PER_PARTITION = "WAIT_ONE_REPLICA_PER_PARTITION=";

    public static final String ACTIVE = "ACTIVE";
    public static final String INACTIVE = "INACTIVE";
}

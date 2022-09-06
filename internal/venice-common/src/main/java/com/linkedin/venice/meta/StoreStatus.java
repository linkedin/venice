package com.linkedin.venice.meta;

public enum StoreStatus {
  FULLLY_REPLICATED, // In the current version of this store, the number of ONLINE replicas in each partition equals to
                     // the replication factor.
  UNDER_REPLICATED, // In the current version of this store, the number of ONLINE replicas in one or more partitions is
                    // larger than 0 and smaller than the replication factor.
  DEGRADED, // In the current version of this store, the number of ONLINE replicas in one or more partitions is 0
  UNAVAILABLE // Store does not exist or store is unreachable.
}

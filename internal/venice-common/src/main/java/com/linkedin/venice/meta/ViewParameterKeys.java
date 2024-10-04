package com.linkedin.venice.meta;

public enum ViewParameterKeys {
  /**
   * Parameter key used to specify the re-partition view name.
   */
  MATERIALIZED_VIEW_NAME,
  /**
   * Parameter key used to specify the partitioner for the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITIONER,
  /**
   * Parameter key used to specify the partitioner parameters for the partitioner associated with the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITIONER_PARAMS,
  /**
   * Parameter key used to specify the partition count for the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITION_COUNT;
}

package com.linkedin.venice.meta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public enum ViewParameters {
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

  public static class Builder {
    private String viewName;
    private String partitioner;
    private String partitionerParams;
    private String partitionCount;

    public Builder(String viewName) {
      this.viewName = Objects.requireNonNull(viewName, "View name cannot be null for ViewParameters");
    }

    public Builder(String viewName, Map<String, String> viewParams) {
      this.viewName = viewName;
      this.partitioner = viewParams.get(MATERIALIZED_VIEW_PARTITIONER.name());
      this.partitionerParams = viewParams.get(MATERIALIZED_VIEW_PARTITIONER_PARAMS.name());
      this.partitionCount = viewParams.get(MATERIALIZED_VIEW_PARTITION_COUNT.name());
    }

    public Builder setPartitioner(String partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder setPartitionerParams(String partitionerParams) {
      this.partitionerParams = partitionerParams;
      return this;
    }

    public Builder setPartitionerParams(Map<String, String> partitionerParams) throws JsonProcessingException {
      this.partitionerParams = ObjectMapperFactory.getInstance().writeValueAsString(partitionerParams);
      return this;
    }

    public Builder setPartitionCount(int partitionCount) {
      this.partitionCount = Integer.toString(partitionCount);
      return this;
    }

    public Map<String, String> build() {
      Map<String, String> viewParams = new HashMap<>();
      viewParams.put(MATERIALIZED_VIEW_NAME.name(), viewName);
      if (partitioner != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITIONER.name(), partitioner);
      }
      if (partitionerParams != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITIONER_PARAMS.name(), partitionerParams);
      }
      if (partitionCount != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITION_COUNT.name(), partitionCount);
      }
      return viewParams;
    }
  }
}

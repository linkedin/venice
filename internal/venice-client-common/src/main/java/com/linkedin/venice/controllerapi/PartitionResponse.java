package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class PartitionResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  int partitionCount;

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  @JsonIgnore
  public String toString() {
    return PartitionResponse.class.getSimpleName() + "(partition_count: " + partitionCount + ", super: "
        + super.toString() + ")";
  }
}

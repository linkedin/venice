package com.linkedin.venice.controllerapi.request;

import java.util.Objects;


public class UpdateAdminTopicMetadataRequest extends ControllerRequest {
  private Long executionId = null;
  private Long offset = null;
  private Long upstreamOffset = null;

  public UpdateAdminTopicMetadataRequest(
      String clusterName,
      String storeName,
      Long executionId,
      Long offset,
      Long upstreamOffset) {
    super(clusterName);
    this.executionId = Objects.requireNonNull(executionId, "executionId cannot be null");

    // optional arguments
    this.offset = offset;
    this.upstreamOffset = upstreamOffset;
    this.storeName = storeName;
  }

  public UpdateAdminTopicMetadataRequest(
      String clusterName,
      String storeName,
      String executionId,
      String offset,
      String upstreamOffset) {
    super(clusterName);
    this.executionId = Long.parseLong(validateParam(executionId, "executionId"));

    // optional arguments
    this.offset = offset != null ? Long.parseLong(offset) : null;
    this.upstreamOffset = upstreamOffset != null ? Long.parseLong(upstreamOffset) : null;
    this.storeName = storeName;
  }

  public Long getExecutionId() {
    return executionId;
  }

  public Long getOffset() {
    return offset;
  }

  public Long getUpstreamOffset() {
    return upstreamOffset;
  }
}

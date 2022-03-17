package com.linkedin.venice.controllerapi;

import java.util.Optional;

public class AdminTopicMetadataResponse extends ControllerResponse {
  /**
   * The last persisted admin message's execution id, which could be store-level or cluster-level. Store-level
   * execution id is stored in /venice/<clusterName>/executionids/succeededPerStore znode, while cluster-level
   * execution id is stored in /venice/<clusterName>/adminTopicMetadata znode.
   */
  private long executionId = -1;
  /**
   * The last persisted offset in local admin topic, which is cluster-level and stored in
   * /venice/<clusterName>/adminTopicMetadata znode
   */
  private Optional<Long> offset = Optional.empty();
  /**
   * The last persisted offset in source admin topic, which is cluster-level and stored in
   * /venice/<clusterName>/adminTopicMetadata znode
   */
  private Optional<Long> upstreamOffset = Optional.empty();

  public long getExecutionId() {
    return executionId;
  }

  public Optional<Long> getOffset() {
    return offset;
  }

  public Optional<Long> getUpstreamOffset() {
    return upstreamOffset;
  }

  public void setExecutionId(long executionId) {
    this.executionId = executionId;
  }

  public void setOffset(Optional<Long> offset) {
    this.offset = offset;
  }

  public void setUpstreamOffset(Optional<Long> upstreamOffset) {
    this.upstreamOffset = upstreamOffset;
  }
}

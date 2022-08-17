package com.linkedin.venice.controllerapi;

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
  private long offset = -1;
  /**
   * The last persisted offset in source admin topic, which is cluster-level and stored in
   * /venice/<clusterName>/adminTopicMetadata znode
   */
  private long upstreamOffset = -1;

  public long getExecutionId() {
    return executionId;
  }

  public long getOffset() {
    return offset;
  }

  public long getUpstreamOffset() {
    return upstreamOffset;
  }

  public void setExecutionId(long executionId) {
    this.executionId = executionId;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setUpstreamOffset(long upstreamOffset) {
    this.upstreamOffset = upstreamOffset;
  }
}

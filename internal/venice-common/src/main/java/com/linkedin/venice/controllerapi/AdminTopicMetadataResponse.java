package com.linkedin.venice.controllerapi;

import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;


public class AdminTopicMetadataResponse extends ControllerResponse {
  /**
   * The last persisted admin message's execution id, which could be store-level or cluster-level. Store-level
   * execution id is stored in /venice/<clusterName>/executionids/succeededPerStore znode, while cluster-level
   * execution id is stored in /venice/<clusterName>/adminTopicMetadata znode.
   */
  private long executionId = -1;
  /**
   * The last persisted position in local admin topic, which is cluster-level and stored in
   * /venice/<clusterName>/adminTopicMetadata znode
   */
  private PubSubPositionJsonWireFormat position = PubSubSymbolicPosition.EARLIEST.toJsonWireFormat();

  /**
   * The last persisted position in source admin topic, which is cluster-level and stored in
   * /venice/<clusterName>/adminTopicMetadata znode
   */
  private PubSubPositionJsonWireFormat upstreamPosition = PubSubSymbolicPosition.EARLIEST.toJsonWireFormat();

  /**
   * The current admin operation protocol version, which is cluster-level and be SOT for serialize/deserialize admin operation message
   */
  private long adminOperationProtocolVersion = -1;

  public long getExecutionId() {
    return executionId;
  }

  public PubSubPositionJsonWireFormat getPosition() {
    return position;
  }

  public PubSubPositionJsonWireFormat getUpstreamPosition() {
    return upstreamPosition;
  }

  public void setExecutionId(long executionId) {
    this.executionId = executionId;
  }

  public void setPosition(PubSubPositionJsonWireFormat position) {
    this.position = position;
  }

  public void setUpstreamPosition(PubSubPositionJsonWireFormat upstreamPosition) {
    this.upstreamPosition = upstreamPosition;
  }

  public void setAdminOperationProtocolVersion(long adminOperationProtocolVersion) {
    this.adminOperationProtocolVersion = adminOperationProtocolVersion;
  }

  public long getAdminOperationProtocolVersion() {
    return adminOperationProtocolVersion;
  }
}

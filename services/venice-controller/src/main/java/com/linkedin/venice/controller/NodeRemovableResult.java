package com.linkedin.venice.controller;

public class NodeRemovableResult {
  private String instanceId;
  private boolean isRemovable = true;
  private String blockingResource;
  private BlockingRemoveReason blockingReason;
  private String details;

  private NodeRemovableResult(String instanceId) {
    this.instanceId = instanceId;
  }

  public boolean isRemovable() {
    return isRemovable;
  }

  public String getBlockingResource() {
    return blockingResource;
  }

  public String getBlockingReason() {
    return blockingReason == null ? "N/A" : blockingReason.toString();
  }

  public String getDetails() {
    return details;
  }

  public static NodeRemovableResult removableResult(String instanceId, String details) {
    NodeRemovableResult result = new NodeRemovableResult(instanceId);
    result.details = details;
    return result;
  }

  /**
   * @return a {@link NodeRemovableResult} object with specified parameters.
   */
  public static NodeRemovableResult nonRemovableResult(
      String instanceId,
      String blockingResource,
      BlockingRemoveReason blockingReason,
      String details) {
    NodeRemovableResult result = new NodeRemovableResult(instanceId);
    result.isRemovable = false;
    result.blockingResource = blockingResource;
    result.blockingReason = blockingReason;
    result.details = details;
    return result;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public enum BlockingRemoveReason {
    WILL_LOSE_DATA, WILL_TRIGGER_LOAD_REBALANCE, WILL_FAIL_PUSH;
  }
}

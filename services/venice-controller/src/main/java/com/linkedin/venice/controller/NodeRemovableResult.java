package com.linkedin.venice.controller;

public class NodeRemovableResult {
  private final String instanceId;
  private final boolean isRemovable;
  private final String blockingResource;
  private final BlockingRemoveReason blockingReason;
  private final String details;

  private NodeRemovableResult(
      String instanceId,
      boolean isRemovable,
      String blockingResource,
      BlockingRemoveReason blockingReason,
      String details) {
    this.instanceId = instanceId;
    this.isRemovable = isRemovable;
    this.blockingResource = blockingResource;
    this.blockingReason = blockingReason;
    this.details = details;
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

  public String getFormattedMessage() {
    if (isRemovable) {
      return "Instance is removable";
    }

    return getBlockingReason() + "(" + getBlockingResource() + ": " + getDetails() + ")";
  }

  public static NodeRemovableResult removableResult(String instanceId, String details) {
    return new NodeRemovableResult(instanceId, true, null, null, details);
  }

  /**
   * @return a {@link NodeRemovableResult} object with specified parameters.
   */
  public static NodeRemovableResult nonRemovableResult(
      String instanceId,
      String blockingResource,
      BlockingRemoveReason blockingReason,
      String details) {
    return new NodeRemovableResult(instanceId, false, blockingResource, blockingReason, details);
  }

  public String getInstanceId() {
    return instanceId;
  }

  public enum BlockingRemoveReason {
    WILL_LOSE_DATA, WILL_TRIGGER_LOAD_REBALANCE
  }
}

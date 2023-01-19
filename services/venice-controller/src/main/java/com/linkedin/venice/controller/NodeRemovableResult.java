package com.linkedin.venice.controller;

public class NodeRemovableResult {
  private boolean isRemovable = true;
  private String blockingResource;
  private BlockingRemoveReason blockingReason;
  private String details;

  private NodeRemovableResult() {

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

  public static NodeRemovableResult removableResult() {
    return new NodeRemovableResult();
  }

  /**
   * @return a {@link NodeRemovableResult} object with specified parameters.
   */
  public static NodeRemovableResult nonremoveableResult(
      String blockingResource,
      BlockingRemoveReason blockingReason,
      String details) {
    NodeRemovableResult result = new NodeRemovableResult();
    result.isRemovable = false;
    result.blockingResource = blockingResource;
    result.blockingReason = blockingReason;
    result.details = details;
    return result;
  }

  public enum BlockingRemoveReason {
    WILL_LOSE_DATA, WILL_TRIGGER_LOAD_REBALANCE, WILL_FAIL_PUSH;
  }
}

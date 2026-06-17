package com.linkedin.venice.controllerapi;

import java.util.Collections;
import java.util.Map;


/**
 * Response for {@link ControllerRoute#CHECK_TOPIC_OPERATION_SAFETY}: whether a destructive PubSub operation (topic
 * truncation, retention update, or deletion) on a topic is safe to run without an operator override. The store name and
 * resolved owning cluster are carried on the base {@link ControllerResponse} ({@code name} / {@code cluster}).
 */
public class TopicOperationSafetyResponse extends ControllerResponse {
  private boolean allowed;
  private String reason;
  /** region -&gt; why that region blocks the operation; empty when {@link #allowed}. */
  private Map<String, String> blockingRegions = Collections.emptyMap();

  public boolean isAllowed() {
    return allowed;
  }

  public void setAllowed(boolean allowed) {
    this.allowed = allowed;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public Map<String, String> getBlockingRegions() {
    return blockingRegions;
  }

  public void setBlockingRegions(Map<String, String> blockingRegions) {
    this.blockingRegions = blockingRegions == null ? Collections.emptyMap() : blockingRegions;
  }
}

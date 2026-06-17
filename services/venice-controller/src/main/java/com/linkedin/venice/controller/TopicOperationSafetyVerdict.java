package com.linkedin.venice.controller;

import java.util.Collections;
import java.util.Map;


/**
 * Result of {@link Admin#checkTopicOperationSafety(String)}: whether a destructive PubSub operation (topic truncation,
 * retention update, or deletion) on a given topic is safe to execute without an operator override.
 *
 * <p>The check is <em>additive</em>: it only reports {@code allowed == false} when it can positively determine that the
 * topic still backs a non-deprecated version (current, future, or backup) of its store in at least one region, or when
 * it cannot verify the per-region current versions (a region is unreachable, or the check is running on a controller
 * that cannot observe every region). When the owning store/cluster cannot be resolved (an orphaned topic) the operation
 * is {@code allowed}, preserving legacy behavior. A blocked operation can still be forced via the admin tool's
 * {@code --force} flag.</p>
 */
public class TopicOperationSafetyVerdict {
  private final boolean allowed;
  private final String topicName;
  private final String storeName;
  private final String cluster;
  private final String reason;
  /** region -&gt; why that region blocks the operation; empty when {@link #allowed}. */
  private final Map<String, String> blockingRegions;

  private TopicOperationSafetyVerdict(
      boolean allowed,
      String topicName,
      String storeName,
      String cluster,
      String reason,
      Map<String, String> blockingRegions) {
    this.allowed = allowed;
    this.topicName = topicName;
    this.storeName = storeName;
    this.cluster = cluster;
    this.reason = reason;
    this.blockingRegions = blockingRegions == null ? Collections.emptyMap() : blockingRegions;
  }

  public static TopicOperationSafetyVerdict allowed(String topicName, String storeName, String cluster, String reason) {
    return new TopicOperationSafetyVerdict(true, topicName, storeName, cluster, reason, Collections.emptyMap());
  }

  public static TopicOperationSafetyVerdict blocked(
      String topicName,
      String storeName,
      String cluster,
      String reason,
      Map<String, String> blockingRegions) {
    return new TopicOperationSafetyVerdict(false, topicName, storeName, cluster, reason, blockingRegions);
  }

  public boolean isAllowed() {
    return allowed;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getCluster() {
    return cluster;
  }

  public String getReason() {
    return reason;
  }

  public Map<String, String> getBlockingRegions() {
    return blockingRegions;
  }

  @Override
  public String toString() {
    return "TopicOperationSafetyVerdict{allowed=" + allowed + ", topicName=" + topicName + ", storeName=" + storeName
        + ", cluster=" + cluster + ", reason=" + reason + ", blockingRegions=" + blockingRegions + "}";
  }
}

package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.concurrent.CompletableFuture;


/**
 * Tracks Declaration of Leadership (DoL) state during STANDBY to LEADER transition.
 * DoL mechanism ensures the new leader is fully caught up with VT before switching to remote VT or RT.
 */
public class DolStamp {
  private final long leadershipTerm;
  private final String hostId;
  private final long produceStartTimeMs; // Timestamp when DoL production started
  private volatile boolean dolProduced; // DoL message was acked by broker
  private volatile boolean dolConsumed; // DoL message was consumed back by this replica
  private volatile CompletableFuture<PubSubProduceResult> dolProduceFuture; // Future tracking DoL produce result

  public DolStamp(long leadershipTerm, String hostId) {
    this.leadershipTerm = leadershipTerm;
    this.hostId = hostId;
    this.produceStartTimeMs = System.currentTimeMillis();
    this.dolProduced = false;
    this.dolConsumed = false;
    this.dolProduceFuture = null;
  }

  public long getLeadershipTerm() {
    return leadershipTerm;
  }

  public String getHostId() {
    return hostId;
  }

  public boolean isDolProduced() {
    return dolProduced;
  }

  public void setDolProduced(boolean dolProduced) {
    this.dolProduced = dolProduced;
  }

  public boolean isDolConsumed() {
    return dolConsumed;
  }

  public void setDolConsumed(boolean dolConsumed) {
    this.dolConsumed = dolConsumed;
  }

  public CompletableFuture<PubSubProduceResult> getDolProduceFuture() {
    return dolProduceFuture;
  }

  public void setDolProduceFuture(CompletableFuture<PubSubProduceResult> dolProduceFuture) {
    this.dolProduceFuture = dolProduceFuture;
  }

  public boolean isDolComplete() {
    return dolProduced && dolConsumed;
  }

  public long getProduceStartTimeMs() {
    return produceStartTimeMs;
  }

  /**
   * Calculate latency from DoL production start to now.
   * @return latency in milliseconds
   */
  public long getLatencyMs() {
    return System.currentTimeMillis() - produceStartTimeMs;
  }

  @Override
  public String toString() {
    String produceResult = "";
    // Only attempt to get result if future completed successfully (not exceptionally)
    if (dolProduceFuture != null && dolProduceFuture.isDone() && !dolProduceFuture.isCompletedExceptionally()) {
      PubSubProduceResult result = dolProduceFuture.join();
      if (result != null) {
        produceResult = ", position=" + result.getPubSubPosition();
      }
    }
    return "DolStamp{term=" + leadershipTerm + ", host=" + hostId + ", produced=" + dolProduced + ", consumed="
        + dolConsumed + produceResult + ", latencyMs=" + getLatencyMs() + "}";
  }
}

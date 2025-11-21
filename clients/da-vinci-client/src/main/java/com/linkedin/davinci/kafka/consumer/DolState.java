package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.concurrent.CompletableFuture;


/**
 * Tracks Declaration of Leadership (DoL) state during STANDBY to LEADER transition.
 * DoL mechanism ensures the new leader is fully caught up with VT before switching to remote VT or RT.
 */
public class DolState {
  private final long leadershipTerm;
  private final String hostId;
  private volatile boolean dolProduced; // DoL message was acked by broker
  private volatile boolean dolConsumed; // DoL message was consumed back by this replica
  private volatile CompletableFuture<PubSubProduceResult> dolProduceFuture; // Future tracking DoL produce result

  public DolState(long leadershipTerm, String hostId) {
    this.leadershipTerm = leadershipTerm;
    this.hostId = hostId;
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

  public boolean isReady() {
    return dolProduced && dolConsumed;
  }

  @Override
  public String toString() {
    return "DolState{term=" + leadershipTerm + ", host=" + hostId + ", produced=" + dolProduced + ", consumed="
        + dolConsumed + ", futureSet=" + (dolProduceFuture != null) + "}";
  }
}
